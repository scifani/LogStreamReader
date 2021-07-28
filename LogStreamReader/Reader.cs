using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Linq;
using System.Diagnostics;
using System.Text;

namespace LogStreamReader
{
  public interface IRecordProcessor
  {
    void Process(string record);
  }

  public class DefaultRecordProcessor : IRecordProcessor
  {
    public void Process(string record) { }
  }

  public class Reader
  {
    private FileSystemWatcher _watcher;
    private List<string> _filesToRead;
    private SpinLock _spinlock;
    private AutoResetEvent _changeEvent;
    private Thread _backgroundThread;
    private volatile bool _doProcess;
    private IRecordProcessor _recordProcessor;

    public class FileChangedEventArgs : EventArgs
    {
      public string FileName { get; internal set; }
    }

    public class FileCompletedEventArgs : FileChangedEventArgs {}

    public event EventHandler<FileChangedEventArgs> FileChanged;
    public event EventHandler<FileCompletedEventArgs> FileCompleted;

    public Reader(string path, string filter)
    {
      _watcher = new FileSystemWatcher(path, filter)
      {
        EnableRaisingEvents = true,
        NotifyFilter = NotifyFilters.CreationTime |
          NotifyFilters.FileName | 
          NotifyFilters.Size     
      };

      _watcher.Changed += Watcher_Changed;
      _watcher.Error += Watcher_Error;

      _spinlock = new SpinLock();
      _changeEvent = new AutoResetEvent(false);
      _filesToRead = new List<string>(Directory.GetFiles(path, filter));

      _recordProcessor = new DefaultRecordProcessor();            
    }

    public void SetRecordProcessor(IRecordProcessor recordProcessor)
    {
      _recordProcessor = recordProcessor;
    }

    public void StartProcessing()
    {
      if (!_doProcess && _backgroundThread == null)
      {
        _doProcess = true;

        _backgroundThread = new Thread(new ThreadStart(this.BackgroundWorker));
        _backgroundThread.Start();
      }
    }

    public void StopProcessing()
    {
      if (_doProcess && _backgroundThread != null)
      {
        _doProcess = false;

        _backgroundThread.Join();
        _backgroundThread = null;
      }
    }

    private void BackgroundWorker()
    {
      Debug.WriteLine("BackgroundWorker thread started");

      FileStream fs;
      CustomStreamReader sr;

      while (_doProcess)
      {
        string filename = GetFirstFile();

        if (String.IsNullOrEmpty(filename))
        {
          // no file to read => wait for the next change
          _changeEvent.WaitOne(timeout: TimeSpan.FromSeconds(1));
        }
        else
        {
          OnRaiseFileChangedEvent(new FileChangedEventArgs() { FileName = filename });

          fs = new FileStream(filename, FileMode.Open, FileAccess.Read, FileShare.Delete | FileShare.ReadWrite);
          sr = new CustomStreamReader(fs);

          while (_doProcess)
          {
            foreach (var line in sr.ReadLineCrLf())
            {
              _recordProcessor.Process(line);
            }
            
            Debug.WriteLine("no more data to read");

            if (_filesToRead.Count == 1)
            {
              Debug.WriteLine("wait for the next change");

              // This is the currently active file => wait for the next change
              _changeEvent.WaitOne(timeout: TimeSpan.FromSeconds(5));

              if (!_doProcess)
                break;
            }

            if (_filesToRead.Count > 1)
            {
              Debug.WriteLine("close file");

              // Close this file and start processing the next one
              sr.Close();
              fs.Close();
              RemoveFirstFile();
              OnRaiseFileCompletedEvent(new FileCompletedEventArgs() { FileName = filename });
              break;
            }
          }

          sr.Close();
          fs.Close();

          break;
        }
      }

      Debug.WriteLine("BackgroundWorker thread ended");
    }


    private void Watcher_Changed(object sender, FileSystemEventArgs e)
    {
      try
      {
        // Prevents same event to be fired twice
        // https://social.msdn.microsoft.com/Forums/vstudio/en-US/f68a4510-2264-41f2-b611-9f1633bca21d/filesystemwatcher-changed-event-fires-twice-why-bug?forum=csharpgeneral
        _watcher.EnableRaisingEvents = false;

        EnqueueNewFile(e.FullPath);

        _changeEvent.Set();
      }
      finally
      {
        _watcher.EnableRaisingEvents = true;
      }
    }

    private void Watcher_Error(object sender, ErrorEventArgs e)
    {
      throw new NotImplementedException();
    }

    private void EnqueueNewFile(string path)
    {
      bool lockTaken = false;
      try
      {
        _spinlock.Enter(ref lockTaken);

        if (_filesToRead.Count == 0 || _filesToRead.Last() != path)
        {
          _filesToRead.Add(path);
        }
      }
      finally
      {
        if (lockTaken) _spinlock.Exit(false);
      }
    }

    private string GetFirstFile()
    {
      string filename = null;

      bool lockTaken = false;
      try
      {
        _spinlock.Enter(ref lockTaken);

        if (_filesToRead.Count > 0)
        {
          filename = _filesToRead[0];
        }
      }
      finally
      {
        if (lockTaken) _spinlock.Exit(false);
      }

      return filename;
    }

    private void RemoveFirstFile()
    {
      bool lockTaken = false;
      try
      {
        _spinlock.Enter(ref lockTaken);

        _filesToRead.RemoveAt(0);
      }
      finally
      {
        if (lockTaken) _spinlock.Exit(false);
      }
    }

    // Wrap event invocations inside a protected virtual method
    // to allow derived classes to override the event invocation behavior
    protected virtual void OnRaiseFileChangedEvent(FileChangedEventArgs e)
    {
      // Make a temporary copy of the event to avoid possibility of
      // a race condition if the last subscriber unsubscribes
      // immediately after the null check and before the event is raised.
      EventHandler<FileChangedEventArgs> evt = FileChanged;

      // Event will be null if there are no subscribers
      if (evt != null)
      {
        evt(this, e);
      }
    }

    protected virtual void OnRaiseFileCompletedEvent(FileCompletedEventArgs e)
    {
      EventHandler<FileCompletedEventArgs> evt = FileCompleted;

      if (evt != null)
      {
        evt(this, e);
      }
    }

    class CustomStreamReader : StreamReader
    {
      StringBuilder lineBuffer = new StringBuilder();

      public CustomStreamReader(Stream stream)
        : base(stream)
      {
      }

      public IEnumerable<string> ReadLineCrLf(int bufferSize = 4096)
      {        
        char[] buffer = new char[bufferSize];
        int charsRead;

        var previousIsLf = false;

        while ((charsRead = this.Read(buffer, 0, bufferSize)) != 0)
        {
          int bufferIdx = 0;
          int writeIdx = 0;
          do
          {
            switch (buffer[bufferIdx])
            {
              case '\n':
                if (previousIsLf)
                {
                  lineBuffer.Append(buffer, writeIdx, bufferIdx - writeIdx);                 
                  lineBuffer.Length--;
                  writeIdx = bufferIdx + 1;
                  yield return lineBuffer.ToString();
                  lineBuffer = new StringBuilder();
                }

                previousIsLf = false;
                break;
              case '\r':
                previousIsLf = true;
                break;
              default:
                previousIsLf = false;
                break;

            }

            bufferIdx++;
          } while (bufferIdx < charsRead);

          if (writeIdx < bufferIdx)
          {
            lineBuffer.Append(buffer, writeIdx, bufferIdx - writeIdx);
          }
        }
      }
    }
  }
}
