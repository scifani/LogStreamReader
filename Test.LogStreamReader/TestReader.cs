using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using LogStreamReader;
using System.Threading;
using System.IO;
using System.Collections.Generic;
using System.Linq;

namespace Test.LogStreamReader
{
  [TestClass]
  public class TestReader
  {
    public string DataPath = @"..\..\Data";
    public string TestPath = @"..\..\Test\TestReader";

    private string _dataDir;
    private string _testDir;

    public TestReader()
    {
      _dataDir = Path.Combine(Directory.GetCurrentDirectory(), DataPath);
      _testDir = Path.Combine(Directory.GetCurrentDirectory(), TestPath);
    }

    [TestMethod]
    public void TestFullLoad()
    {
      var evt_changed = new AutoResetEvent(false);
      var evt_completed = new AutoResetEvent(false);
      var fileChanged = "";
      var fileCompleted = "";
      string[] destFiles, logFiles;
      string filter = "LIS*.XML";
 
      PrepareTestDir(filter, 1, out logFiles, out destFiles);

      // Create Reader
      var logReader = new Reader(_testDir, filter);

      logReader.FileChanged += (s, e) =>
      {
        fileChanged = e.FileName;
        evt_changed.Set();
      };

      logReader.FileCompleted += (s, e) =>
      {
        fileCompleted = e.FileName;
        evt_completed.Set();
      };

      var simpleProcessor = new SimpleProcessor();
      logReader.SetRecordProcessor(simpleProcessor);

      logReader.StartProcessing();

      // Assert
      Assert.IsTrue(evt_changed.WaitOne(timeout: TimeSpan.FromSeconds(1)));
      Assert.AreEqual(fileChanged, destFiles[0]);

      Thread.Sleep(3000); // add a small delay before starting the new file

      var actual_rows = simpleProcessor.Rows;

      File.Copy(destFiles[0],
        Path.Combine(Path.GetDirectoryName(destFiles[0]), "LIS-DUMMY.XML"));

      Assert.IsTrue(evt_completed.WaitOne(timeout: TimeSpan.FromSeconds(1)));
      Assert.AreEqual(fileCompleted, destFiles[0]);

      var fileRows = File.ReadLines(destFiles[0]).Count();
      Assert.AreEqual(fileRows, actual_rows);

      //Stop Reader
      logReader.StopProcessing();
    }

    [TestMethod]
    public void TestDynamicLoad()
    {
      var evt_changed = new AutoResetEvent(false);
      var evt_completed = new AutoResetEvent(false);
      var fileChanged = "";
      var fileCompleted = "";
      string[] destFiles, logFiles;
      string filter = "LIS*.XML";

      PrepareTestDir(filter, 0, out logFiles, out destFiles);

      // Create Reader
      var logReader = new Reader(_testDir, filter);

      logReader.FileChanged += (s, e) =>
      {
        fileChanged = e.FileName;
        evt_changed.Set();
      };

      logReader.FileCompleted += (s, e) =>
      {
        fileCompleted = e.FileName;
        evt_completed.Set();
      };

      var writeProcessor = new WriteProcessor(_testDir);
      logReader.SetRecordProcessor(writeProcessor);

      logReader.StartProcessing();

      var lines = File.ReadLines(logFiles[0]).ToList();

      var destFile = Path.Combine(_testDir, "LIS-001.XML");

      using (var fs = new FileStream(destFile, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Delete | FileShare.ReadWrite))
      {
        using (var sw = new StreamWriter(fs))
        {
          for (int i = 0; i < 100; i++)
            sw.WriteLine(lines[i]);

          Assert.IsTrue(evt_changed.WaitOne(timeout: TimeSpan.FromSeconds(3)));
          Assert.AreEqual(destFile, fileChanged);          

          for (int i = 100; i < lines.Count; i++)
            sw.WriteLine(lines[i]);

          Console.WriteLine("");
        }
      }

      Thread.Sleep(500);

      Assert.AreEqual(lines.Count(), writeProcessor.Rows);

      writeProcessor.Close();

      logReader.StopProcessing();      
    }

    private void PrepareTestDir(string filter, int numFiles, out string[] logFiles, out string[] destFiles)
    {
      var destFilesList = new List<string>();

      if (Directory.Exists(_testDir))
        Array.ForEach(Directory.GetFiles(_testDir), path => File.Delete(path));
      else
        Directory.CreateDirectory(_testDir);

      logFiles = Directory.GetFiles(_dataDir, filter);

      if (numFiles > logFiles.Length)
        numFiles = logFiles.Length;

      for(int i = 0; i < numFiles; i++)
      {
        var dstFile = Path.Combine(_testDir, Path.GetFileName(logFiles[i]));
        File.Copy(logFiles[i], dstFile);
        destFilesList.Add(dstFile);
      }

      destFiles = destFilesList.ToArray();      
    }

    internal class SimpleProcessor : IRecordProcessor
    {
      public int Rows { get; internal set; }
      public void Process(string record)
      {
        Rows++;
      }
    }

    internal class WriteProcessor : IRecordProcessor
    {
      private StreamWriter _sw;

      public int Rows { get; internal set; }

      public WriteProcessor(string testDir)
      {
        _sw = new StreamWriter(Path.Combine(testDir, "Processed.txt"));
      }

      public void Process(string record)
      {
        _sw.WriteLine(record);

        Rows++;
      }

      public void Close()
      {
        if (_sw != null)
        {
          _sw.Close();
          _sw = null;
        }
      }
    }
  }
}
