using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Threading;

/// <summary>
/// Iterative implementation of a folder replication algorithm
/// Uses a stack to hold tuples of directory paths and a bool to check  
/// if the to be copied directory is newly created in the replica folder
/// in order to avoid the file/folder deletion steps.
/// Generates a hashset if the number of files/directories is greater than the set limit
/// in order to improve the performance of the file comparison step.
/// Parallelizes the file deletion and copying steps for better performance.
/// </summary>
/// <param name="args">Command-line arguments: 
/// - <c>-input</c>: Input file path
/// - <c>-output</c>: Output file path
/// - <c>-interval</c>: Sync interval in seconds
/// - <c>-log</c>: Log file path
/// </param>
class FolderReplicationClass
{
    //limit for enabling the use of hashset for file comparison
    static int hashSetEnableLimit = 20;

    static void Main(string[] args)
    {
        string sourceFolderPath = args[0];

        string replicaFolderPath = args[1];

        float syncInterval = float.Parse(args[2]);

        string logFilePath = args[3];

        // start the replication process
        // the process is stopped when the user presses any key
        var stopwatch = new System.Diagnostics.Stopwatch();
        bool continueRunning = true;
        while (continueRunning)
        {
            Console.WriteLine("Starting the replication...");
            stopwatch.Restart();

            ReplicateFolder(sourceFolderPath, replicaFolderPath, logFilePath);
            Console.WriteLine($"Replication finished in {stopwatch.Elapsed.TotalSeconds} seconds.");

            // Wait until the sync interval has elapsed
            TimeSpan interval = TimeSpan.FromSeconds(syncInterval);
            while (stopwatch.Elapsed < interval)
            {
                if (Console.KeyAvailable)
                {
                    Console.ReadKey(true);
                    continueRunning = false;
                    break;
                }
                Thread.Sleep(1000);
            }
        }
    }

    static string GetFileHash(string filePath)
    {
        using (var md5 = MD5.Create())
        using (var stream = File.OpenRead(filePath))
        {
            byte[] hash = md5.ComputeHash(stream);
            return BitConverter.ToString(hash).Replace("-", "").ToLowerInvariant();
        }
    }

    private static readonly object logFileLock = new object();

    static void OutputFileWrite(string logFilePath, string message)
    {
        lock (logFileLock)
        {
            using (StreamWriter writer = new StreamWriter(logFilePath, true))
            {
                writer.WriteLine(message);
            }
        }
    }

    static void DeleteFileOrDirectory(string path, string logFilePath)
    {
        // check whether the file to be deleted is a directory or a file
        if (Directory.Exists(path))
        {
            Directory.Delete(path, true);

            OutputFileWrite(logFilePath, $"Deleted directory: {path}");
            Console.WriteLine($"Deleted directory: {path}");
        }
        else
        {
            File.Delete(path);

            OutputFileWrite(logFilePath, $"Deleted file: {path}");
            Console.WriteLine($"Deleted file: {path}");
        }
    }

    static void CopyFilesToReplica(IEnumerable<string> sourceFilePaths, string currentReplicaDir, string logFilePath)
    {
        // Copy all not yet existing files in the replica directory from the current source directory
        Parallel.ForEach(sourceFilePaths, sourceFilePath =>
        {
            string fileName = Path.GetFileName(sourceFilePath);
            string possibleReplicaFileAddress = Path.Combine(currentReplicaDir, fileName);

            
            if (!File.Exists(possibleReplicaFileAddress))
            {
                File.Copy(sourceFilePath, possibleReplicaFileAddress, true);
                OutputFileWrite(logFilePath, $"Copied file: {fileName} to {possibleReplicaFileAddress}");
                Console.WriteLine($"Copied file: {fileName} to {possibleReplicaFileAddress}");
            }
        });
    }


    static void ReplicateFolder(string sourceFolderPath, string replicaFolderPath, string logFilePath)
    {
        // Stack to hold tuple of directory paths and bool to check
        // if the to be copied directory is newly created in the replica folder
        // in order to avoid the file/folder deletion steps
        Stack<Tuple<bool, string>> directories = new Stack<Tuple<bool, string>>();

        //check if replica folder is already empty and change the dirIsToBeCreated bool accordingly
        if (Directory.EnumerateFileSystemEntries(replicaFolderPath).Any())
        {
            directories.Push(new Tuple<bool, string>(false, sourceFolderPath));
        }
        else
        {
            directories.Push(new Tuple<bool, string>(true, sourceFolderPath));
        }


        while (directories.Count > 0)
        {
            Tuple<bool, string> currentDir = directories.Pop();
            string currentSourceDir = currentDir.Item2;
            bool dirIsToBeCreated = currentDir.Item1;


            //remove the dot at the end of the path in case the source and replica folder are the same
            string relativePath = Path.GetRelativePath(sourceFolderPath, currentSourceDir);
            
            if (relativePath == ".")
            {
                relativePath = string.Empty;
            }
            string currentReplicaDir = Path.Combine(replicaFolderPath, relativePath);

            IEnumerable<string> sourceSubDirs = Directory.EnumerateDirectories(currentSourceDir);


            if (dirIsToBeCreated && sourceFolderPath != currentSourceDir)
            {

                // Create the destination directory if it doesn't exist
                Directory.CreateDirectory(currentReplicaDir);
                OutputFileWrite(logFilePath, $"Created directory: {currentReplicaDir}");
                Console.WriteLine($"Created directory: {currentReplicaDir}");


                // Check if the directory has files (not just subdirectories) in it
                if (Directory.EnumerateFiles(currentSourceDir).Any())
                {
                    // Get names of all files in the current source directory
                    IEnumerable<string> sourceFilesPaths = Directory.EnumerateFiles(currentSourceDir);
                    CopyFilesToReplica(sourceFilesPaths, currentReplicaDir, logFilePath);
                }

                // Push all subdirectories onto the stack
                foreach (string sourceSubDir in sourceSubDirs)
                {
                    directories.Push(new Tuple<bool, string>(true, sourceSubDir));
                }
            }
            else
            {
                IEnumerable<string> replicaFiles = Array.Empty<string>();
                IEnumerable<string> sourceFilePaths = Array.Empty<string>();

                // if there are any files/directories in the current replica directory get them
                if (Directory.EnumerateFileSystemEntries(currentReplicaDir).Any())
                {
                    replicaFiles = Directory.EnumerateFileSystemEntries(currentReplicaDir).Select(Path.GetFileName)!;
                }

                // if there are any files in the current source directory get them
                // delete files in replica that don't exist or are newer than the ones in source
                // copy files that exist in source but not in replica
                // if there are no files in the source directory then delete all files from the replica
                if (Directory.EnumerateFileSystemEntries(currentSourceDir).Any())
                {
                    sourceFilePaths = Directory.EnumerateFileSystemEntries(currentSourceDir);
                    var sourceFilesComparator = sourceFilePaths.Select(Path.GetFileName);

                    //generates a hashset if the number of files is greater than the limit
                    if (sourceFilesComparator.Count() > hashSetEnableLimit)
                    {
                        HashSet<string> sourceFilesHashSet = new HashSet<string>(sourceFilesComparator!);
                        sourceFilesComparator = sourceFilesHashSet;
                    }

                    // delete files in replica that don't exist in source
                    // or have different last write time than the ones in source
                    if (replicaFiles.Any())
                    {

                        // parallelize the file deletion step
                        Parallel.ForEach(replicaFiles, replicaFile =>
                        {
                            string fullReplicaFilePath = Path.Combine(currentReplicaDir, replicaFile);



                            if (!sourceFilesComparator.Contains(replicaFile))
                            {
                                DeleteFileOrDirectory(fullReplicaFilePath, logFilePath);
                            }
                            else
                            {
                                // compare the hashes of the files to check if they are the same   
                                if (File.Exists(fullReplicaFilePath))
                                {
                                    string fullSourceFilePath = Path.Combine(currentSourceDir, replicaFile);
                                    if (GetFileHash(fullSourceFilePath) != GetFileHash(fullReplicaFilePath))
                                    {
                                        DeleteFileOrDirectory(fullReplicaFilePath, logFilePath);
                                    }
                                }

                            }
                        });
                    }

                    // copy only files not folders in the current source directory
                    sourceFilePaths = Directory.EnumerateFiles(currentSourceDir);
                    CopyFilesToReplica(sourceFilePaths, currentReplicaDir, logFilePath);
                }
                else
                {
                    // include also directories for deletion
                    replicaFiles = Directory.EnumerateFileSystemEntries(currentReplicaDir).Select(Path.GetFileName)!;

                    // parallelize the file deletion step
                    Parallel.ForEach(replicaFiles, replicaFile =>
                    {
                        string fullPath = Path.Combine(currentReplicaDir, replicaFile);
                        DeleteFileOrDirectory(fullPath, logFilePath);
                    });
                }


                // if there are any subdirectories in the current source directory
                // check if the replica directory has the same subdirectories and push them onto the stack
                // subdirectories that don't exist in the replica directory are pushed on the stack with True to avoid deletion steps
                if (sourceSubDirs.Any())
                {

                    IEnumerable<string> replicaSubDirs = Directory.EnumerateDirectories(currentReplicaDir).Select(Path.GetFileName)!;

                    if (replicaSubDirs.Any())
                    {
                        var replicaSubDirsComparator = replicaSubDirs;

                        //generates a hashset if the number of files is greater than the limit
                        if (replicaSubDirs.Count() > hashSetEnableLimit)
                        {
                            HashSet<string> replicaSubDirsHashSet = new HashSet<string>(replicaSubDirs);
                            replicaSubDirsComparator = replicaSubDirsHashSet;
                        }


                        // Push all source subdirectories onto the stack
                        foreach (string sourceSubDir in sourceSubDirs)
                        {
                            //if the replica dir does not contain the to be created source dir
                            //it can be passed with a True in order to skip the deletion steps
                            if (replicaSubDirsComparator.Contains(Path.GetFileName(sourceSubDir)))
                            {
                                directories.Push(new Tuple<bool, string>(false, sourceSubDir));
                            }
                            else
                            {
                                Console.WriteLine(sourceSubDir);
                                directories.Push(new Tuple<bool, string>(true, sourceSubDir));
                            }
                        }
                    }
                    else
                    {
                        foreach (string sourceSubDir in sourceSubDirs)
                        {
                            directories.Push(new Tuple<bool, string>(true, sourceSubDir));
                        }
                    }
                }
            }
        }
    }
}
