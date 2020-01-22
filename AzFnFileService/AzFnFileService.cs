using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using System.IO;

using Newtonsoft.Json;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.File;
using Microsoft.WindowsAzure.Storage.Blob;


namespace AzFnFileService
{
    public static class AzFnFileService
    {

        private static string storageConnectionStr = System.Environment.GetEnvironmentVariable("StorageConnectionString");
        private static string fileshare = System.Environment.GetEnvironmentVariable("FileShare");
        private static TraceWriter log;
        private static string TYPE_BLOB = "BLOB";
        private static string TYPE_FILE = "FILE";

        public class FileService
        {
            public string Operation { get; set; }
            public string InputFolder { get; set; }
            public string File { get; set; }
            public string Type { get; set; }
            public string TargetFolder { get; set; }
            public string Status { get; set; }
            public string Message { get; set; }
            public bool Exists { get; set; }
            public string Body { get; set; }
            public string PrefixDateTime { get; set; }
            public string Container { get; set; }
        }

        [FunctionName("AzFnFileService")]
        public static async Task<HttpResponseMessage> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)]HttpRequestMessage req, TraceWriter log)
        {
            FileService fs = new FileService();

            AzFnFileService.log = log;
            log.Info("C# HTTP trigger function started");

            if (req.Method == HttpMethod.Post)
            {
                string data = await req.Content.ReadAsStringAsync();
                log.Info("Body " + data);
                fs.Body = data;
            }

            if (req.Method == HttpMethod.Get)
            {

                log.Info("GET request received");
                // parse query parameters
                fs.Operation = req.GetQueryNameValuePairs()
                    .FirstOrDefault(q => string.Compare(q.Key, "op", true) == 0)
                    .Value?.ToLower();

                fs.InputFolder = req.GetQueryNameValuePairs()
                    .FirstOrDefault(q => string.Compare(q.Key, "inputfolder", true) == 0)
                    .Value?.ToLower();

                fs.File = req.GetQueryNameValuePairs()
                    .FirstOrDefault(q => string.Compare(q.Key, "file", true) == 0)
                    .Value;

                fs.Type = req.GetQueryNameValuePairs()
                    .FirstOrDefault(q => string.Compare(q.Key, "type", true) == 0)
                    .Value?.ToUpper();
                if (fs.Type == null) fs.Type = TYPE_BLOB;

                fs.TargetFolder = req.GetQueryNameValuePairs()
                    .FirstOrDefault(q => string.Compare(q.Key, "targetfolder", true) == 0)
                    .Value?.ToLower();

                fs.Container = req.GetQueryNameValuePairs()
                    .FirstOrDefault(q => string.Compare(q.Key, "container", true) == 0)
                    .Value?.ToLower();

                fs.PrefixDateTime = req.GetQueryNameValuePairs()
                    .FirstOrDefault(q => string.Compare(q.Key, "prefixdatetime", true) == 0)
                    .Value?.ToLower();

            }

            try
            {

                if (fs.Operation != null)
                {
                    log.Info("Operation requested : " + fs.Operation);

                    if (fs.Type == TYPE_FILE)
                    {
                        switch (fs.Operation)
                        {
                            case "copy":
                                CopyFile(fs.InputFolder, fs.File, fs.TargetFolder);
                                break;
                            case "delete":
                                DeleteFile(fs.InputFolder, fs.File);
                                break;
                            case "move":
                                MoveFile(fs.InputFolder, fs.File, fs.TargetFolder, fs.PrefixDateTime);
                                break;
                            case "exist":
                                fs.Exists = FileExists(fs.InputFolder, fs.File);
                                break;
                            default:
                                fs.Operation = "Invalid";
                                break;
                        }

                    }
                    else
                    {
                        switch (fs.Operation)
                        {
                            case "copy":
                                fs.Message = CopyBlob(fs.Container, fs.InputFolder, fs.File, fs.TargetFolder);
                                break;
                            case "delete":
                                //DeleteFile(fs.InputFolder, fs.File);
                                fs.Message = "Delete operation not allowed now";
                                break;
                            case "move":
                                fs.Message = MoveBlob(fs.Container, fs.InputFolder, fs.File, fs.TargetFolder, fs.PrefixDateTime);
                                break;
                            case "exist":
                                fs.Exists = BlobExists(fs.Container, fs.InputFolder, fs.File);
                                break;
                            default:
                                fs.Operation = "Invalid";
                                break;

                        }
                    }


                    fs.Status = "Ok";
                    if (String.IsNullOrEmpty(fs.Message)) fs.Message = "Completed";
                }
                else
                {
                    log.Info("Operation parameter value missing");
                    fs.Status = "Incomplete";
                    fs.Message = "Missing value for parameter : operation";
                }

            }
            catch (Exception ex)
            {
                fs.Status = "Error";
                fs.Message = ex.ToString();

            }

            

            var json = JsonConvert.SerializeObject(fs, Formatting.Indented);
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(json, System.Text.Encoding.UTF8, "application/json")
            };
        }


        static void CopyFile(string strSourceFolderPath, string strSourceFileName, string strTargetFolderPath)
        {
            log.Info("Executing Copy File");
            log.Info("Folder " + strSourceFolderPath);
            log.Info("File : " + strSourceFileName);
            log.Info("Target : " + strTargetFolderPath);

            //Copy File method.
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionStr);
            CloudFileClient fileClient = storageAccount.CreateCloudFileClient();
            CloudFileShare share = fileClient.GetShareReference(fileshare);
            CloudFileDirectory rootDir = share.GetRootDirectoryReference();
            CloudFile sourceFile;
            if (strSourceFolderPath != "")
            {
                CloudFileDirectory sourcefolder = rootDir.GetDirectoryReference(strSourceFolderPath);
                sourceFile = sourcefolder.GetFileReference(strSourceFileName);
            }
            else
            {
                sourceFile = rootDir.GetFileReference(strSourceFileName);
            }


            CloudFileDirectory targetfolder = rootDir.GetDirectoryReference(strTargetFolderPath);
            CloudFile destFile = targetfolder.GetFileReference(strSourceFileName);

            log.Info("Source File : " + sourceFile.Name);
            log.Info("Target File : " + targetfolder.Name);
            destFile.StartCopy(sourceFile);
            log.Info(sourceFile.Name + " copied to " + targetfolder.Name);

        }


        static void MoveFile(string strSourceFolderPath, string strSourceFileName, string strTargetFolderPath, string strPrefixDateTime)
        {
            log.Info("Executing Move File");
            log.Info("Folder " + strSourceFolderPath);
            log.Info("File : " + strSourceFileName);
            log.Info("Target : " + strTargetFolderPath);

            //Move file method.
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionStr);
            CloudFileClient fileClient = storageAccount.CreateCloudFileClient();
            CloudFileShare share = fileClient.GetShareReference(fileshare);
            CloudFileDirectory rootDir = share.GetRootDirectoryReference();
            CloudFile sourceFile;
            if (strSourceFolderPath != "")
            {
                CloudFileDirectory sourcefolder = rootDir.GetDirectoryReference(strSourceFolderPath);
                sourceFile = sourcefolder.GetFileReference(strSourceFileName);
            }
            else
            {
                sourceFile = rootDir.GetFileReference(strSourceFileName);
            }

            string strdestFile;
            log.Info("DateTimePrefix Parameter is " + strPrefixDateTime);
            if (strPrefixDateTime == "y")
            {
                log.Info("Appending the DateTime to File");
                strdestFile = AppendTimeStampToFile(strSourceFileName);
            }
            else
            {
                strdestFile = strSourceFileName;
            }
            CloudFileDirectory targetfolder = rootDir.GetDirectoryReference(strTargetFolderPath);
            CloudFile destFile = targetfolder.GetFileReference(strdestFile);

            destFile.StartCopy(sourceFile);
            sourceFile.DeleteIfExists();
            log.Info(sourceFile.Name + " copied to " + targetfolder.Name + " as " + destFile.Name);


        }



        static void DeleteFile(string strSourceFolderPath, string strSourceFileName)
        {
            log.Info("Executing Delete File");
            log.Info("Folder " + strSourceFolderPath);
            log.Info("File : " + strSourceFileName);

            //Delete File method
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionStr);
            CloudFileClient fileClient = storageAccount.CreateCloudFileClient();
            CloudFileShare share = fileClient.GetShareReference(fileshare);
            CloudFileDirectory rootDir = share.GetRootDirectoryReference();
            CloudFile sourceFile;
            if (strSourceFolderPath != "")
            {
                CloudFileDirectory sourcefolder = rootDir.GetDirectoryReference(strSourceFolderPath);
                sourceFile = sourcefolder.GetFileReference(strSourceFileName);
            }
            else
            {
                sourceFile = rootDir.GetFileReference(strSourceFileName);
            }
            sourceFile.DeleteIfExists();
            log.Info(sourceFile.Name + " deleted.");



        }


        static bool FileExists(string strSourceFolderPath, string strSourceFileName)
        {
            log.Info("Checking if the file exists");
            log.Info("Folder " + strSourceFolderPath);
            log.Info("File : " + strSourceFileName);
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionStr);
            CloudFileClient fileClient = storageAccount.CreateCloudFileClient();
            CloudFileShare share = fileClient.GetShareReference(fileshare);
            CloudFileDirectory rootDir = share.GetRootDirectoryReference();
            CloudFile sourceFile;

            if (strSourceFolderPath != "")
            {
                CloudFileDirectory sourcefolder = rootDir.GetDirectoryReference(strSourceFolderPath);
                sourceFile = sourcefolder.GetFileReference(strSourceFileName);
            }
            else
            {
                sourceFile = rootDir.GetFileReference(strSourceFileName);
            }

            log.Info(sourceFile.Uri.ToString());

            return sourceFile.Exists() ? true : false;

        }

        static bool BlobExists(string strContainer, string strSourceFolderPath, string strSourceFileName)
        {
            log.Info("Checking if the blob exists");
            log.Info("Folder " + strSourceFolderPath);
            log.Info("File : " + strSourceFileName);
            string strPath = strSourceFolderPath + "/" + strSourceFileName;
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionStr);
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = blobClient.GetContainerReference(strContainer);
            CloudBlockBlob blockBlob = container.GetBlockBlobReference(strPath);

            log.Info(strPath);
            return blockBlob.Exists() ? true : false;
        }

        static string CopyBlob(string strContainer, string strSourceFolderPath, string strSourceFileName, string strTargetFolderPath)
        {
            log.Info("Copy BLOB");
            log.Info("Source Folder " + strSourceFolderPath);
            log.Info("File : " + strSourceFileName);
            log.Info("Target Folder " + strTargetFolderPath);
            string strSrcPath = strSourceFolderPath + "/" + strSourceFileName;
            string strTgtPath = strTargetFolderPath + "/" + strSourceFileName;
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionStr);
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = blobClient.GetContainerReference(strContainer);

            CloudBlockBlob srcBlob = container.GetBlockBlobReference(strSrcPath);
            CloudBlockBlob tgtBlob = container.GetBlockBlobReference(strTgtPath);

            tgtBlob.StartCopy(srcBlob);

            if (tgtBlob.Exists())
            {
                return (strSourceFileName + " copied to " + strTargetFolderPath);
            }
            else
            {
                return ("BLOB not copied. Some issues. Please check.");
            }
        }

        static string MoveBlob(string strContainer, string strSourceFolderPath, string strSourceFileName, string strTargetFolderPath, string strPrefixDateTime)
        {
            log.Info("Copy BLOB");
            log.Info("Source Folder " + strSourceFolderPath);
            log.Info("File : " + strSourceFileName);
            log.Info("Target Folder " + strTargetFolderPath);
            string strSrcPath = strSourceFolderPath + "/" + strSourceFileName;
            string strTgtPath;

            if (strPrefixDateTime == "y")
                strTgtPath = strTargetFolderPath + "/" + AppendTimeStampToFile(strSourceFileName);
            else
                strTgtPath = strTargetFolderPath + "/" + strSourceFileName;

            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionStr);
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = blobClient.GetContainerReference(strContainer);

            CloudBlockBlob srcBlob = container.GetBlockBlobReference(strSrcPath);
            CloudBlockBlob tgtBlob = container.GetBlockBlobReference(strTgtPath);

            tgtBlob.StartCopy(srcBlob);

            if (tgtBlob.Exists())
            {
                srcBlob.Delete();
                return (strSourceFileName + " moved to " + strTgtPath);
            }
            else
            {
                return ("BLOB not moved. Some issues. Please check.");
            }
        }


        static string AppendTimeStampToFile(string strFileName)
        {
            strFileName = Path.GetFileNameWithoutExtension(strFileName) + "_" + System.DateTime.Now.ToString("yyyyMMddHHTmmss") + Path.GetExtension(strFileName);
            return strFileName;
        }


    }
}
