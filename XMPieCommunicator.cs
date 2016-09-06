using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.IO;
using System.Collections;

namespace XMPieCommunicator
{
    

     public class XMPie_Communicator
    {
        private string username;
        private string password;
        private string accountID;
        private string campaignID;
        private string documentID;

        /// <summary>
        /// Constructor, must pass in valid username and password - throws exception otherwise
        /// </summary>
        /// <param name="userName"></param>
        /// <param name="password"></param>
        public XMPie_Communicator(string userName, string password)
        {
            User.UserSoapClient client = new User.UserSoapClient();
            User.XMPUser user = new User.XMPUser();
            user.Username = userName;
            user.Password = password;
            try
            {
                //just a check to validate the credentials
                string ID = client.GetID(user, userName);
                this.username = userName;
                this.password = password;

                this.accountID = "";
                this.campaignID = "";
                this.documentID = "";
            }
            catch
            {
                throw new Exception("Invalid Username or Password");
            }
        }

        /// <summary>
        /// validates and sets the given AccountID
        /// </summary>
        /// <param name="accountID"></param>
        /// <returns>boolean if  AccountID was successfully validated</returns>
        public bool setAccountID(string accountID)
        {
            bool success = false;
            Account.AccountSoapClient client = new Account.AccountSoapClient();
            Account.XMPUser user = new Account.XMPUser();
            user.Username = this.username;
            user.Password = this.password;
            //validates that the account exists
            success = client.IsExist(user, accountID);
            if (success)
            {
                this.accountID = accountID;
            }
            return success;
        }

        /// <summary>
        /// validates and sets the given CampaignID
        /// </summary>
        /// <param name="campaignID"></param>
        /// <returns>boolean if  CampaginID was successfully validated</returns>
        public bool setCampaignID(string campaignID)
        {
            bool success = false;
            Campaign.CampaignSoapClient client = new Campaign.CampaignSoapClient();
            Campaign.XMPUser user = new Campaign.XMPUser();
            user.Username = this.username;
            user.Password = this.password;
            //validates the campaign exists
            success = client.IsExist(user, campaignID);
            if (success)
            {
                this.campaignID = campaignID;
            }
            return success;
        }

        /// <summary>
        /// validates and sets the given DocumentID
        /// </summary>
        /// <param name="docID"></param>
        /// <returns>boolean if  DocumentID was successfully validated</returns>
        public bool setDocumentID(string docID)
        {
            bool success = false;
            Document.DocumentSoapClient client = new Document.DocumentSoapClient();
            Document.XMPUser user = new Document.XMPUser();
            user.Username = this.username;
            user.Password = this.password;
            //validates doc exists
            success = client.IsExist(user, docID);
            if (success)
            {
                this.documentID = docID;
            }
            return success;
        }

        /// <summary>
        /// uploades a single file to the set campaign, throws error if accountID, campaignID, or documentID have not been set
        /// </summary>
        /// <param name="filePath">the full path of the data source to upload</param>
        /// <returns>the ID of the new data source</returns>
        public string uploadData(string filePath)
        {
            //validte account/campaign/docID have been set
            this.validateSettings();

            //create another user becuase who needs inheritence 
            DataSource.XMPUser user = new DataSource.XMPUser();
            user.Username = this.username;
            user.Password = this.password;

            //upload to WFOTemp first then use that new path as the datasource
            string tempPath = this.uploadDataToTempFolder(new string[] { filePath });

            //params for SOAP call
            string dataSourceName = Path.GetFileName(filePath);
            string dataSourceType = "TXT";
            string conStr = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=;Extended Properties=text;";
            string additionalParams = Path.GetFileName(filePath);

            DataSource.DataSourceSoapClient client = new DataSource.DataSourceSoapClient();

            //try to createNew if exception is thrown, then most likely a dataSource with the same name exists - replace old data with new
            string id = "";
            try
            {
                id = client.CreateNew(user, this.campaignID, dataSourceType, dataSourceName, conStr, additionalParams, tempPath, false, null);
            }
            catch
            {
                string fname = Path.GetFileName(filePath);
                id = client.GetID(user, this.campaignID, fname);
                client.Delete(user, id);
                id = client.CreateNew(user, this.campaignID, dataSourceType, dataSourceName, conStr, additionalParams, tempPath, false, null);
            }

            //delete temp
            this.deleteTempFolder(tempPath);

            return id;
        }

        /// <summary>
        /// Uploadeds multiple datasources to the set campaign, throws error if accountID, campaignID, or documentID have not been set
        /// </summary>
        /// <param name="filePaths">array of data source paths to upload</param>
        /// <returns>dictionary of data source IDs with the key being the filename</returns>
        public Dictionary<string, string> uploadData(string[] filePaths)
        {
            Dictionary<string, string> dataMapping = new Dictionary<string, string>();

            foreach (string s in filePaths)
            {
                string id = this.uploadData(s);
                if (id.Length > 0)
                {
                    dataMapping.Add(Path.GetFileName(s), id);
                }
            }
            return dataMapping;
        }

        /// <summary>
        /// gets all associated Schemas for the set Campaign, throws error if accountID, campaignID, or documentID have not been set 
        /// </summary>
        /// <returns>array of all schmeas in the plan</returns>
        public string[] getSchemas()
        {
            //validte account/campaign/docID have been set
            this.validateSettings();

            JobTicket.JobTicketSoapClient client = new JobTicket.JobTicketSoapClient();
            JobTicket.XMPUser user = new JobTicket.XMPUser();
            user.Username = this.username;
            user.Password = this.password;
            string ticketID = client.CreateNewTicketForDocument(user, this.documentID, "", false);
            string[] schemas = client.GetSchemasNames(user, ticketID);
            client.RecycleTicketID(user, ticketID);
            return schemas;
        }

        /// <summary>
        /// submits a job to the uProduce server
        /// </summary>
        /// <param name="dataDictionary">Dictionary that contains the mapping between schema values and datatSource IDs, MUST CONTAIN RecipientInfo KEY AND VALUE FOR IT'S DATASOURCE</param>
        /// <param name="batchQTY">Quantity per batch</param>
        /// <param name="outputFileName">Filename for output</param>
        /// <param name="destinationID">ID of output folder</param>
        /// <param name="makeVIPPS">indicates if VIPSS should be produced</param>
        /// <returns>JobID for submitted job</returns>
        public string[] processJob(Dictionary<string, string> dataDictionary, int batchQTY, string outputFileName, string destinationID, bool makeVIPPS)
        {
            //ensure RecipInfo has been set in dictionary
            if (!dataDictionary.ContainsKey("RecipientInfo"))
            {
                throw new Exception("Dictionary must contain RecipientInfo key");
            }

            string riTableID = "";
            string[] tables = null;
            foreach (string key in dataDictionary.Keys)
            {
                if (key.Equals("RecipientInfo"))
                {
                    dataDictionary.TryGetValue(key, out riTableID);
                    DataSource.DataSourceSoapClient dclient = new DataSource.DataSourceSoapClient();
                    DataSource.XMPUser duser = new DataSource.XMPUser();
                    duser.Password = this.password;
                    duser.Username = this.username;
                    tables = dclient.GetTablesNames(duser, riTableID, true);
                }
            }



            //make the job ticket
            JobTicket.JobTicketSoapClient client = new JobTicket.JobTicketSoapClient();
            JobTicket.XMPUser user = new JobTicket.XMPUser();
            user.Username = this.username;
            user.Password = this.password;
            string ticketID = client.CreateNewTicketForDocument(user, this.documentID, tables[0], false);
            //client.SetTicketDefaultsForDocument(user, ticketID, this.documentID, "", false);
            //client.SetTicketDefaultsForCampaign(user, ticketID, this.campaignID, "", false);
            client.SetOutputFileName(user, ticketID, outputFileName);
            client.SetJobType(user, ticketID, "1");
            client.SetOutputMedia(user, ticketID, 1);
            client.AddDestinationByID(user, ticketID, destinationID, "", false);
            
            //TODO: set the jobticket to split...somehow
            
            JobTicket.RecipientsInfo info = client.GetNthRIInfo(user, ticketID, 0);
            
            //set info for making VIPPS or PDF outputType
            if (makeVIPPS)
            {
                client.SetOutputType(user, ticketID, "VIPP");
                client.AddCompression(user, ticketID, outputFileName, true);
            }
            else
            {
                client.SetOutputType(user, ticketID, "PDFO");
            }

            //loop through the keys
            foreach (string key in dataDictionary.Keys)
            {
                string value = "";
                if (key.Equals("RecipientInfo"))
                {
                    dataDictionary.TryGetValue(key, out value);
                    client.SetRIByID(user, ticketID, info, value);
                    JobTicket.RecipientsInfo ri = new JobTicket.RecipientsInfo();
                    ri.m_Filter = value;
                    ri.m_FilterType = 3;
                    ri.m_From = 1;
                    ri.m_To = -1;
                    //client.AddRIByID(user, ticketID, ri, value);
                    //TODO:  default RI - change table to current datasource or something like that
                    
                    //client.AddDefaultRI(user, ticketID, this.campaignID, tables[0], false);
                    //client.SetTicketDefaultsForDocument(user, ticketID, this.documentID, tables[0], false);
                }
                else
                {
                    dataDictionary.TryGetValue(key, out value);
                    client.SetDataSourceByID(user, ticketID, key, value);
                }
            }

            Production.ProductionSoapClient prodClient = new Production.ProductionSoapClient();
            Production.XMPUser pUser = new Production.XMPUser();
            pUser.Username = this.username;
            pUser.Password = this.password;

            if (client.IsSplittedJob(user, ticketID))
            {
                return prodClient.SubmitSplittedJob(pUser, ticketID, "0", batchQTY.ToString(), "0", "");
            }
            else
            {
                string id = prodClient.SubmitJob(pUser, ticketID, "0", "");
                return new string[] { id };
            }            
        }

        /// <summary>
        /// gets any error messages from uProduce with the associated JobID
        /// </summary>
        /// <param name="JobID"></param>
        /// <returns>array of error messages</returns>
        public string[] getErrorMessage(string JobID)
        {
            Job.JobSoapClient client = new Job.JobSoapClient();
            Job.XMPUser user = new Job.XMPUser();
            user.Username = this.username;
            user.Password = this.password;
            return client.GetMessages(user, JobID);
        }

        /// <summary>
        /// gets status of submitted job  
        /// </summary>
        /// <param name="JobID"></param>
        /// <returns>description of status from uProduce</returns>
        public string getJobStatus(string JobID)
        {
            Job.JobSoapClient client = new Job.JobSoapClient();
            Job.XMPUser user = new Job.XMPUser();
            user.Username = this.username;
            user.Password = this.password;
            int status = client.GetStatus(user, JobID);
            string stat = "";
            switch (status)
            {
                case 1: stat = "Waiting"; break;
                case 2: stat = "InProgress"; break;
                case 3: stat = "Completed"; break;
                case 4: stat = "Failed"; break;
                case 5: stat = "Aborting"; break;
                case 6: stat = "Aborted"; break;
                case 7: stat = "Deployed"; break;
                case 8: stat = "Suspended"; break;
            }
            return stat;
        }

        /// <summary>
        /// uploades the specified assets to the current campaign
        /// </summary>
        /// <param name="assetPath">full network path of file to uplaod</param>
        /// <param name="folderName">the desired name of the new folder to store the assets</param>
        public void uploadAssest(string assetPath, string folderName)
        {
            this.validateSettings();
            //copy of the zip to 02 so it can be uploaded
            string tempPath = this.uploadDataToTempFolder(new string[] { assetPath }); 
            string tempFilePath = tempPath + @"\" + Path.GetFileName(assetPath);
            AssetSource.AssetSourceSoapClient client = new AssetSource.AssetSourceSoapClient();
            //user yet again
            AssetSource.XMPUser user = new AssetSource.XMPUser();
            user.Username = this.username;
            user.Password = this.password;
            //paramater crap for uproduce
            AssetSource.AssetSourceParameter p = new AssetSource.AssetSourceParameter();
            p.m_Name = folderName;
            p.m_Value = "LOCAL";
            AssetSource.AssetSourceParameter[] paramters = new AssetSource.AssetSourceParameter[]{p};

            string assetSourceID = client.CreateNewEx(user, this.campaignID, "LOCAL", folderName, paramters, null);
            
            //upload client
            Asset.AssetSoapClient aClient = new Asset.AssetSoapClient();
            //more users 
            Asset.XMPUser aUser = new Asset.XMPUser();
            aUser.Username = this.username;
            aUser.Password = this.password;


            //try to createNew if exception is thrown, then assets already exist, delete them and reupload
            try
            {
                if (Path.GetExtension(assetPath).Equals(".zip"))
                {
                    aClient.CreateNewFromZip(aUser, assetSourceID, tempFilePath, false, false, null);
                }
                else
                {
                    aClient.CreateNew(aUser, assetSourceID, tempFilePath, false, false, null);
                }
            }
            catch
            {
                client.DeleteAllAssets(user, assetSourceID);
                if (Path.GetExtension(assetPath).Equals(".zip"))
                {
                    aClient.CreateNewFromZip(aUser, assetSourceID, tempFilePath, false, false, null);
                }
                else
                {
                    aClient.CreateNew(aUser, assetSourceID, tempFilePath, false, false, null);
                }
            }
            finally
            {
                this.deleteTempFolder(tempPath);
            }
        }

        /// <summary>
        /// returns the document name for a given ID
        /// </summary>
        /// <param name="id"></param>
        /// <returns>name associated with ID or blank if ID is not valid</returns>
        public string getTemplateName(int id)
        {
            try
            {
                Document.DocumentSoapClient client = new Document.DocumentSoapClient();
                Document.XMPUser dUser = new Document.XMPUser();
                dUser.Username = this.username;
                dUser.Password = this.password;
                return client.GetName(dUser, id.ToString());
            }
            catch
            {
                return "";
            }
        }

         /// <summary>
         /// returns the campaign name for a given id
         /// </summary>
         /// <param name="id"></param>
        /// <returns>name associated with ID or blank if ID is not valid</returns>
        public string getCampaignName(int id)
        {
            try
            {
                Campaign.CampaignSoapClient client = new Campaign.CampaignSoapClient();
                Campaign.XMPUser user = new Campaign.XMPUser();
                user.Username = this.username;
                user.Password = this.password;
                Campaign.Property prop = client.GetProperty(user, id.ToString(), "campaignName");
                return prop.m_Value;
            }
            catch
            {
                return "";
            }
        }

         /// <summary>
         /// returns the account name for a given ID
         /// </summary>
         /// <param name="id"></param>
         /// <returns>name associated with ID or blank if ID is not valid</returns>
        public string getAccountName(int id)
        {
            try
            {
                Account.AccountSoapClient client = new Account.AccountSoapClient();
                Account.XMPUser user = new Account.XMPUser();
                user.Username = this.username;
                user.Password = this.password;
                Account.Property prop = client.GetProperty(user, id.ToString(), "accountName");
                return prop.m_Value;
            }
            catch
            {
                return "";
            }
        }

        private void validateSettings()
        {
            if (this.accountID.Length == 0 || this.campaignID.Length == 0 || this.documentID.Length == 0)
            {
                throw new Exception("Must set AccountID, CampaignID, and DocumentID, before uploading data");
            }
        }

        private string uploadDataToTempFolder(string[] filePaths)
        {
            string guid = Guid.NewGuid().ToString();
            string tempPath = Path.Combine(@"\\oh50ms02\XMPie\WFOTemp", guid);
            if (!Directory.Exists(tempPath))
            {
                Directory.CreateDirectory(tempPath);
            }
            foreach (string s in filePaths)
            {
                string fileName = Path.GetFileName(s);
                string saveToPath = Path.Combine(tempPath, fileName);
                File.Copy(s, saveToPath);
            }

            return tempPath;
        }

        private void deleteTempFolder(string tempPath)
        {
            foreach (string s in Directory.GetFiles(tempPath))
            {
                File.Delete(s);
            }
            Directory.Delete(tempPath);

        }
    }
}
