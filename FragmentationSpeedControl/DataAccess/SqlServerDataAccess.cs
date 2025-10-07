using Microsoft.Practices.EnterpriseLibrary.Data;
using Microsoft.Practices.EnterpriseLibrary.Data.Sql;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace FragmentationSpeedControl.DataAccess
{
    public class SqlServerDataAccess
    {
        public Database Db { get; set; }

        public SqlServerDataAccess()
        {
            string dbName = "EUROMSG";
            string connectionString = $@"Server=(localdb)\MSSQLLocalDB;Database={dbName};Trusted_Connection=True;";

            var initializer = new DatabaseInitializer(connectionString, dbName);
            initializer.RunInitialSetup();

            Db = new SqlDatabase(connectionString);
        }

        public int DoBulkInsert(string customerPoolName, string bulkFile, string bulkFileInInbox, string viralBulkFile)
        {
            Stopwatch sw = Stopwatch.StartNew();
            DbCommand dbComm = Db.GetStoredProcCommand("dbo.usp_campaignmanager_bulk_insert", customerPoolName, bulkFile, bulkFileInInbox, viralBulkFile);
            dbComm.CommandTimeout = 2100;
            Db.ExecuteNonQuery(dbComm);
            sw.Stop();
            return Convert.ToInt32(sw.Elapsed.TotalSeconds);
        }

        public DataSet ExecuteDataSet(string commandText, IReadOnlyList<DbParameter> parameters = null)
        {
            using DbCommand dbComm = Db.GetSqlStringCommand(commandText);
            if (parameters != null && parameters.Count > 0)
                foreach (var param in parameters)
                    dbComm.Parameters.Add(param);
            return Db.ExecuteDataSet(dbComm);
        }

        public void DeleteAllCustomerManager()
        {
            string commandText = "TRUNCATE TABLE EUROMSG.dbo.EMAILS_POOL_HIGH";
            using DbCommand dbCommand = Db.GetSqlStringCommand(commandText);
            Db.ExecuteNonQuery(dbCommand);
        }

        public DataSet FragmentationRate()
        {
            Stopwatch sw = Stopwatch.StartNew();
            string commandText = @"SELECT i.name, ips.avg_fragmentation_in_percent, ips.page_count FROM sys.indexes i
                            JOIN sys.dm_db_index_physical_stats(DB_ID(), OBJECT_ID(N'dbo.EMAILS_POOL_HIGH'), NULL, NULL, 'LIMITED') ips
                            ON ips.object_id = i.object_id AND ips.index_id = i.index_id
                            WHERE i.object_id = OBJECT_ID(N'dbo.EMAILS_POOL_HIGH')";
            var ds = ExecuteDataSet(commandText, null);
            sw.Stop();
            Console.WriteLine($"Fragmentation oran bilgileri gelme süresi: {sw.Elapsed.TotalSeconds}");
            return ds;
        }

        public string[] SelectNextMailsCampId(int top)
        {
            string commandText = $"SELECT TOP ({top}) MAIL_ID FROM dbo.EMAILS_POOL_HIGH AS E WITH (ROWLOCK, READPAST, UPDLOCK, INDEX = IX_EMAILS_POOL_HIGH_STATUS_MAIL_ID_FILTERED) WHERE  E.STATUS = 'Q'";

            var ds = ExecuteDataSet(commandText);

            string[] mailIdValues = ds.Tables[0].AsEnumerable().Select(r => r["MAIL_ID"].ToString()).ToArray();

            return mailIdValues;
        }

        public void UpdateStatus(string[] mails, string status)
        {

            List<string> mailIds = mails.Select(p => $"'{p}'").ToList();

            string commandText = $"UPDATE dbo.EMAILS_POOL_HIGH SET STATUS = '{status}' WHERE MAIL_ID IN ({string.Join(",", mailIds)})";

            using DbCommand command = Db.GetSqlStringCommand(commandText);

            var result = Db.ExecuteNonQuery(command);
        }

        public void IndexsRebuild()
        {
            string commandTextIndexName = @"SELECT i.name AS IndexName, t.name AS TableName
                                            FROM sys.indexes AS i
                                            INNER JOIN sys.tables AS t ON i.object_id = t.object_id
                                            WHERE t.name = 'EMAILS_POOL_HIGH' AND i.type_desc IN ('CLUSTERED', 'NONCLUSTERED') AND i.is_disabled = 0 AND i.is_hypothetical = 0 AND i.name IS NOT NULL";

            var dsIndexInfo = ExecuteDataSet(commandTextIndexName, null);

            Stopwatch sw = Stopwatch.StartNew();

            foreach (DataRow indexInfo in dsIndexInfo.Tables[0].Rows)
            {
                
                string commandText = $@" ALTER INDEX [{indexInfo[0]}] ON [dbo].[{indexInfo[1]}] REBUILD PARTITION = ALL WITH (PAD_INDEX = OFF
                                 , STATISTICS_NORECOMPUTE = OFF
                                 , SORT_IN_TEMPDB = OFF
                                 , ALLOW_ROW_LOCKS = ON
                                 , ALLOW_PAGE_LOCKS = ON
                                 , FILLFACTOR = 75
                                 , MAXDOP = 2)";
                DbCommand command = Db.GetSqlStringCommand(commandText);
                command.CommandTimeout = 1500;
                Db.ExecuteNonQuery(command);
                
            }
            sw.Stop();
            Console.WriteLine($"Total Index rebuild = {sw.Elapsed.TotalSeconds}");
        }

        //public (DataSet ds, int elapsedTime) SelectByCampId()
        //{
        //    Stopwatch sw = Stopwatch.StartNew();
        //    string commandText = $"SELECT * FROM dbo.EMAILS_POOL_HIGH with(nolock) WHERE CAMP_ID= '324E3102A2C6468AA2EA95345FB1B68E' ";
        //    var ds = ExecuteDataSet(commandText, null);
        //    sw.Stop();
        //    return (ds, Convert.ToInt32(sw.Elapsed.TotalSeconds));
        //}
    }
}
