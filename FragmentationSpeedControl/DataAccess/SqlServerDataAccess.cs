using System.Data;
using System.Data.Common;
using System.Diagnostics;
using Microsoft.Practices.EnterpriseLibrary.Data;
using Microsoft.Practices.EnterpriseLibrary.Data.Sql;

namespace FragmentationSpeedControl.DataAccess
{
    public class SqlServerDataAccess
    {
        public Database Db { get; set; }

        public SqlServerDataAccess()
        {
            string connectionString = "Server=(localdb)\\MSSQLLocalDB;Database=EUROMSG;Trusted_Connection=True;";
            Db = new SqlDatabase(connectionString);
        }

        public int DoBulkInsert(string customerPoolName, string bulkFile, string bulkFileInInbox, string viralBulkFile)
        {
            Stopwatch sw = Stopwatch.StartNew();
            DbCommand dbComm = Db.GetStoredProcCommand("dbo.usp_campaignmanager_bulk_insert", customerPoolName, bulkFile, bulkFileInInbox, viralBulkFile);
            dbComm.CommandTimeout = 1500; // 25dk kaç olmalı diye bir sor
            Db.ExecuteNonQuery(dbComm);
            sw.Stop();
            return Convert.ToInt32(sw.Elapsed.TotalSeconds);
        }

        public DataSet ExecuteDataSet(string commandText, IReadOnlyList<DbParameter> parameters)
        {
            using DbCommand dbComm = Db.GetSqlStringCommand(commandText);
            if (parameters != null && parameters.Count > 0)
                foreach (var param in parameters)
                    dbComm.Parameters.Add(param);
            return Db.ExecuteDataSet(dbComm);
        }

        public (DataSet ds, int elapsedTime) GetAllCustomerManager()
        {
            Stopwatch sw = Stopwatch.StartNew();
            string commandText = "SELECT * FROM dbo.EMAILS_POOL_HIGH with(nolock)";
            var ds = ExecuteDataSet(commandText, null);
            sw.Stop();
            return (ds, Convert.ToInt32(sw.Elapsed.TotalSeconds));
        }

        public (DataSet ds, int elapsedTime) GetTopTenCustomerManager()
        {
            Stopwatch sw = Stopwatch.StartNew();
            string commandText = "SELECT TOP (10) MAIL_ID FROM dbo.EMAILS_POOL_HIGH AS E WITH (ROWLOCK, READPAST, UPDLOCK, INDEX = IX_EMAILS_POOL_HIGH_STATUS_MAIL_ID_FILTERED) WHERE  E.STATUS = 'Q'";
            var ds = ExecuteDataSet(commandText, null);
            sw.Stop();
            return (ds, Convert.ToInt32(sw.Elapsed.TotalSeconds));
        }

        public (DataSet ds, int elapsedTime) GetCustomerManagerMailId(string mailId)
        {
            Stopwatch sw = Stopwatch.StartNew();
            string commandText = $"SELECT * FROM dbo.EMAILS_POOL_HIGH with(nolock) WHERE MAIL_ID='{mailId}'";
            var ds = ExecuteDataSet(commandText, null);
            sw.Stop();
            return (ds, Convert.ToInt32(sw.Elapsed.TotalSeconds));
        }

        public (DataSet ds, int elapsedTime) GetAllCustomerManagerCampId()
        {
            Stopwatch sw = Stopwatch.StartNew();
            string commandText = $"SELECT * FROM dbo.EMAILS_POOL_HIGH with(nolock) WHERE CAMP_ID= '324E3102A2C6468AA2EA95345FB1B68E' ";
            var ds = ExecuteDataSet(commandText, null);
            sw.Stop();
            return (ds, Convert.ToInt32(sw.Elapsed.TotalSeconds));
        }

        public void DeleteAllCustomerManager()
        {
            string commandText = "TRUNCATE TABLE EUROMSG.dbo.EMAILS_POOL_HIGH";
            Db.ExecuteNonQuery(commandText);
        }

        public DataSet FragmentationRate()
        {
            string commandText = @"SELECT i.name, ips.avg_fragmentation_in_percent, ips.page_count FROM sys.indexes i
                            JOIN sys.dm_db_index_physical_stats(DB_ID(), OBJECT_ID(N'dbo.EMAILS_POOL_HIGH'), NULL, NULL, 'LIMITED') ips
                            ON ips.object_id = i.object_id AND ips.index_id = i.index_id
                            WHERE i.object_id = OBJECT_ID(N'dbo.EMAILS_POOL_HIGH')";
            return ExecuteDataSet(commandText, null);
        }
    }
}
