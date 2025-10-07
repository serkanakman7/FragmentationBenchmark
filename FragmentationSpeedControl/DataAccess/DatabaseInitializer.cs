using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Practices.EnterpriseLibrary.Data;
using Microsoft.Practices.EnterpriseLibrary.Data.Sql;

namespace FragmentationSpeedControl.DataAccess
{
    public class DatabaseInitializer
    {
        private readonly string _connectionString;
        private readonly string _dbName;

        public DatabaseInitializer(string connectionString, string dbName)
        {
            _connectionString = connectionString;
            _dbName = dbName;
        }

        public void RunInitialSetup()
        {
            EnsureDatabase();
            var db = new SqlDatabase(_connectionString);
            EnsureTables(db);
            EnsureIndexes(db);
            EnsureStoredProcedures(db);
        }

        private void EnsureDatabase()
        {
            string masterConnectionString = @"Server=(localdb)\MSSQLLocalDB;Database=master;Trusted_Connection=True;";

            var masterDb = new SqlDatabase(masterConnectionString);
            {
                string checkDbQuery = $"SELECT database_id FROM sys.databases WHERE name = N'{_dbName}'";
                if (masterDb.ExecuteScalar(masterDb.GetSqlStringCommand(checkDbQuery)) == null)
                {
                    string createDbQuery = $"CREATE DATABASE [{_dbName}]";
                    masterDb.ExecuteNonQuery(masterDb.GetSqlStringCommand(createDbQuery));
                }
            }
        }

        private void EnsureTables(Database db)
        {
            string tableName = "dbo.EMAILS_POOL_HIGH";
            string checkTableQuery = $"SELECT OBJECT_ID(N'{tableName}')";

            var result = db.ExecuteScalar(db.GetSqlStringCommand(checkTableQuery));

            if (result == null || result == DBNull.Value)
            {
                string createTableAndIndexScript = $@"
                    CREATE TABLE [dbo].[EMAILS_POOL_HIGH](
                    	[MAIL_ID] [char](32) NOT NULL,
                    	[CAMP_ID] [char](32) NOT NULL,
                    	[CUSTOMER_ID] [char](32) NOT NULL,
                    	[EMAIL] [varchar](80) NOT NULL,
                    	[MEMBER_ID] [char](32) NOT NULL,
                    	[STATUS] [char](1) NOT NULL CONSTRAINT DF_EMAILS_HIGH_HIGH_STATUS DEFAULT('R'),
                    	[PRIORITY] [int] NOT NULL CONSTRAINT [DF_EMAILS_HIGH_HIGH_PRIORITY]  DEFAULT ((0)),
                    	[TAKEN_BY_ENGINE] [varchar](20) NOT NULL CONSTRAINT [DF_EMAILS_POOL_HIGH_TAKEN_BY_ENGINE]  DEFAULT (''),
                    	[ERROR_COUNT] [int] NOT NULL CONSTRAINT [DF_EMAILS_HIGH_ERROR_COUNT]  DEFAULT ((0)),
                    	[ERROR_MESSAGE] [varchar](200) NOT NULL CONSTRAINT [DF_EMAILS_HIGH_ERROR_MESSAGE]  DEFAULT (''),
                    	[CREATED] [datetime] NOT NULL CONSTRAINT [DF_EMAILS_HIGH_CREATED]  DEFAULT (getdate()),
                    	[TAKEN_FOR_SENT] [datetime] NULL,
                    	[SMTP_STATUS] [smallint] NULL,
                    	[DELIVERY_STATUS] [char](2) NULL,
                    	[PID] [bigint] NULL,
                    	[PASSIVE_MEMBER] [char](1) NULL,
                    	[USE_COLUMN_CACHE] [char](1) NULL,
                    	[COLUMN1] [varchar](100) NULL,
                    	[COLUMN2] [varchar](100) NULL,
                    	[COLUMN30] [varchar](100) NULL,
                    	[SEED_MEMBER] [varchar](1) NULL CONSTRAINT [DF_EMAILS_POOL_HIGH_SEED_MEMBER]  DEFAULT ('N'),
                    	[QUEUE_TIME] [datetime] NULL
                    ) ON [PRIMARY]

                    EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'R: Ready, T : Taken, E : Error' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'EMAILS_POOL_HIGH', @level2type=N'COLUMN',@level2name=N'STATUS'";
                db.ExecuteNonQuery(db.GetSqlStringCommand(createTableAndIndexScript));
            }
        }

        private void EnsureIndexes(SqlDatabase db)
        {
            var indexScripts = new[]
            {
            @"IF NOT EXISTS (SELECT name FROM sys.indexes WHERE name = 'IX_EMAILS_POOL_HIGH_CAMP_STATUS_MAIL_ID')
              CREATE NONCLUSTERED INDEX [IX_EMAILS_POOL_HIGH_CAMP_STATUS_MAIL_ID] 
              ON [dbo].[EMAILS_POOL_HIGH]([CAMP_ID] ASC, [STATUS] ASC, [MAIL_ID] ASC)
              WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 75, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]",

            @"IF NOT EXISTS (SELECT name FROM sys.indexes WHERE name = 'IX_EMAILS_POOL_HIGH_MAIL_ID')
              CREATE UNIQUE NONCLUSTERED INDEX [IX_EMAILS_POOL_HIGH_MAIL_ID] 
              ON [dbo].[EMAILS_POOL_HIGH]([MAIL_ID] ASC)
              INCLUDE([STATUS],[TAKEN_FOR_SENT])
              WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 75, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]",

            @"IF NOT EXISTS (SELECT name FROM sys.indexes WHERE name = 'IX_EMAILS_POOL_HIGH_STATUS_MAIL_ID')
              CREATE NONCLUSTERED INDEX [IX_EMAILS_POOL_HIGH_STATUS_MAIL_ID] 
              ON [dbo].[EMAILS_POOL_HIGH]([STATUS] DESC, [CREATED] ASC)
              INCLUDE([MAIL_ID])
              WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 75, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]",

            @"IF NOT EXISTS (SELECT name FROM sys.indexes WHERE name = 'IX_EMAILS_POOL_HIGH_STATUS_MAIL_ID_FILTERED')
              CREATE NONCLUSTERED INDEX [IX_EMAILS_POOL_HIGH_STATUS_MAIL_ID_FILTERED] 
              ON [dbo].[EMAILS_POOL_HIGH]([STATUS] ASC)
              INCLUDE([MAIL_ID])
              WHERE ([STATUS] IN ('Q','W'))
              WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 75, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]"
        };

            foreach (var script in indexScripts)
            {
                db.ExecuteNonQuery(db.GetSqlStringCommand(script));
            }
        }

        private void EnsureStoredProcedures(Database db)
        {
            string spName = "dbo.usp_campaignmanager_bulk_insert";
            string checkSpQuery = $"SELECT OBJECT_ID(N'{spName}', 'P')";

            var result = db.ExecuteScalar(db.GetSqlStringCommand(checkSpQuery));

            if (result == null || result == DBNull.Value)
            {
                string createSpScript = $@"
                    CREATE PROCEDURE [dbo].[usp_campaignmanager_bulk_insert]
                    @poolName varchar(10),
                    @flName varchar(255),
                    @flNameInInbox varchar(255),
                    @flViralName varchar(255)
                    AS

                    SET DEADLOCK_PRIORITY HIGH;

                    declare @vSqlString nvarchar(2000);
                    declare @vResult int

                    if (@flName <> '')
                    begin
                    -- bulk insert into e-mail pool
                    set @vSqlString = N'BULK INSERT [EMAILS_POOL_' + @poolName + '] FROM ''' + @flName + N'''' +  CHAR(13) + 
                 N'WITH ( FIELDTERMINATOR = ''|'', ROWTERMINATOR = ''|' + CHAR(10) + ''', MAXERRORS  =  0, BATCHSIZE = 1000, CODEPAGE=''RAW'')';

                    exec @vResult = sp_executesql @vSqlString
                    if (@@error <> 0) goto RollbackAndReturn 

                    if ( @flViralName <> '')
                    begin
                    -- bulk insert into viral data pool
                    SET @vSqlString = N'BULK INSERT [CAMP_VRL_DATA]  FROM ''' + @flViralName + N'''' +  CHAR(13) + 
                                N'WITH ( FIELDTERMINATOR = ''|'', ROWTERMINATOR = ''|\n'', MAXERRORS  =  0, BATCHSIZE = 1000)';

                    EXEC @vResult = sp_executesql @vSqlString
                    if (@@error <> 0) goto RollbackAndReturn 
                    end 
                    end

                    if ( @flNameInInbox <> '')
                    begin
                    declare @isProd bit
                    set @isProd = (select top 1 IS_PROD from dbo.MASTER with(nolock))
                    if (@isProd = 1)
                    begin 
                    -- prod
                    set @vSqlString = N'BULK INSERT {_dbName}.dbo.IN_INBOX_WAIT_QUEUE FROM ''' + @flNameInInbox + N'''' +  CHAR(13) + 
                                N'WITH ( FIELDTERMINATOR = ''|'', ROWTERMINATOR = ''|\n'', MAXERRORS  =  0, BATCHSIZE = 1000, CODEPAGE=''RAW'')';
                    end
                    else
                    begin 
                    -- test
                    set @vSqlString = N'BULK INSERT T_{_dbName}.dbo.IN_INBOX_WAIT_QUEUE FROM ''' + @flNameInInbox + N'''' +  CHAR(13) + 
                                N'WITH ( FIELDTERMINATOR = ''|'', ROWTERMINATOR = ''|\n'', MAXERRORS  =  0, BATCHSIZE = 1000, CODEPAGE=''RAW'')';
                    end

                    EXEC @vResult = sp_executesql @vSqlString
                    if (@@error <> 0) goto RollbackAndReturn 
                    end

                    return

                    RollbackAndReturn:
                    rollback transaction
                    return";
                db.ExecuteNonQuery(db.GetSqlStringCommand(createSpScript));
            }
        }
    }
}
