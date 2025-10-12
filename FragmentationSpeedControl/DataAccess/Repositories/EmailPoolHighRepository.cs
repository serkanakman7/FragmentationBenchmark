using FragmentationSpeedControl.DataAccess.SqlServer;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FragmentationSpeedControl.DataAccess.Repositories
{
    public class EmailPoolHighRepository
    {
        private readonly SqlServerDataAccess _sqlServerDataAccess;
        public EmailPoolHighRepository(SqlServerDataAccess sqlServerDataAccess)
        {
            _sqlServerDataAccess = sqlServerDataAccess;
        }

        public string[] SelectNextMailsCampId(int top)
        {
            string commandText = $"SELECT TOP ({top}) MAIL_ID FROM dbo.EMAILS_POOL_HIGH AS E WITH (ROWLOCK, READPAST, UPDLOCK, INDEX = IX_EMAILS_POOL_HIGH_STATUS_MAIL_ID_FILTERED) WHERE  E.STATUS = 'Q'";

            var ds = _sqlServerDataAccess.ExecuteDataSet(commandText);

            string[] mailIdValues = ds.Tables[0].AsEnumerable().Select(r => r["MAIL_ID"].ToString()).ToArray();

            return mailIdValues;
        }

        public void UpdateStatus(string[] mails, string status)
        {

            List<string> mailIds = mails.Select(p => $"'{p}'").ToList();

            string commandText = $"UPDATE dbo.EMAILS_POOL_HIGH SET STATUS = '{status}' WHERE MAIL_ID IN ({string.Join(",", mailIds)})";

            _sqlServerDataAccess.ExecuteNonQuery(commandText);
        }

        public void UpdateStatusAllQ(string status)
        {

            string commandText = $"UPDATE TOP(4000000) dbo.EMAILS_POOL_HIGH SET STATUS = '{status}' WHERE STATUS <> '{status}'";

            _sqlServerDataAccess.ExecuteNonQuery(commandText);
        }

        public void DeleteAllCustomerManager()
        {
            string commandText = $"Drop TABLE dbo.EMAILS_POOL_HIGH";
            _sqlServerDataAccess.ExecuteNonQuery(commandText);
        }
    }
}
