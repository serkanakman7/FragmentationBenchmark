using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FragmentationSpeedControl.Utilities
{
    public static class DataTableHelpers
    {
        public static DataTable CreateEmailPoolHighTableSchema()
        {
            DataTable dt = new DataTable();

            dt.Columns.Add("MAIL_ID", typeof(string));
            dt.Columns.Add("CAMP_ID", typeof(string));
            dt.Columns.Add("CUSTOMER_ID", typeof(string));
            dt.Columns.Add("EMAIL", typeof(string));
            dt.Columns.Add("MEMBER_ID", typeof(string));
            dt.Columns.Add("STATUS", typeof(string));
            dt.Columns.Add("PRIORITY", typeof(int));
            dt.Columns.Add("TAKEN_BY_ENGINE", typeof(string));
            dt.Columns.Add("ERROR_COUNT", typeof(int));
            dt.Columns.Add("ERROR_MESSAGE", typeof(string));
            dt.Columns.Add("CREATED", typeof(DateTime));
            dt.Columns.Add("TAKEN_FOR_SENT", typeof(DateTime));
            dt.Columns.Add("SMTP_STATUS", typeof(short));
            dt.Columns.Add("DELIVERY_STATUS", typeof(string));
            dt.Columns.Add("PID", typeof(long));
            dt.Columns.Add("PASSIVE_MEMBER", typeof(string));
            dt.Columns.Add("USE_COLUMN_CACHE", typeof(string));
            dt.Columns.Add("COLUMN1", typeof(string));
            dt.Columns.Add("COLUMN2", typeof(string));
            dt.Columns.Add("COLUMN30", typeof(string));
            dt.Columns.Add("SEED_MEMBER", typeof(string));
            dt.Columns.Add("QUEUE_TIME", typeof(DateTime));

            return dt;
        }
    }
}
