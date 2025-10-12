using FragmentationSpeedControl.Utilities;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FragmentationSpeedControl.DataAccess
{
    public class FileDataAccess
    {
        private readonly string _filePath;
        public FileDataAccess(string filePath)
        {
            _filePath = filePath;
        }
        public async Task<DataTable> BulkInsertFromTxtAsync()
        {
            DataTable dt = DataTableHelpers.CreateEmailPoolHighTableSchema();

            using (StreamReader reader = new StreamReader(_filePath))
            {
                string line;
                while ((line = await reader.ReadLineAsync()) != null)
                {
                    if (string.IsNullOrWhiteSpace(line)) continue;

                    string[] values = line.Split('|');

                    DataRow row = dt.NewRow();

                    row["MAIL_ID"] = values[0];
                    row["CAMP_ID"] = values[1];
                    row["CUSTOMER_ID"] = values[2];
                    row["EMAIL"] = values[3];
                    row["MEMBER_ID"] = values[4];
                    row["STATUS"] = values[5];
                    row["PRIORITY"] = int.Parse(values[6]);
                    row["TAKEN_BY_ENGINE"] = values[7];
                    row["ERROR_COUNT"] = int.Parse(values[8]);
                    row["ERROR_MESSAGE"] = values[9];
                    row["CREATED"] = DateTime.Parse(values[10]);
                    row["TAKEN_FOR_SENT"] = (values[11] == "" || values[11].ToLower() == "null") ? (object)DBNull.Value : DateTime.Parse(values[11]);
                    row["SMTP_STATUS"] = (values[12] == "" || values[12].ToLower() == "null") ? (object)DBNull.Value : short.Parse(values[12]);
                    row["DELIVERY_STATUS"] = (values[13] == "" || values[13].ToLower() == "null") ? (object)DBNull.Value : values[13];
                    row["PID"] = (values[14] == "" || values[14].ToLower() == "null") ? (object)DBNull.Value : long.Parse(values[14]);
                    row["PASSIVE_MEMBER"] = (values[15] == "" || values[15].ToLower() == "null") ? (object)DBNull.Value : values[15];
                    row["USE_COLUMN_CACHE"] = (values[16] == "" || values[16].ToLower() == "null") ? (object)DBNull.Value : values[16];
                    row["COLUMN1"] = (values[17] == "" || values[17].ToLower() == "null") ? (object)DBNull.Value : values[17];
                    row["COLUMN2"] = (values[18] == "" || values[18].ToLower() == "null") ? (object)DBNull.Value : values[18];
                    row["COLUMN30"] = (values[19] == "" || values[19].ToLower() == "null") ? (object)DBNull.Value : values[19];
                    row["SEED_MEMBER"] = (values[20] == "" || values[20].ToLower() == "null") ? (object)DBNull.Value : values[20];
                    row["QUEUE_TIME"] = (values[21] == "" || values[21].ToLower() == "null") ? (object)DBNull.Value : DateTime.Parse(values[21]);

                    dt.Rows.Add(row);
                }
            }

            return dt;
        }
    }
}
