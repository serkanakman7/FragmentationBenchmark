using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FragmentationSpeedControl.DataAccess.Repositories
{
    public class SourceRepository
    {
        private readonly SqlServerDataAccess _sqlServerDataAccess;

        public SourceRepository(SqlServerDataAccess sqlServerDataAccess)
        {
            _sqlServerDataAccess = sqlServerDataAccess;
        }

        public int InsertSelectSource()
        {
            Stopwatch sw = Stopwatch.StartNew();
            //Tablock fragmentation'ı baya düşürdü
            string commandText = @"INSERT INTO EMAILS_POOL_HIGH WITH (TABLOCK) SELECT * FROM EMAILS_POOL_SOURCE_HIGH ORDER BY NEWID()";
            _sqlServerDataAccess.ExecuteNonQuery(commandText);
            sw.Stop();
            return (int)sw.Elapsed.TotalSeconds;
        }
    }
}
