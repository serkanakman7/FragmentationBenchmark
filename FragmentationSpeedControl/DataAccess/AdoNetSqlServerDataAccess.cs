using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Practices.EnterpriseLibrary.Data;

namespace FragmentationSpeedControl.DataAccess
{
    public class AdoNetSqlServerDataAccess
    {
        public SqlConnection Connection { get; set; }

        public async Task<SqlConnection> GetConnectionAsync()
        {
            if (Connection.State != ConnectionState.Open)
                await Connection.OpenAsync();
            return Connection;
        }

        public async Task<DbCommand> GetCommand(string commandText, CommandType commandType)
        {
            if (Connection.State != ConnectionState.Open)
                await Connection.OpenAsync();
            return new SqlCommand(commandText, Connection) { CommandType = commandType };
        }

        public async Task<int> ExecuteNonQueryAsync(string commandText, IReadOnlyList<SqlParameter> parameters, CommandType commandType = CommandType.Text)
        {
            await using var connection = await GetConnectionAsync();
            await using var command = new SqlCommand(commandText, connection) { CommandType = commandType };

            if (parameters != null && parameters.Count > 0)
                command.Parameters.AddRange(parameters.ToArray());

            return await command.ExecuteNonQueryAsync();
        }

        public async Task<object> ExecuteScalarAsync(string commandText, IReadOnlyList<SqlParameter> parameters, CommandType commandType = CommandType.Text)
        {
            await using var connection = await GetConnectionAsync();
            await using var command = new SqlCommand(commandText, connection) { CommandType = commandType };

            if (parameters != null && parameters.Count > 0)
                command.Parameters.AddRange(parameters.ToArray());

            return await command.ExecuteScalarAsync();
        }

        public async Task<DataSet> ExecuteDataSetAsync(string commandText, IReadOnlyList<SqlParameter> parameters, CommandType commandType = CommandType.Text)
        {
            var ds = new DataSet();
            await using var connection = await GetConnectionAsync();
            await using var command = new SqlCommand(commandText, connection) { CommandType = commandType };

            if (parameters != null && parameters.Count > 0)
                command.Parameters.AddRange(parameters.ToArray());

            using var da  = new SqlDataAdapter(command);

            da.Fill(ds);

            return ds;
        }
    }
}

