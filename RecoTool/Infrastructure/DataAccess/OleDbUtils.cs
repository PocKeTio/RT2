using System;
using System.Data.OleDb;
using System.Threading;
using System.Threading.Tasks;
using RecoTool.Infrastructure.Logging;

namespace RecoTool.Services.Helpers
{
    public static class OleDbUtils
    {
        public static async Task OpenWithTimeoutAsync(OleDbConnection connection, TimeSpan timeout, CancellationToken token, string diagCountryId = null, bool isNetwork = false)
        {
            if (connection == null) throw new ArgumentNullException(nameof(connection));
            var openTask = connection.OpenAsync();
            var completed = await Task.WhenAny(openTask, Task.Delay(timeout, token)) == openTask;
            if (!completed)
            {
                var kind = isNetwork ? "network" : "local";
                try { DiagLog.Info("OleDbUtils", $"[{diagCountryId ?? "-"}] Timeout opening {kind} DB after {timeout.TotalSeconds}s"); } catch { }
                try { connection.Close(); } catch { }
                try { connection.Dispose(); } catch { }
                throw new TimeoutException($"Timeout opening {kind} database connection after {timeout.TotalSeconds}s");
            }
            await openTask; // propagate exceptions if any
        }

        public static async Task<OleDbConnection> OpenConnectionWithTimeoutAsync(string connectionString, TimeSpan timeout, CancellationToken token, string diagCountryId = null, bool isNetwork = false)
        {
            if (string.IsNullOrWhiteSpace(connectionString)) throw new ArgumentException("connectionString is required", nameof(connectionString));
            var conn = new OleDbConnection(connectionString);
            await OpenWithTimeoutAsync(conn, timeout, token, diagCountryId, isNetwork);
            return conn;
        }

        public static async Task<DateTime?> GetMaxLastModifiedAsync(OleDbConnection connection, string tableName)
        {
            if (connection == null) throw new ArgumentNullException(nameof(connection));
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentException("tableName is required", nameof(tableName));
            using (var cmd = new OleDbCommand($"SELECT MAX([LastModified]) FROM [{tableName}]", connection))
            {
                var o = await cmd.ExecuteScalarAsync();
                if (o == null || o == DBNull.Value) return null;
                try { return Convert.ToDateTime(o); } catch { return null; }
            }
        }

        public static async Task<long> GetMaxVersionAsync(OleDbConnection connection, string tableName)
        {
            if (connection == null) throw new ArgumentNullException(nameof(connection));
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentException("tableName is required", nameof(tableName));
            using (var cmd = new OleDbCommand($"SELECT MAX([Version]) FROM [{tableName}]", connection))
            {
                var o = await cmd.ExecuteScalarAsync();
                if (o == null || o == DBNull.Value) return -1;
                try { return Convert.ToInt64(o); } catch { return -1; }
            }
        }
    }
}
