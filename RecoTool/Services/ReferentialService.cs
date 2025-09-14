using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Threading;
using System.Threading.Tasks;

namespace RecoTool.Services
{
    /// <summary>
    /// Service dedicated to referential data access (e.g., T_User, T_param) in the referential database.
    /// Keeps responsibilities out of ReconciliationService.
    /// </summary>
    public class ReferentialService
    {
        private readonly OfflineFirstService _offlineFirstService;
        private readonly string _currentUser;

        public ReferentialService(OfflineFirstService offlineFirstService, string currentUser = null)
        {
            _offlineFirstService = offlineFirstService ?? throw new ArgumentNullException(nameof(offlineFirstService));
            _currentUser = currentUser;
        }

        private string GetReferentialConnectionString()
        {
            var refCs = _offlineFirstService?.ReferentialConnectionString;
            if (string.IsNullOrWhiteSpace(refCs))
                throw new InvalidOperationException("Referential connection string is required (inject OfflineFirstService).");
            return refCs;
        }

        /// <summary>
        /// Get user list from T_User and ensure the current user exists.
        /// </summary>
        public async Task<List<(string Id, string Name)>> GetUsersAsync(CancellationToken cancellationToken = default)
        {
            var list = new List<(string, string)>();
            using (var connection = new OleDbConnection(GetReferentialConnectionString()))
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

                // Ensure current user exists in T_User (USR_ID, USR_Name)
                try
                {
                    if (!string.IsNullOrWhiteSpace(_currentUser))
                    {
                        var checkCmd = new OleDbCommand("SELECT COUNT(*) FROM T_User WHERE USR_ID = ?", connection);
                        checkCmd.Parameters.AddWithValue("@p1", _currentUser);
                        var obj = await checkCmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                        var exists = obj != null && int.TryParse(obj.ToString(), out var n) && n > 0;
                        if (!exists)
                        {
                            var insertCmd = new OleDbCommand("INSERT INTO T_User (USR_ID, USR_Name) VALUES (?, ?)", connection);
                            insertCmd.Parameters.AddWithValue("@p1", _currentUser);
                            insertCmd.Parameters.AddWithValue("@p2", _currentUser);
                            await insertCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                        }
                    }
                }
                catch { /* best effort; not critical */ }

                var cmd = new OleDbCommand("SELECT USR_ID, USR_Name FROM T_User ORDER BY USR_Name", connection);
                using (var rdr = await cmd.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    while (await rdr.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        var id = rdr.IsDBNull(0) ? null : rdr.GetValue(0)?.ToString();
                        var name = rdr.IsDBNull(1) ? null : rdr.GetValue(1)?.ToString();
                        if (!string.IsNullOrWhiteSpace(id))
                            list.Add((id, name ?? id));
                    }
                }
            }
            return list;
        }

        /// <summary>
        /// Reads a SQL payload from referential table T_param.Par_Value using a flexible key lookup.
        /// Accepts keys like Export_KPI, Export_PastDUE, Export_IT.
        /// </summary>
        public async Task<string> GetParamValueAsync(string paramKey, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(paramKey)) return null;

            using (var connection = new OleDbConnection(GetReferentialConnectionString()))
            {
                cancellationToken.ThrowIfCancellationRequested();
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

                // Try common key column names to avoid coupling to a specific schema naming
                string[] keyColumns = { "Par_Key", "Par_Code", "Par_Name", "PAR_Key", "PAR_Code", "PAR_Name" };
                foreach (var col in keyColumns)
                {
                    try
                    {
                        var cmd = new OleDbCommand($"SELECT TOP 1 Par_Value FROM T_param WHERE {col} = ?", connection);
                        cmd.Parameters.AddWithValue("@p1", paramKey);
                        cancellationToken.ThrowIfCancellationRequested();
                        var obj = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                        if (obj != null && obj != DBNull.Value)
                            return obj.ToString();
                    }
                    catch
                    {
                        // Ignore and try next column variant
                    }
                }
            }
            return null;
        }
    }
}
