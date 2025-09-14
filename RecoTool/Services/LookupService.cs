using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace RecoTool.Services
{
    /// <summary>
    /// Lookup service for distinct values used by filter combo boxes (currencies, guarantee statuses/types).
    /// Delegates DB path resolution to OfflineFirstService and executes lightweight scalar queries.
    /// </summary>
    public class LookupService
    {
        private readonly OfflineFirstService _offlineFirstService;

        public LookupService(OfflineFirstService offlineFirstService)
        {
            _offlineFirstService = offlineFirstService ?? throw new ArgumentNullException(nameof(offlineFirstService));
        }

        public async Task<List<string>> GetCurrenciesAsync(string countryId)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(countryId)) return new List<string>();
                var ambrePath = _offlineFirstService?.GetLocalAmbreDatabasePath(countryId);
                if (string.IsNullOrWhiteSpace(ambrePath) || !File.Exists(ambrePath)) return new List<string>();
                var ambreCs = $"Provider=Microsoft.ACE.OLEDB.16.0;Data Source={ambrePath};";
                var query = @"SELECT DISTINCT CCY FROM T_Data_Ambre WHERE DeleteDate IS NULL AND CCY IS NOT NULL AND CCY <> '' ORDER BY CCY";
                var values = await ExecuteScalarListAsync<string>(query, ambreCs).ConfigureAwait(false);
                return values?.Where(s => !string.IsNullOrWhiteSpace(s))
                              .Select(s => s.Trim())
                              .Distinct(StringComparer.OrdinalIgnoreCase)
                              .OrderBy(s => s, StringComparer.OrdinalIgnoreCase)
                              .ToList() ?? new List<string>();
            }
            catch { return new List<string>(); }
        }

        public async Task<List<string>> GetGuaranteeStatusesAsync()
        {
            try
            {
                var dwPath = _offlineFirstService?.GetLocalDWDatabasePath();
                if (string.IsNullOrWhiteSpace(dwPath) || !File.Exists(dwPath)) return new List<string>();
                var dwCs = $"Provider=Microsoft.ACE.OLEDB.16.0;Data Source={dwPath};";
                var query = @"SELECT DISTINCT GUARANTEE_STATUS FROM T_DW_Guarantee WHERE GUARANTEE_STATUS IS NOT NULL AND GUARANTEE_STATUS <> '' ORDER BY GUARANTEE_STATUS";
                var values = await ExecuteScalarListAsync<string>(query, dwCs).ConfigureAwait(false);
                return values?.Where(s => !string.IsNullOrWhiteSpace(s))
                              .Select(s => s.Trim())
                              .Distinct(StringComparer.OrdinalIgnoreCase)
                              .OrderBy(s => s, StringComparer.OrdinalIgnoreCase)
                              .ToList() ?? new List<string>();
            }
            catch { return new List<string>(); }
        }

        public async Task<List<string>> GetGuaranteeTypesAsync()
        {
            try
            {
                var dwPath = _offlineFirstService?.GetLocalDWDatabasePath();
                if (string.IsNullOrWhiteSpace(dwPath) || !File.Exists(dwPath)) return new List<string>();
                var dwCs = $"Provider=Microsoft.ACE.OLEDB.16.0;Data Source={dwPath};";
                var query = @"SELECT DISTINCT GUARANTEE_TYPE FROM T_DW_Guarantee WHERE GUARANTEE_TYPE IS NOT NULL AND GUARANTEE_TYPE <> '' ORDER BY GUARANTEE_TYPE";
                var values = await ExecuteScalarListAsync<string>(query, dwCs).ConfigureAwait(false);
                return values?.Where(s => !string.IsNullOrWhiteSpace(s))
                              .Select(s => s.Trim())
                              .Distinct(StringComparer.OrdinalIgnoreCase)
                              .OrderBy(s => s, StringComparer.OrdinalIgnoreCase)
                              .ToList() ?? new List<string>();
            }
            catch { return new List<string>(); }
        }

        private static async Task<List<T>> ExecuteScalarListAsync<T>(string query, string connectionString, params object[] parameters)
        {
            var results = new List<T>();
            using (var connection = new OleDbConnection(connectionString))
            {
                await connection.OpenAsync().ConfigureAwait(false);
                using (var command = new OleDbCommand(query, connection))
                {
                    if (parameters != null)
                    {
                        for (int i = 0; i < parameters.Length; i++)
                        {
                            command.Parameters.AddWithValue($"@param{i}", parameters[i] ?? DBNull.Value);
                        }
                    }

                    using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                    {
                        while (await reader.ReadAsync().ConfigureAwait(false))
                        {
                            object value = reader.IsDBNull(0) ? null : reader.GetValue(0);
                            if (value == null)
                            {
                                results.Add(default);
                                continue;
                            }

                            try
                            {
                                var targetType = Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T);
                                if (targetType.IsEnum)
                                {
                                    results.Add((T)Enum.Parse(targetType, value.ToString()));
                                }
                                else
                                {
                                    results.Add((T)Convert.ChangeType(value, targetType, CultureInfo.InvariantCulture));
                                }
                            }
                            catch
                            {
                                try { results.Add((T)(object)value.ToString()); } catch { results.Add(default); }
                            }
                        }
                    }
                }
            }
            return results;
        }
    }
}
