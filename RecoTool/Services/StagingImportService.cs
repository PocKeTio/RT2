using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.Linq;
using System.Threading.Tasks;
using RecoTool.Models;

namespace RecoTool.Services
{
    /// <summary>
    /// Provides a high-throughput staging-table based import and merge flow for Access.
    /// Focus: set-based UPDATE/INSERT into T_Reconciliation using a staging table to handle 50k+ rows efficiently.
    /// </summary>
    public class StagingImportService
    {
        private readonly string _connectionString;
        private readonly string _currentUser;

        private const string StagingTable = "T_Staging_Reconciliation";
        private const string TargetTable = "T_Reconciliation";

        public StagingImportService(string connectionString, string currentUser)
        {
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            _currentUser = currentUser;
        }

        #region Public API

        /// <summary>
        /// Ensures staging table exists with expected schema and a PK-like index on ID.
        /// </summary>
        public async Task EnsureStagingForReconciliationAsync()
        {
            using (var conn = new OleDbConnection(_connectionString))
            {
                await conn.OpenAsync();
                // Recréer la table de staging à partir du schéma de la table cible (zéro donnée)
                // Cela évite tout drift de schéma et simplifie la maintenance
                if (await TableExistsAsync(conn, StagingTable))
                {
                    await ExecuteNonQueryAsync(conn, $"DROP TABLE [{StagingTable}]");
                }

                var selectInto = $@"SELECT 
                                [ID],[DWINGS_GuaranteeID],[DWINGS_InvoiceID],[DWINGS_CommissionID],
                                [Action],[Comments],[InternalInvoiceReference],[FirstClaimDate],[LastClaimDate],
                                [ToRemind],[ToRemindDate],[ACK],[SwiftCode],[PaymentReference],[KPI],
                                [IncidentType],[RiskyItem],[ReasonNonRisky],[ModifiedBy],[LastModified]
                             INTO [{StagingTable}]
                             FROM [{TargetTable}] 
                             WHERE 1=0";
                await ExecuteNonQueryAsync(conn, selectInto);

                // Create index on ID for fast joins
                try { await ExecuteNonQueryAsync(conn, $"CREATE INDEX IX_{StagingTable}_ID ON [{StagingTable}] ([ID])"); } catch { }
            }
        }

        /// <summary>
        /// Ensures indexes on target table for merge performance (ID).
        /// Safe to call repeatedly.
        /// </summary>
        public async Task EnsureTargetIndexesAsync()
        {
            using (var conn = new OleDbConnection(_connectionString))
            {
                await conn.OpenAsync();
                try { await ExecuteNonQueryAsync(conn, $"CREATE INDEX IX_{TargetTable}_ID ON [{TargetTable}] ([ID])"); } catch { }
            }
        }

        /// <summary>
        /// Deletes all rows from the staging table.
        /// </summary>
        public async Task TruncateStagingAsync()
        {
            using (var conn = new OleDbConnection(_connectionString))
            {
                await conn.OpenAsync();
                if (await TableExistsAsync(conn, StagingTable))
                {
                    await ExecuteNonQueryAsync(conn, $"DELETE FROM [{StagingTable}]");
                }
            }
        }

        /// <summary>
        /// Prototype bulk load: loads a UTF-8 CSV into the staging table using ACE Text driver.
        /// CSV must have headers matching the staging column names.
        /// </summary>
        public async Task<int> BulkLoadCsvIntoStagingAsync(string csvPath)
        {
            if (string.IsNullOrWhiteSpace(csvPath)) throw new ArgumentNullException(nameof(csvPath));

            var dir = System.IO.Path.GetDirectoryName(csvPath);
            var file = System.IO.Path.GetFileName(csvPath);
            if (string.IsNullOrEmpty(dir) || string.IsNullOrEmpty(file))
                throw new ArgumentException("Invalid CSV path", nameof(csvPath));

            using (var conn = new OleDbConnection(_connectionString))
            {
                await conn.OpenAsync();
                // Map columns explicitly to avoid order issues
                var sql = $@"INSERT INTO [{StagingTable}] (
                                [ID],[DWINGS_GuaranteeID],[DWINGS_InvoiceID],[DWINGS_CommissionID],
                                [Action],[Comments],[InternalInvoiceReference],[FirstClaimDate],[LastClaimDate],
                                [ToRemind],[ToRemindDate],[ACK],[SwiftCode],[PaymentReference],[KPI],
                                [IncidentType],[RiskyItem],[ReasonNonRisky],[ModifiedBy],[LastModified]
                             )
                             SELECT 
                                [ID],[DWINGS_GuaranteeID],[DWINGS_InvoiceID],[DWINGS_CommissionID],
                                [Action],[Comments],[InternalInvoiceReference],[FirstClaimDate],[LastClaimDate],
                                [ToRemind],[ToRemindDate],[ACK],[SwiftCode],[PaymentReference],[KPI],
                                [IncidentType],[RiskyItem],[ReasonNonRisky],[ModifiedBy],[LastModified]
                             FROM [Text;HDR=Yes;FMT=Delimited;CharacterSet=65001;Database={dir.Replace("'", "''")}]([{file.Replace("'", "''")}])";

                using (var cmd = new OleDbCommand(sql, conn))
                {
                    return await cmd.ExecuteNonQueryAsync();
                }
            }
        }

        /// <summary>
        /// Fallback: insert rows into staging from memory in chunks inside one transaction.
        /// </summary>
        public async Task<int> InsertStagingRowsAsync(IEnumerable<Reconciliation> rows, int chunkSize = 2000)
        {
            if (rows == null) return 0;
            var list = rows.ToList();
            if (list.Count == 0) return 0;

            int total = 0;
            using (var conn = new OleDbConnection(_connectionString))
            {
                await conn.OpenAsync();
                // Lire les tailles maximales des colonnes texte de la table de staging
                var textSizes = await GetTextColumnMaxLengthsAsync(conn, StagingTable);
                using (var tx = conn.BeginTransaction())
                {
                    try
                    {
                        for (int i = 0; i < list.Count; i += chunkSize)
                        {
                            var chunk = list.Skip(i).Take(chunkSize);
                            foreach (var r in chunk)
                            {
                                using (var cmd = new OleDbCommand($@"INSERT INTO [{StagingTable}] (
                                        [ID],[DWINGS_GuaranteeID],[DWINGS_InvoiceID],[DWINGS_CommissionID],
                                        [Action],[Comments],[InternalInvoiceReference],[FirstClaimDate],[LastClaimDate],
                                        [ToRemind],[ToRemindDate],[ACK],[SwiftCode],[PaymentReference],[KPI],
                                        [IncidentType],[RiskyItem],[ReasonNonRisky],[ModifiedBy],[LastModified]
                                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", conn, tx))
                                {
                                    AddStagingParameters(cmd, r, textSizes);
                                    total += await cmd.ExecuteNonQueryAsync();
                                }
                            }
                        }
                        tx.Commit();
                    }
                    catch
                    {
                        tx.Rollback();
                        throw;
                    }
                }
            }
            return total;
        }

        /// <summary>
        /// Executes set-based UPDATE then INSERT to merge staging rows into T_Reconciliation.
        /// </summary>
        public async Task<(int updated, int inserted)> MergeStagingIntoReconciliationAsync()
        {
            using (var conn = new OleDbConnection(_connectionString))
            {
                await conn.OpenAsync();
                using (var tx = conn.BeginTransaction())
                {
                    try
                    {
                        // UPDATE existing rows using INNER JOIN
                        var updateSql = $@"UPDATE [{TargetTable}] AS t 
                                         INNER JOIN [{StagingTable}] AS s ON t.[ID] = s.[ID]
                                         SET 
                                            t.[DWINGS_GuaranteeID] = s.[DWINGS_GuaranteeID],
                                            t.[DWINGS_InvoiceID] = s.[DWINGS_InvoiceID],
                                            t.[DWINGS_CommissionID] = s.[DWINGS_CommissionID],
                                            t.[Action] = s.[Action],
                                            t.[Comments] = s.[Comments],
                                            t.[InternalInvoiceReference] = s.[InternalInvoiceReference],
                                            t.[FirstClaimDate] = s.[FirstClaimDate],
                                            t.[LastClaimDate] = s.[LastClaimDate],
                                            t.[ToRemind] = s.[ToRemind],
                                            t.[ToRemindDate] = s.[ToRemindDate],
                                            t.[ACK] = s.[ACK],
                                            t.[SwiftCode] = s.[SwiftCode],
                                            t.[PaymentReference] = s.[PaymentReference],
                                            t.[KPI] = s.[KPI],
                                            t.[IncidentType] = s.[IncidentType],
                                            t.[RiskyItem] = s.[RiskyItem],
                                            t.[ReasonNonRisky] = s.[ReasonNonRisky],
                                            t.[ModifiedBy] = s.[ModifiedBy],
                                            t.[LastModified] = s.[LastModified]
                                         ";
                        int updated;
                        using (var cmd = new OleDbCommand(updateSql, conn, tx))
                        {
                            updated = await cmd.ExecuteNonQueryAsync();
                        }

                        // INSERT new rows (ID not present in target)
                        // CreationDate is set to NOW() (OLE Automation date as double)
                        double nowOa = DateTime.Now.ToOADate();
                        var insertSql = $@"INSERT INTO [{TargetTable}] (
                                                [ID],[DWINGS_GuaranteeID],[DWINGS_InvoiceID],[DWINGS_CommissionID],
                                                [Action],[Comments],[InternalInvoiceReference],[FirstClaimDate],[LastClaimDate],
                                                [ToRemind],[ToRemindDate],[ACK],[SwiftCode],[PaymentReference],[KPI],
                                                [IncidentType],[RiskyItem],[ReasonNonRisky],[CreationDate],[ModifiedBy],[LastModified]
                                            )
                                            SELECT 
                                                s.[ID], s.[DWINGS_GuaranteeID], s.[DWINGS_InvoiceID], s.[DWINGS_CommissionID],
                                                s.[Action], s.[Comments], s.[InternalInvoiceReference], s.[FirstClaimDate], s.[LastClaimDate],
                                                s.[ToRemind], s.[ToRemindDate], s.[ACK], s.[SwiftCode], s.[PaymentReference], s.[KPI],
                                                s.[IncidentType], s.[RiskyItem], s.[ReasonNonRisky], ?, s.[ModifiedBy], s.[LastModified]
                                            FROM [{StagingTable}] AS s
                                            LEFT JOIN [{TargetTable}] AS t ON t.[ID] = s.[ID]
                                            WHERE t.[ID] IS NULL";
                        int inserted;
                        using (var cmd = new OleDbCommand(insertSql, conn, tx))
                        {
                            cmd.Parameters.AddWithValue("@CreationDate", nowOa);
                            inserted = await cmd.ExecuteNonQueryAsync();
                        }

                        tx.Commit();
                        return (updated, inserted);
                    }
                    catch
                    {
                        tx.Rollback();
                        throw;
                    }
                }
            }
        }

        /// <summary>
        /// Insert-only merge: inserts rows from staging that do not yet exist in T_Reconciliation.
        /// Does not perform any UPDATE on existing rows.
        /// </summary>
        public async Task<int> InsertMissingFromStagingAsync()
        {
            using (var conn = new OleDbConnection(_connectionString))
            {
                await conn.OpenAsync();
                using (var tx = conn.BeginTransaction())
                {
                    try
                    {
                        double nowOa = DateTime.Now.ToOADate();
                        var insertSql = $@"INSERT INTO [{TargetTable}] (
                                                [ID],[DWINGS_GuaranteeID],[DWINGS_InvoiceID],[DWINGS_CommissionID],
                                                [Action],[Comments],[InternalInvoiceReference],[FirstClaimDate],[LastClaimDate],
                                                [ToRemind],[ToRemindDate],[ACK],[SwiftCode],[PaymentReference],[KPI],
                                                [IncidentType],[RiskyItem],[ReasonNonRisky],[CreationDate],[ModifiedBy],[LastModified]
                                            )
                                            SELECT 
                                                s.[ID], s.[DWINGS_GuaranteeID], s.[DWINGS_InvoiceID], s.[DWINGS_CommissionID],
                                                s.[Action], s.[Comments], s.[InternalInvoiceReference], s.[FirstClaimDate], s.[LastClaimDate],
                                                s.[ToRemind], s.[ToRemindDate], s.[ACK], s.[SwiftCode], s.[PaymentReference], s.[KPI],
                                                s.[IncidentType], s.[RiskyItem], s.[ReasonNonRisky], ?, s.[ModifiedBy], s.[LastModified]
                                            FROM [{StagingTable}] AS s
                                            LEFT JOIN [{TargetTable}] AS t ON t.[ID] = s.[ID]
                                            WHERE t.[ID] IS NULL";
                        int inserted;
                        using (var cmd = new OleDbCommand(insertSql, conn, tx))
                        {
                            cmd.Parameters.AddWithValue("@CreationDate", nowOa);
                            inserted = await cmd.ExecuteNonQueryAsync();
                        }

                        tx.Commit();
                        return inserted;
                    }
                    catch
                    {
                        tx.Rollback();
                        throw;
                    }
                }
            }
        }

        /// <summary>
        /// Returns the subset of provided IDs that already exist in T_Reconciliation.
        /// </summary>
        public async Task<HashSet<string>> GetExistingIdsAsync(IEnumerable<string> ids)
        {
            var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var list = (ids ?? Enumerable.Empty<string>()).Where(s => !string.IsNullOrWhiteSpace(s)).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
            if (list.Count == 0) return result;

            using (var conn = new OleDbConnection(_connectionString))
            {
                await conn.OpenAsync();
                // Chunk to avoid too many parameters
                const int chunkSize = 500;
                for (int i = 0; i < list.Count; i += chunkSize)
                {
                    var chunk = list.Skip(i).Take(chunkSize).ToList();
                    var placeholders = string.Join(",", chunk.Select(_ => "?"));
                    var sql = $"SELECT [ID] FROM [{TargetTable}] WHERE [ID] IN ({placeholders})";
                    using (var cmd = new OleDbCommand(sql, conn))
                    {
                        foreach (var id in chunk)
                            cmd.Parameters.AddWithValue("@ID", id);

                        using (var reader = await cmd.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                var id = reader[0]?.ToString();
                                if (!string.IsNullOrWhiteSpace(id)) result.Add(id);
                            }
                        }
                    }
                }
            }
            return result;
        }

        /// <summary>
        /// Archives target rows by ID using a temporary archive staging table and a single set-based UPDATE.
        /// </summary>
        public async Task<int> ArchiveByIdsAsync(IEnumerable<string> ids)
        {
            var idList = (ids ?? Enumerable.Empty<string>()).Where(s => !string.IsNullOrWhiteSpace(s)).Distinct().ToList();
            if (idList.Count == 0) return 0;

            const string ArchiveStaging = "T_Staging_Reconciliation_Archive";
            using (var conn = new OleDbConnection(_connectionString))
            {
                await conn.OpenAsync();
                using (var tx = conn.BeginTransaction())
                {
                    try
                    {
                        // Ensure archive staging table
                        if (!await TableExistsAsync(conn, ArchiveStaging, tx))
                        {
                            await ExecuteNonQueryAsync(conn, $"CREATE TABLE [{ArchiveStaging}] ([ID] TEXT(64) NOT NULL)", tx);
                            try { await ExecuteNonQueryAsync(conn, $"CREATE INDEX IX_{ArchiveStaging}_ID ON [{ArchiveStaging}] ([ID])", tx); } catch { }
                        }
                        else
                        {
                            await ExecuteNonQueryAsync(conn, $"DELETE FROM [{ArchiveStaging}]", tx);
                        }

                        // Batch insert IDs
                        foreach (var chunk in Chunk(idList, 1000))
                        {
                            foreach (var id in chunk)
                            {
                                using (var cmd = new OleDbCommand($"INSERT INTO [{ArchiveStaging}] ([ID]) VALUES (?)", conn, tx))
                                {
                                    cmd.Parameters.AddWithValue("@ID", id);
                                    await cmd.ExecuteNonQueryAsync();
                                }
                            }
                        }

                        // Set-based archive: set DeleteDate to now for matching IDs
                        double nowOa = DateTime.Now.ToOADate();
                        using (var cmd = new OleDbCommand($@"UPDATE [{TargetTable}] AS t 
                                                             INNER JOIN [{ArchiveStaging}] AS a ON t.[ID] = a.[ID]
                                                             SET t.[DeleteDate] = ?", conn, tx))
                        {
                            cmd.Parameters.AddWithValue("@DeleteDate", nowOa);
                            var affected = await cmd.ExecuteNonQueryAsync();
                            tx.Commit();
                            return affected;
                        }
                    }
                    catch
                    {
                        tx.Rollback();
                        throw;
                    }
                }
            }
        }

        #endregion

        #region Helpers

        private async Task<bool> TableExistsAsync(OleDbConnection conn, string tableName, OleDbTransaction tx = null)
        {
            var schema = conn.GetSchema("Tables");
            var rows = schema?.Rows?.Cast<DataRow>() ?? Enumerable.Empty<DataRow>();
            return rows.Any(r => string.Equals(r[2]?.ToString(), tableName, StringComparison.OrdinalIgnoreCase));
        }

        private async Task ExecuteNonQueryAsync(OleDbConnection conn, string sql, OleDbTransaction tx = null)
        {
            using (var cmd = tx == null ? new OleDbCommand(sql, conn) : new OleDbCommand(sql, conn, tx))
            {
                await cmd.ExecuteNonQueryAsync();
            }
        }

        private void AddStagingParameters(OleDbCommand cmd, Reconciliation r, System.Collections.Generic.Dictionary<string, int?> textMax)
        {
            object Db(object v) => v ?? (object)DBNull.Value;
            double? Oa(DateTime? dt) => dt.HasValue ? (double?)dt.Value.ToOADate() : null;
            string Trunc(string value, string col, bool isKey = false)
            {
                if (value == null) return null;
                if (textMax != null && textMax.TryGetValue(col, out var max) && max.HasValue && max.Value > 0 && value.Length > max.Value)
                {
                    if (isKey) throw new InvalidOperationException($"Valeur trop longue pour la clé {col} (max {max.Value})");
                    return value.Substring(0, max.Value);
                }
                return value;
            }

            cmd.Parameters.AddWithValue("@ID", Db(Trunc(r.ID, "ID", isKey: true)));
            cmd.Parameters.AddWithValue("@DWINGS_GuaranteeID", Db(Trunc(r.DWINGS_GuaranteeID, "DWINGS_GuaranteeID")));
            cmd.Parameters.AddWithValue("@DWINGS_InvoiceID", Db(Trunc(r.DWINGS_InvoiceID, "DWINGS_InvoiceID")));
            cmd.Parameters.AddWithValue("@DWINGS_CommissionID", Db(Trunc(r.DWINGS_CommissionID, "DWINGS_CommissionID")));
            cmd.Parameters.AddWithValue("@Action", Db(r.Action));
            cmd.Parameters.AddWithValue("@Comments", Db(Trunc(r.Comments, "Comments")));
            cmd.Parameters.AddWithValue("@InternalInvoiceReference", Db(Trunc(r.InternalInvoiceReference, "InternalInvoiceReference")));
            cmd.Parameters.AddWithValue("@FirstClaimDate", (object)Oa(r.FirstClaimDate) ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@LastClaimDate", (object)Oa(r.LastClaimDate) ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@ToRemind", r.ToRemind);
            cmd.Parameters.AddWithValue("@ToRemindDate", (object)Oa(r.ToRemindDate) ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@ACK", r.ACK);
            cmd.Parameters.AddWithValue("@SwiftCode", Db(Trunc(r.SwiftCode, "SwiftCode")));
            cmd.Parameters.AddWithValue("@PaymentReference", Db(Trunc(r.PaymentReference, "PaymentReference")));
            cmd.Parameters.AddWithValue("@KPI", Db(r.KPI));
            cmd.Parameters.AddWithValue("@IncidentType", Db(r.IncidentType));
            cmd.Parameters.AddWithValue("@RiskyItem", Db(r.RiskyItem));
            cmd.Parameters.AddWithValue("@ReasonNonRisky", Db(Trunc(r.ReasonNonRisky, "ReasonNonRisky")));
            cmd.Parameters.AddWithValue("@ModifiedBy", Db(Trunc(r.ModifiedBy ?? _currentUser, "ModifiedBy")));
            cmd.Parameters.AddWithValue("@LastModified", (object)Oa(r.LastModified ?? DateTime.Now) ?? DBNull.Value);
        }

        private async Task<System.Collections.Generic.Dictionary<string, int?>> GetTextColumnMaxLengthsAsync(OleDbConnection conn, string tableName)
        {
            var dict = new System.Collections.Generic.Dictionary<string, int?>(StringComparer.OrdinalIgnoreCase);
            var schema = conn.GetSchema("Columns");
            foreach (System.Data.DataRow row in schema.Rows)
            {
                var tbl = row["TABLE_NAME"]?.ToString();
                if (!string.Equals(tbl, tableName, StringComparison.OrdinalIgnoreCase)) continue;
                var col = row["COLUMN_NAME"]?.ToString();
                if (string.IsNullOrEmpty(col)) continue;
                int? max = null;
                if (schema.Columns.Contains("CHARACTER_MAXIMUM_LENGTH") && int.TryParse(row["CHARACTER_MAXIMUM_LENGTH"]?.ToString(), out var cm)) max = cm;
                else if (schema.Columns.Contains("COLUMN_SIZE") && int.TryParse(row["COLUMN_SIZE"]?.ToString(), out var cs)) max = cs;
                dict[col] = max;
            }
            return await Task.FromResult(dict);
        }

        private static IEnumerable<List<T>> Chunk<T>(IEnumerable<T> source, int size)
        {
            var bucket = new List<T>(size);
            foreach (var item in source)
            {
                bucket.Add(item);
                if (bucket.Count == size)
                {
                    yield return bucket;
                    bucket = new List<T>(size);
                }
            }
            if (bucket.Count > 0)
                yield return bucket;
        }

        #endregion
    }
}
