using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using RecoTool.Services.DTOs;

namespace RecoTool.Services
{
    /// <summary>
    /// Encapsulates KPI daily snapshot persistence and computation against the Control DB.
    /// Delegates data retrieval to ReconciliationService (dashboard view) and connection data to OfflineFirstService.
    /// </summary>
    public class KpiSnapshotService
    {
        private readonly OfflineFirstService _offlineFirstService;
        private readonly ReconciliationService _reconciliationService;

        public KpiSnapshotService(OfflineFirstService offlineFirstService, ReconciliationService reconciliationService)
        {
            _offlineFirstService = offlineFirstService ?? throw new ArgumentNullException(nameof(offlineFirstService));
            _reconciliationService = reconciliationService ?? throw new ArgumentNullException(nameof(reconciliationService));
        }

        public async Task<List<DateTime>> GetKpiSnapshotDatesAsync(string countryId, CancellationToken cancellationToken = default)
        {
            var dates = new List<DateTime>();
            if (string.IsNullOrWhiteSpace(countryId)) return dates;

            using (var connection = new OleDbConnection(GetControlConnectionString()))
            {
                await connection.OpenAsync(cancellationToken);
                await EnsureKpiDailySnapshotTableAsync(connection, cancellationToken);
                var cmd = new OleDbCommand("SELECT DISTINCT SnapshotDate FROM KpiDailySnapshot WHERE CountryId = ? ORDER BY SnapshotDate", connection);
                cmd.Parameters.Add(new OleDbParameter("@p1", OleDbType.VarWChar) { Value = countryId ?? string.Empty });
                using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
                {
                    while (await reader.ReadAsync(cancellationToken))
                    {
                        if (!reader.IsDBNull(0))
                        {
                            var dt = Convert.ToDateTime(reader.GetValue(0));
                            dates.Add(dt.Date);
                        }
                    }
                }
            }

            return dates.Distinct().OrderBy(d => d).ToList();
        }

        public async Task<bool> SaveDailyKpiSnapshotAsync(DateTime date, string countryId, string sourceVersion = null, CancellationToken cancellationToken = default)
        {
            var dto = await BuildKpiDailySnapshotAsync(date.Date, countryId, sourceVersion, cancellationToken);
            return await InsertDailyKpiSnapshotAsync(dto, cancellationToken);
        }

        public async Task<DataTable> GetKpiSnapshotsAsync(DateTime from, DateTime to, string countryId, CancellationToken cancellationToken = default)
        {
            using (var connection = new OleDbConnection(GetControlConnectionString()))
            {
                await connection.OpenAsync(cancellationToken);
                await EnsureKpiDailySnapshotTableAsync(connection, cancellationToken);
                var cmd = new OleDbCommand("SELECT * FROM KpiDailySnapshot WHERE CountryId = ? AND SnapshotDate BETWEEN ? AND ? ORDER BY SnapshotDate", connection);
                cmd.Parameters.Add(new OleDbParameter("@p1", OleDbType.VarWChar) { Value = countryId ?? string.Empty });
                cmd.Parameters.Add(new OleDbParameter("@p2", OleDbType.Date) { Value = from.Date });
                cmd.Parameters.Add(new OleDbParameter("@p3", OleDbType.Date) { Value = to.Date });
                using (var adapter = new OleDbDataAdapter(cmd))
                {
                    var table = new DataTable();
                    adapter.Fill(table);
                    return table;
                }
            }
        }

        public async Task<DataTable> GetKpiSnapshotAsync(DateTime date, string countryId, CancellationToken cancellationToken = default)
        {
            using (var connection = new OleDbConnection(GetControlConnectionString()))
            {
                await connection.OpenAsync(cancellationToken);
                await EnsureKpiDailySnapshotTableAsync(connection, cancellationToken);
                var cmd = new OleDbCommand("SELECT TOP 1 * FROM KpiDailySnapshot WHERE SnapshotDate = ? AND CountryId = ? ORDER BY CreatedAtUtc DESC", connection);
                cmd.Parameters.Add(new OleDbParameter("@p1", OleDbType.Date) { Value = date.Date });
                cmd.Parameters.Add(new OleDbParameter("@p2", OleDbType.VarWChar) { Value = countryId ?? string.Empty });
                using (var adapter = new OleDbDataAdapter(cmd))
                {
                    var table = new DataTable();
                    adapter.Fill(table);
                    return table;
                }
            }
        }

        public async Task<bool> FreezeLatestSnapshotAsync(string countryId, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(countryId)) return false;
            using (var connection = new OleDbConnection(GetControlConnectionString()))
            {
                await connection.OpenAsync(cancellationToken);
                await EnsureKpiDailySnapshotTableAsync(connection, cancellationToken);
                // Find latest non-frozen snapshot (CreatedAtUtc DESC)
                var findCmd = new OleDbCommand(
                    "SELECT TOP 1 Id FROM KpiDailySnapshot WHERE CountryId = ? AND FrozenAt IS NULL ORDER BY CreatedAtUtc DESC", connection);
                findCmd.Parameters.AddWithValue("@p1", countryId);
                var obj = await findCmd.ExecuteScalarAsync(cancellationToken);
                if (obj == null || obj == DBNull.Value) return false;
                if (!int.TryParse(Convert.ToString(obj), out var id)) return false;

                var upd = new OleDbCommand("UPDATE KpiDailySnapshot SET FrozenAt = ? WHERE Id = ?", connection);
                var pFrozen = new OleDbParameter("@p1", OleDbType.Date) { Value = DateTime.UtcNow };
                upd.Parameters.Add(pFrozen);
                upd.Parameters.AddWithValue("@p2", id);
                var n = await upd.ExecuteNonQueryAsync(cancellationToken);
                return n > 0;
            }
        }

        private async Task<KpiDailySnapshotDto> BuildKpiDailySnapshotAsync(DateTime date, string countryId, string sourceVersion, CancellationToken ct)
        {
            var list = await _reconciliationService.GetReconciliationViewAsync(countryId, null, true);

            var currentCountry = _offlineFirstService?.CurrentCountry;
            string receivableId = currentCountry?.CNT_AmbreReceivable;
            string pivotId = currentCountry?.CNT_AmbrePivot;

            var dto = new KpiDailySnapshotDto
            {
                SnapshotDate = date.Date,
                CountryId = countryId,
                CreatedAtUtc = DateTime.UtcNow,
                SourceVersion = sourceVersion
            };

            // KPI counts
            dto.PaidNotReconciled = list.LongCount(r => r.KPI == (int)KPIType.PaidButNotReconciled);
            dto.UnderInvestigation = list.LongCount(r => r.KPI == (int)KPIType.UnderInvestigation);
            dto.MissingInvoices = list.LongCount(r => r.KPI == (int)KPIType.NotClaimed);

            // Receivable vs Pivot totals
            var receivableData = string.IsNullOrEmpty(receivableId) ? new List<ReconciliationViewData>() : list.Where(r => r.Account_ID == receivableId).ToList();
            var pivotData = string.IsNullOrEmpty(pivotId) ? new List<ReconciliationViewData>() : list.Where(r => r.Account_ID == pivotId).ToList();
            dto.ReceivableAmount = receivableData.Sum(r => r.SignedAmount);
            dto.ReceivableCount = receivableData.LongCount();
            dto.PivotAmount = pivotData.Sum(r => r.SignedAmount);
            dto.PivotCount = pivotData.LongCount();

            // New vs Deleted for that day
            dto.NewCount = list.LongCount(r => r.CreationDate.HasValue && r.CreationDate.Value.Date == date.Date);
            dto.DeletedCount = list.LongCount(r => r.DeleteDate.HasValue && r.DeleteDate.Value.Date == date.Date);

            // Deletion delay buckets (average days)
            var durations = list
                .Where(r => r.CreationDate.HasValue && r.DeleteDate.HasValue)
                .Select(r => (int)(r.DeleteDate.Value.Date - r.CreationDate.Value.Date).TotalDays)
                .Where(d => d >= 0)
                .ToList();

            var buckets = new[]
            {
                new { Key = "0-14d", Min = 0,  Max = 14 },
                new { Key = "15-30d", Min = 15, Max = 30 },
                new { Key = "1-3m",  Min = 31, Max = 92 },
                new { Key = "3-6m",  Min = 93, Max = 185 },
                new { Key = ">6m",   Min = 186, Max = int.MaxValue }
            };

            var bucketAvgs = buckets.ToDictionary(b => b.Key, b =>
            {
                var xs = durations.Where(d => d >= b.Min && d <= b.Max).ToList();
                return xs.Count == 0 ? 0 : (int)Math.Round(xs.Average());
            });
            dto.DeletionDelayBucketsJson = JsonSerializer.Serialize(bucketAvgs);

            // KPI distribution
            var kpiDist = list
                .GroupBy(r => r.KPI ?? -1)
                .ToDictionary(g => g.Key, g => g.LongCount());
            dto.KpiDistributionJson = JsonSerializer.Serialize(kpiDist);

            // Simple risk matrix (KPI x Action)
            var riskMatrix = list
                .GroupBy(r => new { r.KPI, r.Action })
                .ToDictionary(g => $"{g.Key.KPI}|{g.Key.Action}", g => g.LongCount());
            dto.KpiRiskMatrixJson = JsonSerializer.Serialize(riskMatrix);

            // Currency distribution
            var currencyDist = list
                .GroupBy(r => string.IsNullOrWhiteSpace(r.CCY) ? "?" : r.CCY)
                .ToDictionary(g => g.Key, g => g.LongCount());
            dto.CurrencyDistributionJson = JsonSerializer.Serialize(currencyDist);

            // Action distribution (Pivot-only)
            var pivotActions = pivotData
                .GroupBy(r => r.Action ?? -1)
                .ToDictionary(g => g.Key, g => g.LongCount());
            dto.ActionDistributionJson = JsonSerializer.Serialize(pivotActions);

            // Receivable/Pivot by Action breakdown
            var byAction = list
                .Where(r => r.Action.HasValue)
                .GroupBy(r => r.Action.Value)
                .ToDictionary(g => g.Key,
                    g => new
                    {
                        Receivable = g.Where(r => r.Account_ID == receivableId).LongCount(),
                        Pivot = g.Where(r => r.Account_ID == pivotId).LongCount()
                    });
            dto.ReceivablePivotByActionJson = JsonSerializer.Serialize(byAction);

            return dto;
        }

        private async Task<bool> InsertDailyKpiSnapshotAsync(KpiDailySnapshotDto dto, CancellationToken cancellationToken)
        {
            using (var connection = new OleDbConnection(GetControlConnectionString()))
            {
                await connection.OpenAsync(cancellationToken);

                await EnsureKpiDailySnapshotTableAsync(connection, cancellationToken);

                // Discover existing columns and their data types in KpiDailySnapshot to avoid mismatches and type errors
                var schema = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Columns, new object[] { null, null, "KpiDailySnapshot", null });
                var existing = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                foreach (DataRow row in schema.Rows)
                {
                    var name = Convert.ToString(row["COLUMN_NAME"], CultureInfo.InvariantCulture);
                    var dataType = row["DATA_TYPE"];
                    if (!string.IsNullOrWhiteSpace(name) && dataType != null && int.TryParse(dataType.ToString(), out var oleType))
                        existing[name] = oleType;
                }

                (OleDbType Type, object Value) Map(string col, object val)
                {
                    if (existing.TryGetValue(col, out var oleType))
                    {
                        var t = (OleDbType)oleType;
                        return (t, val ?? DBNull.Value);
                    }

                    // Fallback guesses
                    switch (col)
                    {
                        case "SnapshotDate":
                            return (OleDbType.Date, (object)val ?? DBNull.Value);
                        case "CreatedAtUtc":
                            return (OleDbType.Date, (object)val ?? DBNull.Value);
                        case "FrozenAt":
                            return (OleDbType.Date, (object)val ?? DBNull.Value);
                        case "CountryId":
                            return (OleDbType.VarWChar, (object)(val ?? string.Empty));
                        case "MissingInvoices":
                        case "PaidNotReconciled":
                        case "UnderInvestigation":
                        case "ReceivableCount":
                        case "PivotCount":
                        case "NewCount":
                        case "DeletedCount":
                            return (OleDbType.Integer, Convert.ToInt64(val ?? 0));
                        case "ReceivableAmount":
                        case "PivotAmount":
                            return (OleDbType.Currency, Convert.ToDecimal(val ?? 0));
                        default:
                            return (OleDbType.LongVarWChar, (object)(val?.ToString() ?? string.Empty));
                    }
                }

                // Prepare the set of potential values
                var raw = new List<(string Name, object Val)>
                {
                    ("SnapshotDate", dto.SnapshotDate),
                    ("CountryId", dto.CountryId ?? string.Empty),
                    ("MissingInvoices", dto.MissingInvoices),
                    ("PaidNotReconciled", dto.PaidNotReconciled),
                    ("UnderInvestigation", dto.UnderInvestigation),
                    ("ReceivableAmount", dto.ReceivableAmount),
                    ("ReceivableCount", dto.ReceivableCount),
                    ("PivotAmount", dto.PivotAmount),
                    ("PivotCount", dto.PivotCount),
                    ("NewCount", dto.NewCount),
                    ("DeletedCount", dto.DeletedCount),
                    ("DeletionDelayBucketsJson", dto.DeletionDelayBucketsJson),
                    ("ReceivablePivotByActionJson", dto.ReceivablePivotByActionJson),
                    ("KpiDistributionJson", dto.KpiDistributionJson),
                    ("KpiRiskMatrixJson", dto.KpiRiskMatrixJson),
                    ("CurrencyDistributionJson", dto.CurrencyDistributionJson),
                    ("ActionDistributionJson", dto.ActionDistributionJson),
                    ("CreatedAtUtc", dto.CreatedAtUtc),
                    ("FrozenAt", DBNull.Value),
                    ("SourceVersion", dto.SourceVersion ?? string.Empty)
                };

                // Map to existing columns
                var used = new List<(string Name, OleDbType Type, object Value)>();
                foreach (var r in raw)
                {
                    var mapped = Map(r.Name, r.Val);
                    used.Add((r.Name, mapped.Type, mapped.Value));
                }
                if (used.Count == 0)
                    throw new InvalidOperationException("La table KpiDailySnapshot ne contient aucune des colonnes attendues.");

                var colList = string.Join(", ", used.Select(u => $"[{u.Name}]"));
                var placeholders = string.Join(", ", used.Select(_ => "?"));
                var sql = $"INSERT INTO KpiDailySnapshot ({colList}) VALUES ({placeholders})";
                using (var insert = new OleDbCommand(sql, connection))
                {
                    foreach (var u in used)
                    {
                        var p = insert.Parameters.Add($"@{u.Name}", u.Type);
                        p.Value = u.Value ?? DBNull.Value;
                    }
                    var n = await insert.ExecuteNonQueryAsync(cancellationToken);
                    return n > 0;
                }
            }
        }

        private async Task EnsureKpiDailySnapshotTableAsync(OleDbConnection connection, CancellationToken ct)
        {
            var tables = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, new object[] { null, null, "KpiDailySnapshot", "TABLE" });
            if (tables != null && tables.Rows.Count > 0) return;

            var createSql = @"CREATE TABLE KpiDailySnapshot (
                Id COUNTER PRIMARY KEY,
                SnapshotDate DATETIME,
                CountryId TEXT(50),
                MissingInvoices LONG,
                PaidNotReconciled LONG,
                UnderInvestigation LONG,
                ReceivableAmount CURRENCY,
                ReceivableCount LONG,
                PivotAmount CURRENCY,
                PivotCount LONG,
                NewCount LONG,
                DeletedCount LONG,
                DeletionDelayBucketsJson LONGTEXT,
                ReceivablePivotByActionJson LONGTEXT,
                KpiDistributionJson LONGTEXT,
                KpiRiskMatrixJson LONGTEXT,
                CurrencyDistributionJson LONGTEXT,
                ActionDistributionJson LONGTEXT,
                CreatedAtUtc DATETIME,
                FrozenAt DATETIME,
                SourceVersion TEXT(50)
            )";
            using (var cmd = new OleDbCommand(createSql, connection))
            {
                await cmd.ExecuteNonQueryAsync();
            }
        }

        private string GetControlConnectionString()
        {
            if (_offlineFirstService != null)
                return _offlineFirstService.GetControlConnectionString(_offlineFirstService.CurrentCountryId);
            throw new InvalidOperationException("OfflineFirstService is required for Control DB connection.");
        }
    }
}
