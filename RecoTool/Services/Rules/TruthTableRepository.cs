using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using RecoTool.Services;

namespace RecoTool.Services.Rules
{
    /// <summary>
    /// Administrative interface to manage rules storage (create table, upsert and delete rules).
    /// Placed here to avoid new files while project includes are static.
    /// </summary>
    public interface IRulesAdmin
    {
        Task<bool> EnsureRulesTableAsync(CancellationToken token = default);
        Task<bool> UpsertRuleAsync(TruthRule rule, CancellationToken token = default);
        Task<int> DeleteRuleAsync(string ruleId, CancellationToken token = default);
    }


    public class TruthTableRepository : IRulesAdmin
    {
        private readonly OfflineFirstService _offlineFirstService;
        private readonly ReferentialService _referentialService;

        public TruthTableRepository(OfflineFirstService offlineFirstService)
        {
            _offlineFirstService = offlineFirstService ?? throw new ArgumentNullException(nameof(offlineFirstService));
            _referentialService = new ReferentialService(_offlineFirstService);
        }

        private async Task<string> GetRulesTableNameAsync(CancellationToken token)
        {
            return "T_Reco_Rules";
        }

        private static bool HasColumn(DataTable schema, string columnName)
            => schema != null && schema.Rows.Cast<DataRow>().Any(r => string.Equals(Convert.ToString(r["COLUMN_NAME"]), columnName, StringComparison.OrdinalIgnoreCase));

        /// <summary>
        /// Loads enabled rules ordered by Priority ASC, RuleId ASC.
        /// Returns empty list when table is missing or unreadable.
        /// </summary>
        public async Task<List<TruthRule>> LoadRulesAsync(CancellationToken token = default)
        {
            var list = new List<TruthRule>();
            try
            {
                var tableName = await GetRulesTableNameAsync(token).ConfigureAwait(false);
                var cs = _offlineFirstService.ReferentialConnectionString;
                using (var conn = new OleDbConnection(cs))
                {
                    await conn.OpenAsync(token).ConfigureAwait(false);
                    // Introspect columns to be robust to schema variants
                    var schema = conn.GetOleDbSchemaTable(OleDbSchemaGuid.Columns, new object[] { null, null, tableName, null });
                    if (schema == null || schema.Rows.Count == 0)
                    {
                        // Table missing or not accessible
                        return list;
                    }

                    var sql = $"SELECT * FROM [{tableName}]";
                    using (var cmd = new OleDbCommand(sql, conn))
                    using (var rdr = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false))
                    {
                        var table = new DataTable();
                        table.Load(rdr);
                        foreach (DataRow row in table.Rows)
                        {
                            try
                            {
                                // Read Enabled (default true if column missing or null)
                                bool enabled = true;
                                if (table.Columns.Contains("Enabled") && row["Enabled"] != DBNull.Value) enabled = Convert.ToBoolean(row["Enabled"]);
                                if (!enabled) continue;

                                var rule = new TruthRule
                                {
                                    RuleId = table.Columns.Contains("RuleId") ? Convert.ToString(row["RuleId"]) : null,
                                    Enabled = enabled,
                                    Priority = table.Columns.Contains("Priority") && row["Priority"] != DBNull.Value ? Convert.ToInt32(row["Priority"]) : 100,
                                    Scope = ParseScope(GetString(row, table, "Scope") ?? "Both"),
                                    AccountSide = NormalizeWildcard(GetString(row, table, "AccountSide") ?? "*"),
                                    GuaranteeType = NormalizeWildcard(GetString(row, table, "GuaranteeType")),
                                    TransactionType = NormalizeWildcard(GetString(row, table, "TransactionType")),
                                    Booking = NormalizeWildcard(GetString(row, table, "Booking")),
                                    HasDwingsLink = GetNullableBool(row, table, "HasDwingsLink"),
                                    IsGrouped = GetNullableBool(row, table, "IsGrouped"),
                                    IsAmountMatch = GetNullableBool(row, table, "IsAmountMatch"),
                                    Sign = NormalizeWildcard(GetString(row, table, "Sign") ?? "*"),
                                    MTStatus = GetMtStatusCondition(row, table, "MTStatus"),
                                    CommIdEmail = GetNullableBool(row, table, "CommIdEmail"),
                                    BgiStatusInitiated = GetNullableBool(row, table, "BgiStatusInitiated"),
                                    TriggerDateIsNull = GetNullableBool(row, table, "TriggerDateIsNull"),
                                    DaysSinceTriggerMin = GetNullableInt(row, table, "DaysSinceTriggerMin"),
                                    DaysSinceTriggerMax = GetNullableInt(row, table, "DaysSinceTriggerMax"),
                                    OperationDaysAgoMin = GetNullableInt(row, table, "OperationDaysAgoMin"),
                                    OperationDaysAgoMax = GetNullableInt(row, table, "OperationDaysAgoMax"),
                                    IsMatched = GetNullableBool(row, table, "IsMatched"),
                                    HasManualMatch = GetNullableBool(row, table, "HasManualMatch"),
                                    IsFirstRequest = GetNullableBool(row, table, "IsFirstRequest"),
                                    DaysSinceReminderMin = GetNullableInt(row, table, "DaysSinceReminderMin"),
                                    DaysSinceReminderMax = GetNullableInt(row, table, "DaysSinceReminderMax"),
                                    CurrentActionId = NormalizeWildcard(GetString(row, table, "CurrentActionId")),
                                    PaymentRequestStatus = NormalizeWildcard(GetString(row, table, "PaymentRequestStatus")),
                                    OutputActionId = GetNullableInt(row, table, "OutputActionId"),
                                    OutputKpiId = GetNullableInt(row, table, "OutputKpiId"),
                                    OutputIncidentTypeId = GetNullableInt(row, table, "OutputIncidentTypeId"),
                                    OutputRiskyItem = GetNullableBool(row, table, "OutputRiskyItem"),
                                    OutputReasonNonRiskyId = GetNullableInt(row, table, "OutputReasonNonRiskyId"),
                                    OutputToRemind = GetNullableBool(row, table, "OutputToRemind"),
                                    OutputToRemindDays = GetNullableInt(row, table, "OutputToRemindDays"),
                                    OutputFirstClaimToday = GetNullableBool(row, table, "OutputFirstClaimToday"),
                                    ApplyTo = ParseApplyTo(GetString(row, table, "ApplyTo") ?? "Self"),
                                    AutoApply = table.Columns.Contains("AutoApply") && row["AutoApply"] != DBNull.Value ? Convert.ToBoolean(row["AutoApply"]) : true,
                                    Message = GetString(row, table, "Message")
                                };
                                list.Add(rule);
                            }
                            catch { }
                        }
                    }
                }

                // Order by priority, then RuleId stable
                list = list.OrderBy(r => r.Priority).ThenBy(r => r.RuleId, StringComparer.OrdinalIgnoreCase).ToList();
            }
            catch
            {
                // Return empty list on any error to avoid blocking
                return new List<TruthRule>();
            }
            return list;
        }

        /// <summary>
        /// Creates the rules table in the referential DB if it does not already exist.
        /// Returns true if the table exists or was created successfully.
        /// </summary>
        public async Task<bool> EnsureRulesTableAsync(CancellationToken token = default)
        {
            var tableName = await GetRulesTableNameAsync(token).ConfigureAwait(false);
            var cs = _offlineFirstService.ReferentialConnectionString;
            using (var conn = new OleDbConnection(cs))
            {
                await conn.OpenAsync(token).ConfigureAwait(false);
                // Check existence
                try
                {
                    var schema = conn.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, new object[] { null, null, tableName, "TABLE" });
                    if (schema != null && schema.Rows.Count > 0)
                    {
                        // Table exists; ensure new columns are present
                        await EnsureMissingColumnsAsync(conn, tableName).ConfigureAwait(false);
                        return true; // already exists
                    }
                }
                catch { /* fallback to create */ }

                try
                {
                    // Create table with full schema (columns are optional when reading; we create full set)
                    var sql = $@"CREATE TABLE [{tableName}] (
                        RuleId TEXT(50),
                        Enabled YESNO,
                        Priority INTEGER,
                        Scope TEXT(10),
                        AccountSide TEXT(1),
                        GuaranteeType TEXT(255),
                        TransactionType TEXT(255),
                        Booking TEXT(50),
                        HasDwingsLink INTEGER,
                        IsGrouped INTEGER,
                        IsAmountMatch INTEGER,
                        Sign TEXT(1),
                        MTStatus TEXT(20),
                        CommIdEmail INTEGER,
                        BgiStatusInitiated INTEGER,
                        TriggerDateIsNull INTEGER,
                        DaysSinceTriggerMin INTEGER,
                        DaysSinceTriggerMax INTEGER,
                        OperationDaysAgoMin INTEGER,
                        OperationDaysAgoMax INTEGER,
                        IsMatched INTEGER,
                        HasManualMatch INTEGER,
                        IsFirstRequest INTEGER,
                        DaysSinceReminderMin INTEGER,
                        DaysSinceReminderMax INTEGER,
                        CurrentActionId TEXT(255),
                        PaymentRequestStatus TEXT(255),
                        OutputActionId INTEGER,
                        OutputKpiId INTEGER,
                        OutputIncidentTypeId INTEGER,
                        OutputRiskyItem INTEGER,
                        OutputReasonNonRiskyId INTEGER,
                        OutputToRemind INTEGER,
                        OutputToRemindDays INTEGER,
                        OutputFirstClaimToday INTEGER,
                        ApplyTo TEXT(12),
                        AutoApply YESNO,
                        Message LONGTEXT
                    )";
                    using (var cmd = new OleDbCommand(sql, conn))
                    {
                        await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                    }

                    // Unique index on RuleId to avoid duplicates
                    try
                    {
                        var idx = $"CREATE UNIQUE INDEX UX_{tableName}_RuleId ON [{tableName}] (RuleId)";
                        using (var cmd = new OleDbCommand(idx, conn))
                        {
                            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                        }
                    }
                    catch { /* index best-effort */ }

                    return true;
                }
                catch
                {
                    return false;
                }
            }
        }

        private static async Task EnsureMissingColumnsAsync(OleDbConnection conn, string tableName)
        {
            // Introspect existing columns
            var cols = conn.GetOleDbSchemaTable(OleDbSchemaGuid.Columns, new object[] { null, null, tableName, null });
            var need = new List<(string Name, string Ddl)> {
                ("Booking", "TEXT(50)"),
                ("TriggerDateIsNull", "INTEGER"),
                ("DaysSinceTriggerMin", "INTEGER"),
                ("DaysSinceTriggerMax", "INTEGER"),
                ("OperationDaysAgoMin", "INTEGER"),
                ("OperationDaysAgoMax", "INTEGER"),
                ("IsMatched", "INTEGER"),
                ("HasManualMatch", "INTEGER"),
                ("IsFirstRequest", "INTEGER"),
                ("DaysSinceReminderMin", "INTEGER"),
                ("DaysSinceReminderMax", "INTEGER"),
                ("CurrentActionId", "TEXT(255)"),
                ("PaymentRequestStatus", "TEXT(255)"),
                ("MTStatus", "TEXT(20)"),
                ("CommIdEmail", "INTEGER"),
                ("BgiStatusInitiated", "INTEGER"),
                ("OutputFirstClaimToday", "INTEGER")
            };
            foreach (var (name, ddl) in need)
            {
                bool exists = cols != null && cols.Rows.Cast<System.Data.DataRow>().Any(r => string.Equals(Convert.ToString(r["COLUMN_NAME"]), name, StringComparison.OrdinalIgnoreCase));
                if (!exists)
                {
                    try
                    {
                        using (var cmd = new OleDbCommand($"ALTER TABLE [{tableName}] ADD COLUMN [{name}] {ddl}", conn))
                        {
                            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                        }
                    }
                    catch { /* best-effort */ }
                }
            }

            // Ensure tri-state input boolean columns are INTEGER to allow NULL (-1/0/NULL)
            try
            {
                var triStateBoolCols = new[]
                {
                    "HasDwingsLink", "IsGrouped", "IsAmountMatch",
                    "TriggerDateIsNull", "IsMatched",
                    "HasManualMatch", "IsFirstRequest", "OutputRiskyItem", "OutputToRemind",
                    "CommIdEmail", "BgiStatusInitiated", "OutputFirstClaimToday"
                };

                // Reload schema for nullability info
                cols = conn.GetOleDbSchemaTable(OleDbSchemaGuid.Columns, new object[] { null, null, tableName, null });
                foreach (var colName in triStateBoolCols)
                {
                    var row = cols?.Rows.Cast<System.Data.DataRow>()
                        .FirstOrDefault(r => string.Equals(Convert.ToString(r["COLUMN_NAME"]), colName, StringComparison.OrdinalIgnoreCase));
                    if (row == null) continue; // column may not exist in older schemas; added above if missing
                    try
                    {
                        using (var cmd = new OleDbCommand($"ALTER TABLE [{tableName}] ALTER COLUMN [{colName}] INTEGER", conn))
                        {
                            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                        }
                    }
                    catch { /* best-effort; if provider disallows direct ALTER, manual migration may be required */ }
                }
            }
            catch { }
        }

        /// <summary>
        /// Inserts or updates a rule identified by RuleId. Returns true if one row was inserted/updated.
        /// </summary>
        public async Task<bool> UpsertRuleAsync(TruthRule rule, CancellationToken token = default)
        {
            if (rule == null || string.IsNullOrWhiteSpace(rule.RuleId)) return false;
            var tableName = await GetRulesTableNameAsync(token).ConfigureAwait(false);
            var cs = _offlineFirstService.ReferentialConnectionString;
            using (var conn = new OleDbConnection(cs))
            {
                await conn.OpenAsync(token).ConfigureAwait(false);

                // Exists?
                int count = 0;
                using (var check = new OleDbCommand($"SELECT COUNT(*) FROM [{tableName}] WHERE RuleId = ?", conn))
                {
                    check.Parameters.AddWithValue("@p1", rule.RuleId);
                    var obj = await check.ExecuteScalarAsync(token).ConfigureAwait(false);
                    int.TryParse(Convert.ToString(obj), out count);
                }

                if (count > 0)
                {
                    // UPDATE
                    // Ensure columns exist before update
                    await EnsureMissingColumnsAsync(conn, tableName).ConfigureAwait(false);
                    var sql = $@"UPDATE [{tableName}] SET
                        Enabled=?, Priority=?, Scope=?, AccountSide=?, GuaranteeType=?, TransactionType=?, Booking=?,
                        HasDwingsLink=?, IsGrouped=?, IsAmountMatch=?, Sign=?,
                        MTStatus=?, CommIdEmail=?, BgiStatusInitiated=?,
                        TriggerDateIsNull=?, DaysSinceTriggerMin=?, DaysSinceTriggerMax=?,
                        OperationDaysAgoMin=?, OperationDaysAgoMax=?,
                        IsMatched=?, HasManualMatch=?, IsFirstRequest=?, DaysSinceReminderMin=?, DaysSinceReminderMax=?, CurrentActionId=?,
                        OutputActionId=?, OutputKpiId=?, OutputIncidentTypeId=?, OutputRiskyItem=?, OutputReasonNonRiskyId=?,
                        OutputToRemind=?, OutputToRemindDays=?, OutputFirstClaimToday=?, ApplyTo=?, AutoApply=?, Message=?
                        WHERE RuleId=?";
                    using (var cmd = new OleDbCommand(sql, conn))
                    {
                        AddRuleParams(cmd, rule, includeRuleIdAtEnd: true);
                        var n = await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);
                        return n == 1;
                    }
                }
                else
                {
                    // INSERT
                    await EnsureMissingColumnsAsync(conn, tableName).ConfigureAwait(false);
                    var sql = $@"INSERT INTO [{tableName}] (
                        RuleId, Enabled, Priority, Scope, AccountSide, GuaranteeType, TransactionType, Booking,
                        HasDwingsLink, IsGrouped, IsAmountMatch, Sign,
                        MTStatus, CommIdEmail, BgiStatusInitiated,
                        TriggerDateIsNull, DaysSinceTriggerMin, DaysSinceTriggerMax,
                        OperationDaysAgoMin, OperationDaysAgoMax,
                        IsMatched, HasManualMatch, IsFirstRequest, DaysSinceReminderMin, DaysSinceReminderMax, CurrentActionId, PaymentRequestStatus,
                        OutputActionId, OutputKpiId, OutputIncidentTypeId, OutputRiskyItem, OutputReasonNonRiskyId,
                        OutputToRemind, OutputToRemindDays, OutputFirstClaimToday, ApplyTo, AutoApply, Message)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
                    using (var cmd = new OleDbCommand(sql, conn))
                    {
                        AddRuleParams(cmd, rule, includeRuleIdAtEnd: false);
                        var n = await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);
                        return n == 1;
                    }
                }
            }
        }

        /// <summary>
        /// Deletes a rule by RuleId. Returns number of rows deleted.
        /// </summary>
        public async Task<int> DeleteRuleAsync(string ruleId, CancellationToken token = default)
        {
            if (string.IsNullOrWhiteSpace(ruleId)) return 0;
            var tableName = await GetRulesTableNameAsync(token).ConfigureAwait(false);
            var cs = _offlineFirstService.ReferentialConnectionString;
            using (var conn = new OleDbConnection(cs))
            {
                await conn.OpenAsync(token).ConfigureAwait(false);
                using (var cmd = new OleDbCommand($"DELETE FROM [{tableName}] WHERE RuleId = ?", conn))
                {
                    cmd.Parameters.AddWithValue("@p1", ruleId);
                    var n = await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);
                    return n;
                }
            }
        }

        private static void AddRuleParams(OleDbCommand cmd, TruthRule r, bool includeRuleIdAtEnd)
        {
            // Order must match INSERT/UPDATE placeholders
            if (!includeRuleIdAtEnd) cmd.Parameters.AddWithValue("@RuleId", (object)r.RuleId ?? DBNull.Value);
            cmd.Parameters.Add("@Enabled", OleDbType.Boolean).Value = (object)r.Enabled;
            cmd.Parameters.Add("@Priority", OleDbType.Integer).Value = (object)r.Priority;
            cmd.Parameters.AddWithValue("@Scope", (object)r.Scope.ToString() ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@AccountSide", (object)r.AccountSide ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@GuaranteeType", (object)r.GuaranteeType ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@TransactionType", (object)r.TransactionType ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@Booking", (object)r.Booking ?? DBNull.Value);
            cmd.Parameters.Add("@HasDwingsLink", OleDbType.Integer).Value = (object)(r.HasDwingsLink.HasValue ? (r.HasDwingsLink.Value ? -1 : 0) : (int?)null) ?? DBNull.Value;
            cmd.Parameters.Add("@IsGrouped", OleDbType.Integer).Value = (object)(r.IsGrouped.HasValue ? (r.IsGrouped.Value ? -1 : 0) : (int?)null) ?? DBNull.Value;
            cmd.Parameters.Add("@IsAmountMatch", OleDbType.Integer).Value = (object)(r.IsAmountMatch.HasValue ? (r.IsAmountMatch.Value ? -1 : 0) : (int?)null) ?? DBNull.Value;
            cmd.Parameters.AddWithValue("@Sign", (object)r.Sign ?? DBNull.Value);
            // New DWINGS-related inputs
            cmd.Parameters.AddWithValue("@MTStatus", (object)MtStatusConditionToString(r.MTStatus) ?? DBNull.Value);
            cmd.Parameters.Add("@CommIdEmail", OleDbType.Integer).Value = (object)(r.CommIdEmail.HasValue ? (r.CommIdEmail.Value ? -1 : 0) : (int?)null) ?? DBNull.Value;
            cmd.Parameters.Add("@BgiStatusInitiated", OleDbType.Integer).Value = (object)(r.BgiStatusInitiated.HasValue ? (r.BgiStatusInitiated.Value ? -1 : 0) : (int?)null) ?? DBNull.Value;
            cmd.Parameters.Add("@TriggerDateIsNull", OleDbType.Integer).Value = (object)(r.TriggerDateIsNull.HasValue ? (r.TriggerDateIsNull.Value ? -1 : 0) : (int?)null) ?? DBNull.Value;
            cmd.Parameters.Add("@DaysSinceTriggerMin", OleDbType.Integer).Value = (object)(r.DaysSinceTriggerMin.HasValue ? r.DaysSinceTriggerMin.Value : (int?)null) ?? DBNull.Value;
            cmd.Parameters.Add("@DaysSinceTriggerMax", OleDbType.Integer).Value = (object)(r.DaysSinceTriggerMax.HasValue ? r.DaysSinceTriggerMax.Value : (int?)null) ?? DBNull.Value;
            cmd.Parameters.Add("@OperationDaysAgoMin", OleDbType.Integer).Value = (object)(r.OperationDaysAgoMin.HasValue ? r.OperationDaysAgoMin.Value : (int?)null) ?? DBNull.Value;
            cmd.Parameters.Add("@OperationDaysAgoMax", OleDbType.Integer).Value = (object)(r.OperationDaysAgoMax.HasValue ? r.OperationDaysAgoMax.Value : (int?)null) ?? DBNull.Value;
            cmd.Parameters.Add("@IsMatched", OleDbType.Integer).Value = (object)(r.IsMatched.HasValue ? (r.IsMatched.Value ? -1 : 0) : (int?)null) ?? DBNull.Value;
            cmd.Parameters.Add("@HasManualMatch", OleDbType.Integer).Value = (object)(r.HasManualMatch.HasValue ? (r.HasManualMatch.Value ? -1 : 0) : (int?)null) ?? DBNull.Value;
            cmd.Parameters.Add("@IsFirstRequest", OleDbType.Integer).Value = (object)(r.IsFirstRequest.HasValue ? (r.IsFirstRequest.Value ? -1 : 0) : (int?)null) ?? DBNull.Value;
            cmd.Parameters.Add("@DaysSinceReminderMin", OleDbType.Integer).Value = (object)(r.DaysSinceReminderMin.HasValue ? r.DaysSinceReminderMin.Value : (int?)null) ?? DBNull.Value;
            cmd.Parameters.Add("@DaysSinceReminderMax", OleDbType.Integer).Value = (object)(r.DaysSinceReminderMax.HasValue ? r.DaysSinceReminderMax.Value : (int?)null) ?? DBNull.Value;
            cmd.Parameters.AddWithValue("@CurrentActionId", (object)r.CurrentActionId ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@PaymentRequestStatus", (object)r.PaymentRequestStatus ?? DBNull.Value);
            cmd.Parameters.Add("@OutputActionId", OleDbType.Integer).Value = (object)(r.OutputActionId.HasValue ? r.OutputActionId.Value : (int?)null) ?? DBNull.Value;
            cmd.Parameters.Add("@OutputKpiId", OleDbType.Integer).Value = (object)(r.OutputKpiId.HasValue ? r.OutputKpiId.Value : (int?)null) ?? DBNull.Value;
            cmd.Parameters.Add("@OutputIncidentTypeId", OleDbType.Integer).Value = (object)(r.OutputIncidentTypeId.HasValue ? r.OutputIncidentTypeId.Value : (int?)null) ?? DBNull.Value;
            cmd.Parameters.Add("@OutputRiskyItem", OleDbType.Integer).Value = (object)(r.OutputRiskyItem.HasValue ? (r.OutputRiskyItem.Value ? -1 : 0) : (int?)null) ?? DBNull.Value;
            cmd.Parameters.Add("@OutputReasonNonRiskyId", OleDbType.Integer).Value = (object)(r.OutputReasonNonRiskyId.HasValue ? r.OutputReasonNonRiskyId.Value : (int?)null) ?? DBNull.Value;
            cmd.Parameters.Add("@OutputToRemind", OleDbType.Integer).Value = (object)(r.OutputToRemind.HasValue ? (r.OutputToRemind.Value ? -1 : 0) : (int?)null) ?? DBNull.Value;
            cmd.Parameters.Add("@OutputToRemindDays", OleDbType.Integer).Value = (object)(r.OutputToRemindDays.HasValue ? r.OutputToRemindDays.Value : (int?)null) ?? DBNull.Value;
            cmd.Parameters.Add("@OutputFirstClaimToday", OleDbType.Integer).Value = (object)(r.OutputFirstClaimToday.HasValue ? (r.OutputFirstClaimToday.Value ? -1 : 0) : (int?)null) ?? DBNull.Value;
            cmd.Parameters.AddWithValue("@ApplyTo", (object)r.ApplyTo.ToString() ?? DBNull.Value);
            cmd.Parameters.Add("@AutoApply", OleDbType.Boolean).Value = (object)r.AutoApply;
            cmd.Parameters.AddWithValue("@Message", (object)r.Message ?? DBNull.Value);
            if (includeRuleIdAtEnd) cmd.Parameters.AddWithValue("@RuleId", (object)r.RuleId ?? DBNull.Value);
        }

        private static string GetString(DataRow row, DataTable table, string col)
        {
            if (!table.Columns.Contains(col)) return null;
            var v = row[col];
            if (v == null || v == DBNull.Value) return null;
            return Convert.ToString(v);
        }

        private static bool? GetNullableBool(DataRow row, DataTable table, string col)
        {
            if (!table.Columns.Contains(col)) return null;
            var v = row[col];
            if (v == null || v == DBNull.Value) return null;
            try { return Convert.ToBoolean(v); } catch { return null; }
        }

        private static int? GetNullableInt(DataRow row, DataTable table, string col)
        {
            if (!table.Columns.Contains(col)) return null;
            var v = row[col];
            if (v == null || v == DBNull.Value) return null;
            try { return Convert.ToInt32(v); } catch { return null; }
        }

        private static RuleScope ParseScope(string s)
        {
            if (string.IsNullOrWhiteSpace(s)) return RuleScope.Both;
            if (Enum.TryParse<RuleScope>(s.Trim(), true, out var e)) return e;
            return RuleScope.Both;
        }
        private static ApplyTarget ParseApplyTo(string s)
        {
            if (string.IsNullOrWhiteSpace(s)) return ApplyTarget.Self;
            if (Enum.TryParse<ApplyTarget>(s.Trim(), true, out var e)) return e;
            return ApplyTarget.Self;
        }
        private static string NormalizeWildcard(string s)
        {
            if (string.IsNullOrWhiteSpace(s)) return null;
            s = s.Trim();
            if (s == "*") return "*";
            return s;
        }

        /// <summary>
        /// Seed a set of default rules (idempotent) inspired by legacy reconciliation logic.
        /// Returns the number of rules successfully upserted.
        /// </summary>
        public async Task<int> SeedDefaultRulesAsync(CancellationToken token = default)
        {
            try
            {
                var tableOk = await EnsureRulesTableAsync(token).ConfigureAwait(false);
                if (!tableOk) return 0;

                // Resolve Action and KPI user field IDs by friendly name
                int? A(string name) => ResolveUserFieldId("Action", name);
                int? K(string name) => ResolveUserFieldId("KPI", name);

                var rules = new List<TruthRule>
                {
                    // Heuristic rules with new DWINGS inputs
                    // NOTE: Scope=Import to prevent infinite loops during manual edits
                    new TruthRule { RuleId = "Receivable - First Incoming Payment Request", Scope = RuleScope.Import, AccountSide = "R", TransactionType = "INCOMING_PAYMENT", IsFirstRequest = true, OutputActionId = 1, OutputKpiId = 16, Enabled = true, Priority = 100, ApplyTo = ApplyTarget.Self, AutoApply = true, Message = "First claim request - automatic action assigned" },
                    new TruthRule { RuleId = "Receivable - Issuance Reminder (30+ days)", Scope = RuleScope.Import, AccountSide = "R", GuaranteeType = "ISSUANCE", TransactionType = "INCOMING_PAYMENT", DaysSinceReminderMin = 30, OutputActionId = 3, OutputKpiId = 16, Enabled = true, Priority = 100, ApplyTo = ApplyTarget.Self, AutoApply = true, Message = "Reminder sent automatically via Dwings" },
                    new TruthRule { RuleId = "Receivable - Reissuance Reminder Acknowledged", Scope = RuleScope.Import, AccountSide = "R", GuaranteeType = "REISSUANCE", TransactionType = "INCOMING_PAYMENT", MTStatus = MtStatusCondition.Acked, DaysSinceReminderMin = 30, OutputActionId = 1, OutputKpiId = 16, Enabled = true, Priority = 100, ApplyTo = ApplyTarget.Self, AutoApply = true, Message = "Invoice identified in the Receivable account by RecoTool" },
                    new TruthRule { RuleId = "Receivable - Reissuance Reminder Not Acknowledged", Scope = RuleScope.Import, AccountSide = "R", GuaranteeType = "REISSUANCE", TransactionType = "INCOMING_PAYMENT", MTStatus = MtStatusCondition.NotAcked, DaysSinceReminderMin = 30, OutputActionId = 7, OutputKpiId = 17, Enabled = true, Priority = 100, ApplyTo = ApplyTarget.Self, AutoApply = true, Message = "⚠️ Reminder required - MT791 not acknowledged (30+ days)" },

                    // Pivot side - COLLECTION
                    // NOTE: Scope=Import + CurrentActionId=null to prevent re-applying on daily imports
                    new TruthRule { RuleId = "Pivot - Collection Credit (Grouped)", Scope = RuleScope.Import, AccountSide = "P", TransactionType = "COLLECTION", IsAmountMatch = true, Sign = "C", OutputActionId = 4, OutputKpiId = 18, Enabled = true, Priority = 100, ApplyTo = ApplyTarget.Both, AutoApply = true },
                    new TruthRule { RuleId = "Pivot - Collection Credit (Not Grouped)", Scope = RuleScope.Import, AccountSide = "P", TransactionType = "COLLECTION", Sign = "C", CurrentActionId = null, OutputActionId = 7, OutputKpiId = 18, Enabled = true, Priority = 100, ApplyTo = ApplyTarget.Self, AutoApply = true, Message = "⚠️ Collection Credit without amount match - investigation required" },
                    new TruthRule { RuleId = "Pivot - Collection Debit", Scope = RuleScope.Import, AccountSide = "P", TransactionType = "COLLECTION", Sign = "D", CurrentActionId = null, OutputActionId = 1, OutputKpiId = 19, Enabled = true, Priority = 100, ApplyTo = ApplyTarget.Self, AutoApply = true },

                    // Pivot side - PAYMENT
                    // NOTE: Scope=Import + CurrentActionId=null to prevent re-applying on daily imports
                    new TruthRule { RuleId = "Pivot - Payment Debit", Scope = RuleScope.Import, AccountSide = "P", TransactionType = "PAYMENT", Sign = "D", CurrentActionId = null, OutputActionId = 13, OutputKpiId = 21, Enabled = true, Priority = 100, ApplyTo = ApplyTarget.Self, AutoApply = true },
                    new TruthRule { RuleId = "Pivot - Payment Credit", Scope = RuleScope.Import, AccountSide = "P", TransactionType = "PAYMENT", Sign = "C", CurrentActionId = null, OutputActionId = 7, OutputKpiId = 22, Enabled = true, Priority = 100, ApplyTo = ApplyTarget.Self, AutoApply = true },

                    // Pivot side - Other transaction types
                    // NOTE: Scope=Import + CurrentActionId=null to prevent re-applying on daily imports
                    new TruthRule { RuleId = "Pivot - Adjustment", Scope = RuleScope.Import, AccountSide = "P", TransactionType = "ADJUSTMENT", CurrentActionId = null, OutputActionId = 1, OutputKpiId = 18, Enabled = true, Priority = 100, ApplyTo = ApplyTarget.Self, AutoApply = true },
                    new TruthRule { RuleId = "Pivot - XCL Loader & Trigger", Scope = RuleScope.Import, AccountSide = "P", TransactionType = "XCL_LOADER;TRIGGER", CurrentActionId = null, OutputActionId = 6, OutputKpiId = 18, Enabled = true, Priority = 100, ApplyTo = ApplyTarget.Self, AutoApply = true },
                    new TruthRule { RuleId = "Pivot - Manual Outgoing", Scope = RuleScope.Import, AccountSide = "P", TransactionType = "MANUAL_OUTGOING", CurrentActionId = null, OutputActionId = 4, OutputKpiId = 15, Enabled = true, Priority = 100, ApplyTo = ApplyTarget.Self, AutoApply = true },

                    // Receivable side - INCOMING_PAYMENT with DWINGS conditions
                    // NOTE: Scope=Import + IsFirstRequest=true to apply only on first appearance
                    new TruthRule { RuleId = "Receivable - Reissuance/Advising MT791 Acknowledged", Scope = RuleScope.Import, AccountSide = "R", GuaranteeType = "REISSUANCE;ADVISING", TransactionType = "INCOMING_PAYMENT", MTStatus = MtStatusCondition.Acked, IsFirstRequest = true, OutputActionId = 1, OutputKpiId = 16, OutputFirstClaimToday = true, Enabled = true, Priority = 100, ApplyTo = ApplyTarget.Self, AutoApply = true, Message = "MT791 Sent automatically via Dwings" },
                    new TruthRule { RuleId = "Receivable - Issuance with Email", Scope = RuleScope.Import, AccountSide = "R", GuaranteeType = "ISSUANCE", TransactionType = "INCOMING_PAYMENT", CommIdEmail = true, IsFirstRequest = true, OutputActionId = 1, OutputKpiId = 16, OutputFirstClaimToday = true, Enabled = true, Priority = 100, ApplyTo = ApplyTarget.Self, AutoApply = true, Message = "First claim email sent - awaiting response" },
                    new TruthRule { RuleId = "Receivable - Incoming Payment (Other)", Scope = RuleScope.Import, AccountSide = "R", TransactionType = "INCOMING_PAYMENT", Enabled = true, Priority = 120, ApplyTo = ApplyTarget.Self, AutoApply = true },
                    new TruthRule { RuleId = "Receivable - Reissuance/Advising MT791 Not Acknowledged", Scope = RuleScope.Import, AccountSide = "R", GuaranteeType = "REISSUANCE;ADVISING", TransactionType = "INCOMING_PAYMENT", MTStatus = MtStatusCondition.NotAcked, IsFirstRequest = true, OutputActionId = 2, OutputKpiId = 17, Enabled = true, Priority = 100, ApplyTo = ApplyTarget.Self, AutoApply = true, Message = "⚠️ MT791 not acknowledged - manual follow-up required" },
                    new TruthRule { RuleId = "Receivable - Issuance without Email", Scope = RuleScope.Import, AccountSide = "R", GuaranteeType = "ISSUANCE", TransactionType = "INCOMING_PAYMENT", CommIdEmail = false, IsFirstRequest = true, OutputActionId = 2, OutputKpiId = 17, Enabled = true, Priority = 100, ApplyTo = ApplyTarget.Self, AutoApply = true, Message = "⚠️ No email communication ID - manual claim required" },

                    // Receivable side - Other transaction types
                    // NOTE: Scope=Import + CurrentActionId=null to prevent re-applying on daily imports
                    new TruthRule { RuleId = "Receivable - Direct Debit", Scope = RuleScope.Import, AccountSide = "R", TransactionType = "DIRECT_DEBIT", CurrentActionId = null, OutputActionId = 7, OutputKpiId = 19, Enabled = true, Priority = 100, ApplyTo = ApplyTarget.Self, AutoApply = true },
                    new TruthRule { RuleId = "Receivable - Outgoing Payment (Not Initiated)", Scope = RuleScope.Import, AccountSide = "R", TransactionType = "OUTGOING_PAYMENT", BgiStatusInitiated = false, CurrentActionId = null, OutputActionId = 7, OutputKpiId = 22, Enabled = true, Priority = 100, ApplyTo = ApplyTarget.Self, AutoApply = true },
                    new TruthRule { RuleId = "Receivable - External Debit Payment", Scope = RuleScope.Import, AccountSide = "R", TransactionType = "EXTERNAL_DEBIT_PAYMENT", CurrentActionId = null, OutputActionId = 10, OutputKpiId = 17, Enabled = true, Priority = 100, ApplyTo = ApplyTarget.Self, AutoApply = true },
                    new TruthRule { RuleId = "Receivable - Outgoing Payment (Initiated)", Scope = RuleScope.Import, AccountSide = "R", TransactionType = "OUTGOING_PAYMENT", BgiStatusInitiated = true, CurrentActionId = null, OutputActionId = 5, OutputKpiId = 15, Enabled = true, Priority = 100, ApplyTo = ApplyTarget.Self, AutoApply = true }
                };

                // Only keep rules that have at least one output to apply
                rules = rules.Where(r => r.OutputActionId.HasValue || r.OutputKpiId.HasValue || r.OutputIncidentTypeId.HasValue || r.OutputRiskyItem.HasValue || r.OutputReasonNonRiskyId.HasValue || r.OutputToRemind.HasValue || r.OutputToRemindDays.HasValue || r.OutputFirstClaimToday.HasValue).ToList();

                int saved = 0;
                foreach (var r in rules)
                {
                    var ok = await UpsertRuleAsync(r, token).ConfigureAwait(false);
                    if (ok) saved++;
                }
                return saved;
            }
            catch
            {
                return 0;
            }
        }

        private int? ResolveUserFieldId(string category, string name)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(category) || string.IsNullOrWhiteSpace(name)) return null;
                var all = _offlineFirstService?.UserFields;
                if (all == null) return null;
                string norm(string s) => new string((s ?? "").Where(ch => !char.IsWhiteSpace(ch) && ch != '-' && ch != '_' && ch != ':' && ch != '/').ToArray()).ToUpperInvariant();
                var ncat = category.Trim();
                var nname = norm(name);

                var query = all.Where(u => string.Equals(u?.USR_Category, ncat, StringComparison.OrdinalIgnoreCase));
                foreach (var u in query)
                {
                    var desc = norm(u?.USR_FieldDescription);
                    if (!string.IsNullOrEmpty(desc) && desc == nname)
                        return u.USR_ID;
                }
                foreach (var u in query)
                {
                    var fname = norm(u?.USR_FieldName);
                    if (!string.IsNullOrEmpty(fname) && fname == nname)
                        return u.USR_ID;
                }
                // Common synonyms for Action
                if (string.Equals(ncat, "Action", StringComparison.OrdinalIgnoreCase))
                {
                    if (nname == norm("Not Applicable")) return ResolveUserFieldId(category, "NA");
                    if (nname == norm("Do Pricing")) return ResolveUserFieldId(category, "DoPricing");
                }
                return null;
            }
            catch { return null; }
        }

        /// <summary>
        /// Reads MtStatusCondition enum from database column
        /// </summary>
        private static MtStatusCondition GetMtStatusCondition(System.Data.DataRow row, System.Data.DataTable table, string columnName)
        {
            if (!table.Columns.Contains(columnName) || row[columnName] == DBNull.Value)
                return MtStatusCondition.Wildcard;
            
            var val = Convert.ToString(row[columnName])?.Trim().ToUpperInvariant();
            if (string.IsNullOrEmpty(val) || val == "*" || val == "WILDCARD")
                return MtStatusCondition.Wildcard;
            if (val == "ACKED" || val == "ACK")
                return MtStatusCondition.Acked;
            if (val == "NOTACKED" || val == "NOT_ACKED" || val == "NACK")
                return MtStatusCondition.NotAcked;
            if (val == "NULL" || val == "EMPTY")
                return MtStatusCondition.Null;
            
            return MtStatusCondition.Wildcard;
        }

        /// <summary>
        /// Converts MtStatusCondition enum to string for database storage
        /// </summary>
        private static string MtStatusConditionToString(MtStatusCondition condition)
        {
            switch (condition)
            {
                case MtStatusCondition.Acked:
                    return "ACKED";
                case MtStatusCondition.NotAcked:
                    return "NOT_ACKED";
                case MtStatusCondition.Null:
                    return "NULL";
                case MtStatusCondition.Wildcard:
                default:
                    return "*";
            }
        }
    }
}
