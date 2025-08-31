using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.OleDb;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using RecoTool.Models;
using OfflineFirstAccess.ChangeTracking;
using OfflineFirstAccess.Helpers;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using System.ComponentModel;
using System.Reflection;
using System.Globalization;
using System.Text.Json;

namespace RecoTool.Services
{
    /// <summary>
    /// Service principal de réconciliation
    /// Gère les opérations de réconciliation, règles automatiques, Actions/KPI
    /// </summary>
    public class ReconciliationService
    {
        private readonly string _connectionString;
        private readonly string _currentUser;
        private readonly Dictionary<string, Country> _countries;
        private readonly OfflineFirstService _offlineFirstService;

        public ReconciliationService(string connectionString, string currentUser, IEnumerable<Country> countries)
        {
            _connectionString = connectionString;
            _currentUser = currentUser;
            _countries = countries?.ToDictionary(c => c.CNT_Id, c => c) ?? new Dictionary<string, Country>();
        }

        /// <summary>
        /// Returns all distinct snapshot dates available for a country, sorted ascending.
        /// </summary>
        public async Task<List<DateTime>> GetKpiSnapshotDatesAsync(string countryId, CancellationToken cancellationToken = default)
        {
            var dates = new List<DateTime>();
            if (string.IsNullOrWhiteSpace(countryId)) return dates;

            // KPI snapshots are stored in the Control DB, not the country DB
            using (var connection = new OleDbConnection(GetControlConnectionString()))
            {
                await connection.OpenAsync(cancellationToken);
                await EnsureKpiDailySnapshotTableAsync(connection, cancellationToken);
                var cmd = new OleDbCommand("SELECT DISTINCT SnapshotDate FROM KpiDailySnapshot WHERE CountryId = ? ORDER BY SnapshotDate", connection);
                cmd.Parameters.AddWithValue("@p1", countryId);
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

        /// <summary>
        /// Returns the last known AMBRE operation date in the current dataset for the country.
        /// Used as the snapshot date for the pre-import KPI snapshot.
        /// </summary>
        public async Task<DateTime?> GetLastAmbreOperationDateAsync(string countryId, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(countryId)) return null;

            // Use the AMBRE database for this country
            var ambrePath = _offlineFirstService?.GetLocalAmbreDatabasePath(countryId);
            if (string.IsNullOrWhiteSpace(ambrePath) || !File.Exists(ambrePath)) return null;
            var ambreCs = $"Provider=Microsoft.ACE.OLEDB.16.0;Data Source={ambrePath};";

            using (var connection = new OleDbConnection(ambreCs))
            {
                await connection.OpenAsync(cancellationToken);
                var cmd = new OleDbCommand("SELECT MAX(Operation_Date) FROM T_Data_Ambre", connection);
                var obj = await cmd.ExecuteScalarAsync(cancellationToken);
                if (obj != null && obj != DBNull.Value)
                {
                    try { return Convert.ToDateTime(obj).Date; } catch { return null; }
                }
            }
            return null;
        }

        /// <summary>
        /// Execute a query that returns a single column list of values.
        /// </summary>
        private async Task<List<T>> ExecuteScalarListAsync<T>(string query, string connectionString, params object[] parameters)
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
                                    results.Add((T)Convert.ChangeType(value, targetType));
                                }
                            }
                            catch
                            {
                                // Fallback to string conversion then cast if possible
                                try
                                {
                                    results.Add((T)(object)value.ToString());
                                }
                                catch { results.Add(default); }
                            }
                        }
                    }
                }
            }
            return results;
        }

        public ReconciliationService(string connectionString, string currentUser, IEnumerable<Country> countries, OfflineFirstService offlineFirstService)
            : this(connectionString, currentUser, countries)
        {
            _offlineFirstService = offlineFirstService;
        }

        public string CurrentUser => _currentUser;

        #region Data Retrieval

        /// <summary>
        /// Récupère toutes les données Ambre pour un pays
        /// </summary>
        public async Task<List<DataAmbre>> GetAmbreDataAsync(string countryId, bool includeDeleted = false)
        {
            // Ambre est désormais dans une base séparée par pays
            var ambrePath = _offlineFirstService?.GetLocalAmbreDatabasePath(countryId);
            if (string.IsNullOrWhiteSpace(ambrePath))
                throw new InvalidOperationException("Chemin de la base AMBRE introuvable pour le pays courant.");
            var ambreCs = $"Provider=Microsoft.ACE.OLEDB.16.0;Data Source={ambrePath};";

            var query = @"SELECT * FROM T_Data_Ambre";
            if (!includeDeleted)
                query += " WHERE DeleteDate IS NULL";
            query += " ORDER BY Operation_Date DESC";

            return await ExecuteQueryAsync<DataAmbre>(query, ambreCs);
        }

        /// <summary>
        /// Récupère les données de réconciliation pour un pays
        /// </summary>
        public async Task<List<Reconciliation>> GetReconciliationDataAsync(string countryId, bool includeDeleted = false)
        {
            // Jointure entre Réconciliation (base locale courante) et AMBRE (base séparée)
            string ambrePath = _offlineFirstService?.GetLocalAmbreDatabasePath(countryId);
            string ambreEsc = string.IsNullOrEmpty(ambrePath) ? null : ambrePath.Replace("'", "''");
            string ambreJoin = string.IsNullOrEmpty(ambreEsc) ? "T_Data_Ambre AS a" : $"(SELECT * FROM [{ambreEsc}].T_Data_Ambre) AS a";

            var query = $@"SELECT r.* FROM (T_Reconciliation AS r 
                         INNER JOIN {ambreJoin} ON r.ID = a.ID)";
            if (!includeDeleted)
                query += " WHERE r.DeleteDate IS NULL";
            query += " ORDER BY r.LastModified DESC";

            return await ExecuteQueryAsync<Reconciliation>(query);
        }

        /// <summary>
        /// Récupère uniquement les réconciliations dont l'action est TRIGGER (non supprimées)
        /// </summary>
        public async Task<List<Reconciliation>> GetTriggerReconciliationsAsync(string countryId)
        {
            // Jointure sur AMBRE identique pour respecter la portée pays, mais seules les colonnes r.* sont nécessaires
            string ambrePath = _offlineFirstService?.GetLocalAmbreDatabasePath(countryId);
            string ambreEsc = string.IsNullOrEmpty(ambrePath) ? null : ambrePath.Replace("'", "''");
            string ambreJoin = string.IsNullOrEmpty(ambreEsc) ? "T_Data_Ambre AS a" : $"(SELECT * FROM [{ambreEsc}].T_Data_Ambre) AS a";

            // Filter by Action = Trigger (use enum to avoid hardcoded IDs)
            var query = $@"SELECT r.* FROM (T_Reconciliation AS r
                             INNER JOIN {ambreJoin} ON r.ID = a.ID)
                           WHERE r.DeleteDate IS NULL AND r.Action = ?
                           ORDER BY r.LastModified DESC";

            return await ExecuteQueryAsync<Reconciliation>(query, _connectionString, (int)ActionType.Trigger);
        }

        /// <summary>
        /// Récupère les données jointes Ambre + Réconciliation
        /// </summary>
        public async Task<List<ReconciliationViewData>> GetReconciliationViewAsync(string countryId, string filterSql = null)
        {
            var swBuild = Stopwatch.StartNew();
            string dwPath = _offlineFirstService?.GetLocalDWDatabasePath();
            string ambrePath = _offlineFirstService?.GetLocalAmbreDatabasePath(countryId);
            string dwEsc = string.IsNullOrEmpty(dwPath) ? null : dwPath.Replace("'", "''");
            string ambreEsc = string.IsNullOrEmpty(ambrePath) ? null : ambrePath.Replace("'", "''");
            // Build JOIN targets: use IN 'path' subqueries when external, otherwise direct tables
            string dwDataJoinInv = string.IsNullOrEmpty(dwEsc) ? "T_DW_Data AS dInv" : $"(SELECT * FROM [{dwEsc}].T_DW_Data) AS dInv";
            string dwGuaranteeJoin = string.IsNullOrEmpty(dwEsc) ? "T_DW_Guarantee AS g" : $"(SELECT * FROM [{dwEsc}].T_DW_Guarantee) AS g";
            string ambreJoin = string.IsNullOrEmpty(ambreEsc) ? "T_Data_Ambre AS a" : $"(SELECT * FROM [{ambreEsc}].T_Data_Ambre) AS a";

            var query = $@"SELECT 
                                   a.*,
                                   r.DWINGS_GuaranteeID,
                                   r.DWINGS_InvoiceID,
                                   r.DWINGS_CommissionID,
                                   r.Action,
                                   r.Assignee,
                                   r.Comments,
                                   r.InternalInvoiceReference,
                                   r.FirstClaimDate,
                                   r.LastClaimDate,
                                   r.ToRemind,
                                   r.ToRemindDate,
                                   r.ACK,
                                   r.SwiftCode,
                                   r.PaymentReference,
                                   r.KPI,
                                   r.IncidentType,
                                   r.RiskyItem,
                                   r.ReasonNonRisky,
                                   r.ModifiedBy AS Reco_ModifiedBy,
                                    NULL AS SYNDICATE,
                                    g.OUTSTANDING_AMOUNT AS GUARANTEE_AMOUNT,
                                    g.CURRENCYNAME AS GUARANTEE_CURRENCY,
                                    g.GUARANTEE_STATUS AS GUARANTEE_STATUS,
                                    g.GUARANTEE_TYPE AS GUARANTEE_TYPE,
                                   dInv.INVOICE_ID AS INVOICE_ID,
                                    NULL AS COMMISSION_ID,
                                    g.GUARANTEE_ID,

                                  g.NATURE AS G_NATURE,
                                  g.EVENT_STATUS AS G_EVENT_STATUS,
                                  g.EVENT_EFFECTIVEDATE AS G_EVENT_EFFECTIVEDATE,
                                  g.ISSUEDATE AS G_ISSUEDATE,
                                  g.OFFICIALREF AS G_OFFICIALREF,
                                  g.UNDERTAKINGEVENT AS G_UNDERTAKINGEVENT,
                                  g.PROCESS AS G_PROCESS,
                                  g.EXPIRYDATETYPE AS G_EXPIRYDATETYPE,
                                  g.EXPIRYDATE AS G_EXPIRYDATE,
                                  g.PARTY_ID AS G_PARTY_ID,
                                  g.PARTY_REF AS G_PARTY_REF,
                                  g.SECONDARY_OBLIGOR AS G_SECONDARY_OBLIGOR,
                                  g.SECONDARY_OBLIGOR_NATURE AS G_SECONDARY_OBLIGOR_NATURE,
                                  g.ROLE AS G_ROLE,
                                  g.COUNTRY AS G_COUNTRY,
                                  g.CENTRAL_PARTY_CODE AS G_CENTRAL_PARTY_CODE,
                                  g.NAME1 AS G_NAME1,
                                  g.NAME2 AS G_NAME2,
                                  g.GROUPE AS G_GROUPE,
                                  g.PREMIUM AS G_PREMIUM,
                                  g.BRANCH_CODE AS G_BRANCH_CODE,
                                  g.BRANCH_NAME AS G_BRANCH_NAME,
                                  g.OUTSTANDING_AMOUNT_IN_BOOKING_CURRENCY AS G_OUTSTANDING_AMOUNT_IN_BOOKING_CURRENCY,
                                  g.CANCELLATIONDATE AS G_CANCELLATIONDATE,
                                  g.CONTROLER AS G_CONTROLER,
                                  g.AUTOMATICBOOKOFF AS G_AUTOMATICBOOKOFF,
                                  g.NATUREOFDEAL AS G_NATUREOFDEAL,
                                  g.GUARANTEE_TYPE AS G_GUARANTEE_TYPE,

                                  dInv.REQUESTED_INVOICE_AMOUNT AS I_REQUESTED_INVOICE_AMOUNT,
                                  dInv.SENDER_NAME AS I_SENDER_NAME,
                                  dInv.RECEIVER_NAME AS I_RECEIVER_NAME,
                                  dInv.SENDER_REFERENCE AS I_SENDER_REFERENCE,
                                  dInv.RECEIVER_REFERENCE AS I_RECEIVER_REFERENCE,
                                  dInv.T_INVOICE_STATUS AS I_T_INVOICE_STATUS,
                                  dInv.BILLING_AMOUNT AS I_BILLING_AMOUNT,
                                  dInv.BILLING_CURRENCY AS I_BILLING_CURRENCY,
                                  dInv.START_DATE AS I_START_DATE,
                                  dInv.END_DATE AS I_END_DATE,
                                  dInv.FINAL_AMOUNT AS I_FINAL_AMOUNT,
                                  dInv.T_COMMISSION_PERIOD_STATUS AS I_T_COMMISSION_PERIOD_STATUS,
                                  dInv.BUSINESS_CASE_REFERENCE AS I_BUSINESS_CASE_REFERENCE,
                                  dInv.BUSINESS_CASE_ID AS I_BUSINESS_CASE_ID,
                                  dInv.POSTING_PERIODICITY AS I_POSTING_PERIODICITY,
                                  dInv.EVENT_ID AS I_EVENT_ID,
                                  dInv.COMMENTS AS I_COMMENTS,
                                  dInv.SENDER_ACCOUNT_NUMBER AS I_SENDER_ACCOUNT_NUMBER,
                                  dInv.SENDER_ACCOUNT_BIC AS I_SENDER_ACCOUNT_BIC,
                                  dInv.RECEIVER_ACCOUNT_NUMBER AS I_RECEIVER_ACCOUNT_NUMBER,
                                  dInv.RECEIVER_ACCOUNT_BIC AS I_RECEIVER_ACCOUNT_BIC,
                                  dInv.REQUESTED_AMOUNT AS I_REQUESTED_AMOUNT,
                                  dInv.EXECUTED_AMOUNT AS I_EXECUTED_AMOUNT,
                                  dInv.REQUESTED_EXECUTION_DATE AS I_REQUESTED_EXECUTION_DATE,
                                  dInv.T_PAYMENT_REQUEST_STATUS AS I_T_PAYMENT_REQUEST_STATUS,
                                  dInv.BGPMT AS I_BGPMT,
                                  dInv.DEBTOR_ACCOUNT_ID AS I_DEBTOR_ACCOUNT_ID,
                                  dInv.CREDITOR_ACCOUNT_ID AS I_CREDITOR_ACCOUNT_ID,
                                  dInv.MT_STATUS AS I_MT_STATUS,
                                  dInv.REMINDER_NUMBER AS I_REMINDER_NUMBER,
                                  dInv.ERROR_MESSAGE AS I_ERROR_MESSAGE,
                                  dInv.DEBTOR_PARTY_ID AS I_DEBTOR_PARTY_ID,
                                  dInv.PAYMENT_METHOD AS I_PAYMENT_METHOD,
                                  dInv.PAYMENT_TYPE AS I_PAYMENT_TYPE,
                                  dInv.DEBTOR_PARTY_NAME AS I_DEBTOR_PARTY_NAME,
                                  dInv.DEBTOR_ACCOUNT_NUMBER AS I_DEBTOR_ACCOUNT_NUMBER,
                                  dInv.CREDITOR_PARTY_ID AS I_CREDITOR_PARTY_ID,
                                  dInv.CREDITOR_ACCOUNT_NUMBER AS I_CREDITOR_ACCOUNT_NUMBER
                           FROM (({ambreJoin} 
                           LEFT JOIN T_Reconciliation AS r ON a.ID = r.ID)
                           LEFT JOIN {dwDataJoinInv} ON r.DWINGS_InvoiceID = dInv.INVOICE_ID)
                           LEFT JOIN {dwGuaranteeJoin} ON  g.GUARANTEE_ID = r.DWINGS_GuaranteeID
                           WHERE 1=1";

            if (!string.IsNullOrEmpty(filterSql))
            {
                var cond = filterSql.Trim();
                // 1) Strip optional embedded JSON snapshot comment prefix first
                var m = Regex.Match(cond, @"^/\*JSON:(.*?)\*/\s*(.*)$", RegexOptions.Singleline);
                if (m.Success)
                    cond = m.Groups[2].Value?.Trim();
                // 2) If wrapped in a single pair of parentheses, unwrap and trim repeatedly
                while (!string.IsNullOrEmpty(cond) && cond.StartsWith("(") && cond.EndsWith(")"))
                {
                    cond = cond.Substring(1, cond.Length - 2).Trim();
                }
                // 3) Strip leading WHERE if present (case-insensitive)
                if (cond.StartsWith("WHERE ", StringComparison.OrdinalIgnoreCase))
                    cond = cond.Substring(6).Trim();

                // 4) Minimal safety gate: reject dangerous keywords to avoid logic injections or heavy subqueries
                if (!string.IsNullOrEmpty(cond))
                {
                    var lower = cond.ToLowerInvariant();
                    string[] banned = { " union ", " select ", " insert ", " delete ", " update ", " drop ", " alter ", " exec ", ";" };
                    bool hasBanned = banned.Any(k => lower.Contains(k));
                    if (!hasBanned)
                    {
                        query += $" AND ({cond})";
                    }
                    else
                    {
                        try { System.Diagnostics.Debug.WriteLine("[RecoView][WARN] Unsafe filterSql detected and ignored."); } catch { }
                    }
                }
            }

            query += " ORDER BY a.Operation_Date DESC";

            swBuild.Stop();
            var swExec = Stopwatch.StartNew();
            var list = await ExecuteQueryAsync<ReconciliationViewData>(query);
            swExec.Stop();

            return list;
        }

        #endregion

        /// <summary>
        /// Returns Control DB connection string via OfflineFirstService, with fallback to local DB if not configured.
        /// </summary>
        private string GetControlConnectionString()
        {
            if (_offlineFirstService != null)
                return _offlineFirstService.GetControlConnectionString(_offlineFirstService.CurrentCountryId);
            return _connectionString;
        }

        /// <summary>
        /// Ensure KpiDailySnapshot table exists in the provided connection. Creates it if missing.
        /// </summary>
        private async Task EnsureKpiDailySnapshotTableAsync(OleDbConnection connection, CancellationToken ct)
        {
            // Check existence via schema
            var tables = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, new object[] { null, null, "KpiDailySnapshot", "TABLE" });
            if (tables != null && tables.Rows.Count > 0) return;

            var createSql = @"CREATE TABLE KpiDailySnapshot (
                Id COUNTER PRIMARY KEY,
                SnapshotDate DATETIME,
                CountryId TEXT(50),
                MissingInvoices LONG,
                PaidNotReconciled LONG,
                UnderInvestigation LONG,
                ReceivableCount LONG,
                ReceivableAmount CURRENCY,
                PivotCount LONG,
                PivotAmount CURRENCY,
                NewCount LONG,
                DeletedCount LONG,
                DeletionDelayBucketsJson LONGTEXT,
                ReceivablePivotByActionJson LONGTEXT,
                KpiDistributionJson LONGTEXT,
                KpiRiskMatrixJson LONGTEXT,
                CurrencyDistributionJson LONGTEXT,
                ActionDistributionJson LONGTEXT,
                CreatedAtUtc DATETIME,
                SourceVersion TEXT(255),
                FrozenAt DATETIME
            )";

            using (var cmd = new OleDbCommand(createSql, connection))
            {
                await cmd.ExecuteNonQueryAsync(ct);
            }
        }

        #region Exports (T_param driven)

        /// <summary>
        /// Reads a SQL payload from referential table T_param.Par_Value using a flexible key lookup.
        /// Accepts keys like Export_KPI, Export_PastDUE, Export_IT.
        /// </summary>
        public async Task<string> GetParamValueAsync(string paramKey, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(paramKey)) return null;

            var cs = GetReferentialConnectionString();
            using (var connection = new OleDbConnection(cs))
            {
                cancellationToken.ThrowIfCancellationRequested();
                await connection.OpenAsync();

                // Try common key column names to avoid coupling to a specific schema naming
                string[] keyColumns = { "Par_Key", "Par_Code", "Par_Name", "PAR_Key", "PAR_Code", "PAR_Name" };
                foreach (var col in keyColumns)
                {
                    try
                    {
                        var cmd = new OleDbCommand($"SELECT TOP 1 Par_Value FROM T_param WHERE {col} = ?", connection);
                        cmd.Parameters.AddWithValue("@p1", paramKey);
                        cancellationToken.ThrowIfCancellationRequested();
                        var obj = await cmd.ExecuteScalarAsync();
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

        /// <summary>
        /// Build OleDb parameters for a detected parameter name set.
        /// Supported names: @CountryId, @AccountId, @FromDate, @ToDate, @UserId
        /// </summary>
        public IEnumerable<DbParameter> BuildSqlParameters(IEnumerable<string> paramNames, string countryId, string accountId, DateTime? fromDate, DateTime? toDate, string userId)
        {
            var list = new List<DbParameter>();
            if (paramNames == null) return list;

            foreach (var name in paramNames)
            {
                var p = new OleDbParameter();
                p.ParameterName = "@" + name;
                switch (name.ToLowerInvariant())
                {
                    case "countryid":
                        p.Value = (object)(countryId ?? string.Empty) ?? DBNull.Value; break;
                    case "accountid":
                        p.Value = (object)(accountId ?? string.Empty) ?? DBNull.Value; break;
                    case "fromdate":
                        p.Value = fromDate.HasValue ? (object)fromDate.Value.ToOADate() : DBNull.Value; break;
                    case "todate":
                        p.Value = toDate.HasValue ? (object)toDate.Value.ToOADate() : DBNull.Value; break;
                    case "userid":
                        p.Value = (object)(userId ?? _currentUser ?? string.Empty) ?? DBNull.Value; break;
                    default:
                        // Unknown param -> set as DBNull
                        p.Value = DBNull.Value; break;
                }
                list.Add(p);
            }
            return list;
        }

        /// <summary>
        /// Execute arbitrary SQL (select) and return a DataTable. Parameters must be OleDb-compatible.
        /// </summary>
        public async Task<DataTable> ExecuteExportAsync(string sql, IEnumerable<DbParameter> parameters, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(sql)) throw new ArgumentNullException(nameof(sql));

            using (var connection = new OleDbConnection(_connectionString))
            {
                cancellationToken.ThrowIfCancellationRequested();
                await connection.OpenAsync();
                using (var cmd = new OleDbCommand(sql, connection))
                {
                    if (parameters != null)
                    {
                        foreach (var p in parameters)
                        {
                            // OleDb uses positional parameters; however we still attach names for clarity
                            var clone = new OleDbParameter
                            {
                                ParameterName = p.ParameterName,
                                Value = p.Value ?? DBNull.Value
                            };
                            cmd.Parameters.Add(clone);
                        }
                    }

                    using (var adapter = new OleDbDataAdapter(cmd))
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        var table = new DataTable();
                        adapter.Fill(table);
                        return table;
                    }
                }
            }
        }

        #endregion

        #region Connection helpers

        /// <summary>
        /// Returns the referential database connection string from OfflineFirstService.
        /// Throws if unavailable.
        /// </summary>
        private string GetReferentialConnectionString()
        {
            var refCs = _offlineFirstService?.ReferentialDatabasePath;
            if (string.IsNullOrWhiteSpace(refCs))
                throw new InvalidOperationException("Referential connection string is required for saved views (inject OfflineFirstService).");
            return refCs;
        }

        #endregion

        #region Automatic Rules and Actions

        /// <summary>
        /// Applique les règles automatiques pour déterminer Actions et KPI
        /// </summary>
        public async Task<bool> ApplyAutomaticRulesAsync(string countryId)
        {
            try
            {
                var country = _countries.ContainsKey(countryId) ? _countries[countryId] : null;
                if (country == null) return false;

                var ambreData = await GetAmbreDataAsync(countryId);
                var updates = new List<Reconciliation>();

                foreach (var data in ambreData)
                {
                    var reconciliation = await GetOrCreateReconciliationAsync(data.ID);

                    // Déterminer si c'est Pivot ou Receivable
                    bool isPivot = data.IsPivotAccount(country.CNT_AmbrePivot);
                    bool isReceivable = data.IsReceivableAccount(country.CNT_AmbreReceivable);

                    if (isPivot)
                    {
                        ApplyPivotRules(reconciliation, data);
                    }
                    else if (isReceivable)
                    {
                        ApplyReceivableRules(reconciliation, data);
                    }

                    updates.Add(reconciliation);
                }

                // Sauvegarder en batch
                await SaveReconciliationsAsync(updates);
                return true;
            }
            catch (Exception ex)
            {
                throw new Exception($"Erreur lors de l'application des règles automatiques: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Applique les règles pour le compte Pivot
        /// </summary>
        private void ApplyPivotRules(Reconciliation reconciliation, DataAmbre data)
        {
            var transactionType = data.Pivot_TransactionCodesFromLabel?.ToUpper();
            bool isCredit = data.SignedAmount > 0;

            switch (transactionType)
            {
                case "COLLECTION":
                    reconciliation.Action = isCredit ? (int)ActionType.Match : (int)ActionType.NA;
                    reconciliation.KPI = isCredit ? (int)KPIType.PaidButNotReconciled : (int)KPIType.ITIssues;
                    break;

                case "PAYMENT":
                case "AUTOMATIC REFUND":
                    reconciliation.Action = !isCredit ? (int)ActionType.DoPricing : (int)ActionType.NA;
                    reconciliation.KPI = !isCredit ? (int)KPIType.CorrespondentChargesToBeInvoiced : (int)KPIType.ITIssues;
                    break;

                case "ADJUSTMENT":
                    reconciliation.Action = (int)ActionType.Adjust;
                    reconciliation.KPI = (int)KPIType.PaidButNotReconciled;
                    break;

                case "XCL LOADER":
                    reconciliation.Action = isCredit ? (int)ActionType.Match : (int)ActionType.Investigate;
                    reconciliation.KPI = isCredit ? (int)KPIType.PaidButNotReconciled : (int)KPIType.UnderInvestigation;
                    break;

                case "TRIGGER":
                    if (isCredit)
                    {
                        reconciliation.Action = (int)ActionType.Investigate;
                        reconciliation.KPI = (int)KPIType.UnderInvestigation;
                    }
                    else
                    {
                        reconciliation.Action = (int)ActionType.DoPricing;
                        reconciliation.KPI = (int)KPIType.CorrespondentChargesToBeInvoiced;
                    }
                    break;

                default:
                    reconciliation.Action = (int)ActionType.Investigate;
                    reconciliation.KPI = (int)KPIType.UnderInvestigation;
                    break;
            }
        }

        /// <summary>
        /// Applique les règles pour le compte Receivable
        /// </summary>
        private void ApplyReceivableRules(Reconciliation reconciliation, DataAmbre data)
        {
            var transactionType = data.Pivot_TransactionCodesFromLabel?.ToUpper();
            var guaranteeType = ExtractGuaranteeTypeFromLabel(data.RawLabel);

            switch (transactionType)
            {
                case "INCOMING PAYMENT":
                    switch (guaranteeType?.ToUpper())
                    {
                        case "REISSUANCE":
                            reconciliation.Action = (int)ActionType.Request;
                            reconciliation.KPI = (int)KPIType.NotClaimed;
                            break;
                        case "ISSUANCE":
                            reconciliation.Action = (int)ActionType.NA;
                            reconciliation.KPI = (int)KPIType.ClaimedButNotPaid;
                            break;
                        case "ADVISING":
                            reconciliation.Action = (int)ActionType.Trigger;
                            reconciliation.KPI = (int)KPIType.PaidButNotReconciled;
                            break;
                        default:
                            reconciliation.Action = (int)ActionType.Investigate;
                            reconciliation.KPI = (int)KPIType.ITIssues;
                            break;
                    }
                    break;

                case "DIRECT DEBIT":
                    reconciliation.Action = (int)ActionType.Investigate;
                    reconciliation.KPI = (int)KPIType.ITIssues;
                    break;

                case "MANUAL OUTGOING":
                case "OUTGOING PAYMENT":
                    reconciliation.Action = (int)ActionType.Trigger;
                    reconciliation.KPI = (int)KPIType.CorrespondentChargesPendingTrigger;
                    break;

                case "EXTERNAL DEBIT PAYMENT":
                    reconciliation.Action = (int)ActionType.Execute;
                    reconciliation.KPI = (int)KPIType.NotClaimed;
                    break;

                default:
                    reconciliation.Action = (int)ActionType.Investigate;
                    reconciliation.KPI = (int)KPIType.ITIssues;
                    break;
            }
        }

        /// <summary>
        /// Extrait le type de garantie depuis le libellé
        /// </summary>
        private string ExtractGuaranteeTypeFromLabel(string rawLabel)
        {
            if (string.IsNullOrEmpty(rawLabel)) return null;

            var upperLabel = rawLabel.ToUpper();
            if (upperLabel.Contains("REISSUANCE")) return "REISSUANCE";
            if (upperLabel.Contains("ISSUANCE")) return "ISSUANCE";
            if (upperLabel.Contains("ADVISING")) return "ADVISING";

            return null;
        }

        #endregion

        #region Automatic Matching

        /// <summary>
        /// Effectue un rapprochement automatique basé sur les références
        /// </summary>
        public async Task<int> PerformAutomaticMatchingAsync(string countryId)
        {
            try
            {
                var country = _countries.ContainsKey(countryId) ? _countries[countryId] : null;
                if (country == null) return 0;

                var ambreData = await GetAmbreDataAsync(countryId);
                var pivotLines = ambreData.Where(d => d.IsPivotAccount(country.CNT_AmbrePivot)).ToList();
                var receivableLines = ambreData.Where(d => d.IsReceivableAccount(country.CNT_AmbreReceivable)).ToList();

                int matchCount = 0;

                foreach (var receivableLine in receivableLines)
                {
                    if (string.IsNullOrEmpty(receivableLine.Receivable_InvoiceFromAmbre)) continue;

                    // Rechercher des lignes pivot avec la même référence invoice
                    var matchingPivotLines = pivotLines.Where(p =>
                        !string.IsNullOrEmpty(p.Pivot_MbawIDFromLabel) &&
                        p.Pivot_MbawIDFromLabel.Contains(receivableLine.Receivable_InvoiceFromAmbre))
                        .ToList();

                    if (matchingPivotLines.Any())
                    {
                        // Créer ou mettre à jour les réconciliations
                        await CreateMatchingReconciliationsAsync(receivableLine, matchingPivotLines);
                        matchCount++;
                    }
                }

                return matchCount;
            }
            catch (Exception ex)
            {
                throw new Exception($"Erreur lors du rapprochement automatique: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Crée les réconciliations pour les lignes appariées
        /// </summary>
        private async Task CreateMatchingReconciliationsAsync(DataAmbre receivableLine, List<DataAmbre> pivotLines)
        {
            var receivableReco = await GetOrCreateReconciliationAsync(receivableLine.ID).ConfigureAwait(false);

            // Marquer comme matché et ajouter commentaire
            receivableReco.Action = (int)ActionType.Match;
            receivableReco.KPI = (int)KPIType.PaidButNotReconciled;
            receivableReco.Comments = $"Auto-matched with {pivotLines.Count} pivot line(s)";

            var pivotTasks = pivotLines.Select(p => GetOrCreateReconciliationAsync(p.ID));
            var pivotReconciliations = await Task.WhenAll(pivotTasks).ConfigureAwait(false);

            foreach (var pivotReco in pivotReconciliations)
            {
                pivotReco.Action = (int)ActionType.Match;
                pivotReco.KPI = (int)KPIType.PaidButNotReconciled;
                pivotReco.Comments = $"Auto-matched with receivable line {receivableLine.ID}";
            }

            await SaveReconciliationsAsync(new[] { receivableReco }.Concat(pivotReconciliations)).ConfigureAwait(false);
        }

        #endregion

        #region CRUD Operations

        /// <summary>
        /// Récupère ou crée une réconciliation pour une ligne Ambre
        /// </summary>
        public async Task<Reconciliation> GetOrCreateReconciliationAsync(string id)
        {
            // Lookup by ID
            var query = "SELECT * FROM T_Reconciliation WHERE ID = ? AND DeleteDate IS NULL";
            // Explicitly pass the connection string to avoid binding to the wrong overload (id mistaken for connection string)
            var existing = await ExecuteQueryAsync<Reconciliation>(query, _connectionString, id).ConfigureAwait(false);

            if (existing.Any())
                return existing.First();

            return Reconciliation.CreateForAmbreLine(id);
        }

        /// <summary>
        /// Sauvegarde une réconciliation
        /// </summary>
        public async Task<bool> SaveReconciliationAsync(Reconciliation reconciliation)
        {
            return await SaveReconciliationsAsync(new[] { reconciliation });
        }

        /// <summary>
        /// Sauvegarde plusieurs réconciliations en batch
        /// </summary>
        public async Task<bool> SaveReconciliationsAsync(IEnumerable<Reconciliation> reconciliations)
        {
            try
            {
                using (var connection = new OleDbConnection(_connectionString))
                {
                    await connection.OpenAsync().ConfigureAwait(false);
                    using (var transaction = connection.BeginTransaction())
                    {
                        try
                        {
                            var changeTuples = new List<(string TableName, string RecordId, string OperationType)>();
                            foreach (var reconciliation in reconciliations)
                            {
                                var op = await SaveSingleReconciliationAsync(connection, transaction, reconciliation).ConfigureAwait(false);
                                if (!string.Equals(op, "NOOP", StringComparison.OrdinalIgnoreCase))
                                {
                                    changeTuples.Add(("T_Reconciliation", reconciliation.ID, op));
                                }
                            }

                            transaction.Commit();

                            // Record changes in ChangeLog (stored locally via OfflineFirstService configuration)
                            try
                            {
                                if (_offlineFirstService != null && changeTuples.Count > 0)
                                {
                                    var countryId = _offlineFirstService.CurrentCountryId;
                                    if (!string.IsNullOrWhiteSpace(countryId))
                                    {
                                        using (var session = await _offlineFirstService.BeginChangeLogSessionAsync(countryId).ConfigureAwait(false))
                                        {
                                            foreach (var t in changeTuples)
                                            {
                                                await session.AddAsync(t.TableName, t.RecordId, t.OperationType).ConfigureAwait(false);
                                            }
                                            await session.CommitAsync().ConfigureAwait(false);
                                        }
                                    }
                                }
                            }
                            catch
                            {
                                // Swallow change-log errors to not block user saves
                            }

                            // Background push of reconciliation changes via serialized queue
                            try
                            {
                                if (_offlineFirstService != null)
                                {
                                    var cid = _offlineFirstService.CurrentCountryId;
                                    if (!string.IsNullOrWhiteSpace(cid))
                                    {
                                        BackgroundTaskQueue.Instance.Enqueue(async () =>
                                        {
                                            try { await _offlineFirstService.PushReconciliationIfPendingAsync(cid).ConfigureAwait(false); }
                                            catch { }
                                        });
                                    }
                                }
                            }
                            catch { }
                            return true;
                        }
                        catch
                        {
                            transaction.Rollback();
                            throw;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception($"Erreur lors de la sauvegarde des réconciliations: {ex.Message}", ex);
            }
        }



        /// <summary>
        /// Sauvegarde une réconciliation unique dans une transaction
        /// </summary>
        private async Task<string> SaveSingleReconciliationAsync(OleDbConnection connection, OleDbTransaction transaction, Reconciliation reconciliation)
        {
            // Vérifier si l'enregistrement existe (par ID)
            var checkQuery = "SELECT COUNT(*) FROM T_Reconciliation WHERE ID = ?";
            using (var checkCmd = new OleDbCommand(checkQuery, connection, transaction))
            {
                checkCmd.Parameters.AddWithValue("@ID", reconciliation.ID);
                var exists = (int)await checkCmd.ExecuteScalarAsync().ConfigureAwait(false) > 0;

                // If the row exists, compare business fields to avoid no-op updates
                if (exists)
                {
                    var changed = new System.Collections.Generic.List<string>();
                    var selectCmd = new OleDbCommand(@"SELECT 
                                [DWINGS_GuaranteeID], [DWINGS_InvoiceID], [DWINGS_CommissionID],
                                [Action], [Assignee], [Comments], [InternalInvoiceReference],
                                [FirstClaimDate], [LastClaimDate], [ToRemind], [ToRemindDate],
                                [ACK], [SwiftCode], [PaymentReference], [KPI],
                                [IncidentType], [RiskyItem], [ReasonNonRisky],
                                [MbawData], [SpiritData]
                              FROM T_Reconciliation WHERE [ID] = ?", connection, transaction);
                    selectCmd.Parameters.AddWithValue("@ID", reconciliation.ID);
                    using (var rdr = await selectCmd.ExecuteReaderAsync().ConfigureAwait(false))
                    {
                        if (await rdr.ReadAsync().ConfigureAwait(false))
                        {
                            object DbVal(int i) => rdr.IsDBNull(i) ? null : rdr.GetValue(i);
                            bool Equal(object a, object b) => (a == null && b == null) || (a != null && a.Equals(b));

                            bool? DbBool(object o)
                            {
                                if (o == null) return null;
                                try
                                {
                                    if (o is bool bb) return bb;
                                    if (o is byte by) return by != 0;
                                    if (o is short s) return s != 0;
                                    if (o is int ii) return ii != 0;
                                    return Convert.ToBoolean(o);
                                }
                                catch { return null; }
                            }

                            // Build the list of changed business fields
                            if (!Equal(DbVal(0), (object)reconciliation.DWINGS_GuaranteeID)) changed.Add("DWINGS_GuaranteeID");
                            if (!Equal(DbVal(1), (object)reconciliation.DWINGS_InvoiceID)) changed.Add("DWINGS_InvoiceID");
                            if (!Equal(DbVal(2), (object)reconciliation.DWINGS_CommissionID)) changed.Add("DWINGS_CommissionID");
                            if (!Equal(DbVal(3), (object)reconciliation.Action)) changed.Add("Action");
                            if (!Equal(DbVal(4), (object)reconciliation.Assignee)) changed.Add("Assignee");
                            if (!Equal(DbVal(5), (object)reconciliation.Comments)) changed.Add("Comments");
                            if (!Equal(DbVal(6), (object)reconciliation.InternalInvoiceReference)) changed.Add("InternalInvoiceReference");
                            if (!Equal(DbVal(7), reconciliation.FirstClaimDate.HasValue ? (object)reconciliation.FirstClaimDate.Value : null)) changed.Add("FirstClaimDate");
                            if (!Equal(DbVal(8), reconciliation.LastClaimDate.HasValue ? (object)reconciliation.LastClaimDate.Value : null)) changed.Add("LastClaimDate");
                            if (!Equal(DbBool(DbVal(9)), (object)reconciliation.ToRemind)) changed.Add("ToRemind");
                            if (!Equal(DbVal(10), reconciliation.ToRemindDate.HasValue ? (object)reconciliation.ToRemindDate.Value : null)) changed.Add("ToRemindDate");
                            if (!Equal(DbBool(DbVal(11)), (object)reconciliation.ACK)) changed.Add("ACK");
                            if (!Equal(DbVal(12), (object)reconciliation.SwiftCode)) changed.Add("SwiftCode");
                            if (!Equal(DbVal(13), (object)reconciliation.PaymentReference)) changed.Add("PaymentReference");
                            if (!Equal(DbVal(14), (object)reconciliation.KPI)) changed.Add("KPI");
                            if (!Equal(DbVal(15), (object)reconciliation.IncidentType)) changed.Add("IncidentType");
                            if (!Equal(DbBool(DbVal(16)), (object)reconciliation.RiskyItem)) changed.Add("RiskyItem");
                            if (!Equal(DbVal(17), (object)reconciliation.ReasonNonRisky)) changed.Add("ReasonNonRisky");
                            if (!Equal(DbVal(18), (object)reconciliation.MbawData)) changed.Add("MbawData");
                            if (!Equal(DbVal(19), (object)reconciliation.SpiritData)) changed.Add("SpiritData");

                            if (changed.Count == 0)
                            {
                                // No business-field change: skip UPDATE and ChangeLog
                                LogManager.Debug($"Reconciliation NOOP: ID={reconciliation.ID} - no business-field changes detected.");
                                return "NOOP";
                            }
                        }
                    }

                    // Apply update with refreshed modification metadata (partial update of changed fields only)
                    LogManager.Debug($"Reconciliation UPDATE detected: ID={reconciliation.ID} Changed=[{string.Join(",", changed)}]");
                    reconciliation.ModifiedBy = _currentUser;
                    reconciliation.LastModified = DateTime.UtcNow;

                    // Build dynamic UPDATE statement
                    var setClauses = new System.Collections.Generic.List<string>();
                    foreach (var col in changed)
                    {
                        setClauses.Add($"[{col}] = ?");
                    }
                    // Always update metadata
                    setClauses.Add("[ModifiedBy] = ?");
                    setClauses.Add("[LastModified] = ?");
                    var updateQuery = $"UPDATE T_Reconciliation SET {string.Join(", ", setClauses)} WHERE [ID] = ?";

                    using (var cmd = new OleDbCommand(updateQuery, connection, transaction))
                    {
                        // Add parameters in the same order as placeholders
                        foreach (var col in changed)
                        {
                            switch (col)
                            {
                                case "DWINGS_GuaranteeID":
                                    cmd.Parameters.AddWithValue("@DWINGS_GuaranteeID", reconciliation.DWINGS_GuaranteeID ?? (object)DBNull.Value);
                                    break;
                                case "DWINGS_InvoiceID":
                                    cmd.Parameters.AddWithValue("@DWINGS_InvoiceID", reconciliation.DWINGS_InvoiceID ?? (object)DBNull.Value);
                                    break;
                                case "DWINGS_CommissionID":
                                    cmd.Parameters.AddWithValue("@DWINGS_CommissionID", reconciliation.DWINGS_CommissionID ?? (object)DBNull.Value);
                                    break;
                                case "Action":
                                    cmd.Parameters.AddWithValue("@Action", reconciliation.Action ?? (object)DBNull.Value);
                                    break;
                                case "Assignee":
                                    cmd.Parameters.AddWithValue("@Assignee", string.IsNullOrWhiteSpace(reconciliation.Assignee) ? (object)DBNull.Value : reconciliation.Assignee);
                                    break;
                                case "Comments":
                                    cmd.Parameters.AddWithValue("@Comments", reconciliation.Comments ?? (object)DBNull.Value);
                                    break;
                                case "InternalInvoiceReference":
                                    cmd.Parameters.AddWithValue("@InternalInvoiceReference", reconciliation.InternalInvoiceReference ?? (object)DBNull.Value);
                                    break;
                                case "FirstClaimDate":
                                    {
                                        var p = cmd.Parameters.Add("@FirstClaimDate", OleDbType.Date);
                                        p.Value = reconciliation.FirstClaimDate.HasValue ? (object)reconciliation.FirstClaimDate.Value : DBNull.Value;
                                        break;
                                    }
                                case "LastClaimDate":
                                    {
                                        var p = cmd.Parameters.Add("@LastClaimDate", OleDbType.Date);
                                        p.Value = reconciliation.LastClaimDate.HasValue ? (object)reconciliation.LastClaimDate.Value : DBNull.Value;
                                        break;
                                    }
                                case "ToRemind":
                                    {
                                        var p = cmd.Parameters.Add("@ToRemind", OleDbType.Boolean);
                                        p.Value = reconciliation.ToRemind;
                                        break;
                                    }
                                case "ToRemindDate":
                                    {
                                        var p = cmd.Parameters.Add("@ToRemindDate", OleDbType.Date);
                                        p.Value = reconciliation.ToRemindDate.HasValue ? (object)reconciliation.ToRemindDate.Value : DBNull.Value;
                                        break;
                                    }
                                case "ACK":
                                    {
                                        var p = cmd.Parameters.Add("@ACK", OleDbType.Boolean);
                                        p.Value = reconciliation.ACK;
                                        break;
                                    }
                                case "SwiftCode":
                                    cmd.Parameters.AddWithValue("@SwiftCode", reconciliation.SwiftCode ?? (object)DBNull.Value);
                                    break;
                                case "PaymentReference":
                                    cmd.Parameters.AddWithValue("@PaymentReference", reconciliation.PaymentReference ?? (object)DBNull.Value);
                                    break;
                                case "MbawData":
                                    cmd.Parameters.AddWithValue("@MbawData", reconciliation.MbawData ?? (object)DBNull.Value);
                                    break;
                                case "SpiritData":
                                    cmd.Parameters.AddWithValue("@SpiritData", reconciliation.SpiritData ?? (object)DBNull.Value);
                                    break;
                                case "KPI":
                                    {
                                        var p = cmd.Parameters.Add("@KPI", OleDbType.Integer);
                                        p.Value = reconciliation.KPI.HasValue ? (object)reconciliation.KPI.Value : DBNull.Value;
                                        break;
                                    }
                                case "IncidentType":
                                    {
                                        var p = cmd.Parameters.Add("@IncidentType", OleDbType.Integer);
                                        p.Value = reconciliation.IncidentType.HasValue ? (object)reconciliation.IncidentType.Value : DBNull.Value;
                                        break;
                                    }
                                case "RiskyItem":
                                    {
                                        var p = cmd.Parameters.Add("@RiskyItem", OleDbType.Boolean);
                                        p.Value = reconciliation.RiskyItem.HasValue ? (object)reconciliation.RiskyItem.Value : DBNull.Value;
                                        break;
                                    }
                                case "ReasonNonRisky":
                                    {
                                        var p = cmd.Parameters.Add("@ReasonNonRisky", OleDbType.Integer);
                                        p.Value = reconciliation.ReasonNonRisky.HasValue ? (object)reconciliation.ReasonNonRisky.Value : DBNull.Value;
                                        break;
                                    }
                            }
                        }

                        // Metadata
                        cmd.Parameters.AddWithValue("@ModifiedBy", reconciliation.ModifiedBy ?? (object)DBNull.Value);
                        var pMod = cmd.Parameters.Add("@LastModified", OleDbType.Date);
                        pMod.Value = reconciliation.LastModified.HasValue ? (object)reconciliation.LastModified.Value : DBNull.Value;

                        // WHERE ID
                        cmd.Parameters.AddWithValue("@ID", reconciliation.ID);

                        // Debug SQL and parameters
                        try
                        {
                            var paramDbg = string.Join(" | ", cmd.Parameters
                                .Cast<OleDbParameter>()
                                .Select(p =>
                                {
                                    var val = p.Value;
                                    string display = val == null || val is DBNull ? "NULL" : (val is byte[] b ? $"byte[{b.Length}]" : val.ToString());
                                    return $"{p.ParameterName} type={p.OleDbType} value={display}";
                                }));
                            LogManager.Debug($"Reconciliation UPDATE SQL: {updateQuery} | Params: {paramDbg}");
                        }
                        catch { }

                            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                        // Encode changed fields for partial update during sync
                        var op = $"UPDATE({string.Join(",", changed)})";
                        LogManager.Debug($"Reconciliation UPDATE operation encoded: {op}");
                        return op;
                    }
                }
                else
                {
                    // Prepare metadata for insert
                    if (!reconciliation.CreationDate.HasValue)
                        reconciliation.CreationDate = DateTime.UtcNow;
                    reconciliation.ModifiedBy = _currentUser;
                    reconciliation.LastModified = DateTime.UtcNow;

                    var insertQuery = @"INSERT INTO T_Reconciliation 
                             ([ID], [DWINGS_GuaranteeID], [DWINGS_InvoiceID], [DWINGS_CommissionID],
                              [Action], [Assignee], [Comments], [InternalInvoiceReference], [FirstClaimDate], [LastClaimDate],
                              [ToRemind], [ToRemindDate], [ACK], [SwiftCode], [PaymentReference], [MbawData], [SpiritData], [KPI],
                              [IncidentType], [RiskyItem], [ReasonNonRisky], [CreationDate], [ModifiedBy], [LastModified])
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

                    using (var cmd = new OleDbCommand(insertQuery, connection, transaction))
                    {
                        AddReconciliationParameters(cmd, reconciliation, isInsert: true);
                        LogManager.Debug($"Reconciliation INSERT: ID={reconciliation.ID}");
                            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                        return "INSERT";
                    }
                }
            }
        }

        /// <summary>
        /// Ajoute les paramètres pour les requêtes de réconciliation
        /// </summary>
        private void AddReconciliationParameters(OleDbCommand cmd, Reconciliation reconciliation, bool isInsert)
        {
            if (isInsert)
            {
                // ID as stable key
                cmd.Parameters.AddWithValue("@ID", reconciliation.ID ?? (object)DBNull.Value);
            }

            cmd.Parameters.AddWithValue("@DWINGS_GuaranteeID", reconciliation.DWINGS_GuaranteeID ?? (object)DBNull.Value);
            cmd.Parameters.AddWithValue("@DWINGS_InvoiceID", reconciliation.DWINGS_InvoiceID ?? (object)DBNull.Value);
            cmd.Parameters.AddWithValue("@DWINGS_CommissionID", reconciliation.DWINGS_CommissionID ?? (object)DBNull.Value);
            cmd.Parameters.AddWithValue("@Action", reconciliation.Action ?? (object)DBNull.Value);
            cmd.Parameters.AddWithValue("@Assignee", string.IsNullOrWhiteSpace(reconciliation.Assignee) ? (object)DBNull.Value : reconciliation.Assignee);
            cmd.Parameters.AddWithValue("@Comments", reconciliation.Comments ?? (object)DBNull.Value);
            cmd.Parameters.AddWithValue("@InternalInvoiceReference", reconciliation.InternalInvoiceReference ?? (object)DBNull.Value);
            var pFirst = cmd.Parameters.Add("@FirstClaimDate", OleDbType.Date);
            pFirst.Value = reconciliation.FirstClaimDate.HasValue ? (object)reconciliation.FirstClaimDate.Value : DBNull.Value;
            var pLast = cmd.Parameters.Add("@LastClaimDate", OleDbType.Date);
            pLast.Value = reconciliation.LastClaimDate.HasValue ? (object)reconciliation.LastClaimDate.Value : DBNull.Value;
            var pToRemind = cmd.Parameters.Add("@ToRemind", OleDbType.Boolean);
            pToRemind.Value = reconciliation.ToRemind;
            var pRem = cmd.Parameters.Add("@ToRemindDate", OleDbType.Date);
            pRem.Value = reconciliation.ToRemindDate.HasValue ? (object)reconciliation.ToRemindDate.Value : DBNull.Value;
            var pAck = cmd.Parameters.Add("@ACK", OleDbType.Boolean);
            pAck.Value = reconciliation.ACK;
            cmd.Parameters.AddWithValue("@SwiftCode", reconciliation.SwiftCode ?? (object)DBNull.Value);
            cmd.Parameters.AddWithValue("@PaymentReference", reconciliation.PaymentReference ?? (object)DBNull.Value);
            cmd.Parameters.AddWithValue("@MbawData", reconciliation.MbawData ?? (object)DBNull.Value);
            cmd.Parameters.AddWithValue("@SpiritData", reconciliation.SpiritData ?? (object)DBNull.Value);
            var pKpi = cmd.Parameters.Add("@KPI", OleDbType.Integer);
            pKpi.Value = reconciliation.KPI.HasValue ? (object)reconciliation.KPI.Value : DBNull.Value;
            var pInc = cmd.Parameters.Add("@IncidentType", OleDbType.Integer);
            pInc.Value = reconciliation.IncidentType.HasValue ? (object)reconciliation.IncidentType.Value : DBNull.Value;
            var pRisky = cmd.Parameters.Add("@RiskyItem", OleDbType.Boolean);
            pRisky.Value = reconciliation.RiskyItem.HasValue ? (object)reconciliation.RiskyItem.Value : DBNull.Value;
            var pReason = cmd.Parameters.Add("@ReasonNonRisky", OleDbType.Integer);
            pReason.Value = reconciliation.ReasonNonRisky.HasValue ? (object)reconciliation.ReasonNonRisky.Value : DBNull.Value;

            if (isInsert)
            {
                var pCreate = cmd.Parameters.Add("@CreationDate", OleDbType.Date);
                pCreate.Value = reconciliation.CreationDate.HasValue ? (object)reconciliation.CreationDate.Value : DBNull.Value;
            }

            cmd.Parameters.AddWithValue("@ModifiedBy", reconciliation.ModifiedBy ?? (object)DBNull.Value);
            var pMod = cmd.Parameters.Add("@LastModified", OleDbType.Date);
            pMod.Value = reconciliation.LastModified.HasValue ? (object)reconciliation.LastModified.Value : DBNull.Value;

            if (!isInsert)
                cmd.Parameters.AddWithValue("@ID", reconciliation.ID);
        }

        /// <summary>
        /// Récupère la liste des utilisateurs (référentiel T_User)
        /// </summary>
        public async Task<List<(string Id, string Name)>> GetUsersAsync()
        {
            var list = new List<(string, string)>();
            using (var connection = new OleDbConnection(GetReferentialConnectionString()))
            {
                await connection.OpenAsync();

                // Ensure current user exists in T_User (USR_ID, USR_Name)
                try
                {
                    if (!string.IsNullOrWhiteSpace(_currentUser))
                    {
                        var checkCmd = new OleDbCommand("SELECT COUNT(*) FROM T_User WHERE USR_ID = ?", connection);
                        checkCmd.Parameters.AddWithValue("@p1", _currentUser);
                        var obj = await checkCmd.ExecuteScalarAsync().ConfigureAwait(false);
                        var exists = obj != null && int.TryParse(obj.ToString(), out var n) && n > 0;
                        if (!exists)
                        {
                            var insertCmd = new OleDbCommand("INSERT INTO T_User (USR_ID, USR_Name) VALUES (?, ?)", connection);
                            insertCmd.Parameters.AddWithValue("@p1", _currentUser);
                            insertCmd.Parameters.AddWithValue("@p2", _currentUser);
                            await insertCmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                        }
                    }
                }
                catch { /* best effort; not critical */ }

                var cmd = new OleDbCommand("SELECT USR_ID, USR_Name FROM T_User ORDER BY USR_Name", connection);
                using (var rdr = await cmd.ExecuteReaderAsync())
                {
                    while (await rdr.ReadAsync())
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

        #endregion

        #region KPI Daily Snapshots

        private class KpiDailySnapshotDto
        {
            public DateTime SnapshotDate { get; set; }
            public string CountryId { get; set; }

            public long MissingInvoices { get; set; }
            public long PaidNotReconciled { get; set; }
            public long UnderInvestigation { get; set; }

            public long ReceivableCount { get; set; }
            public decimal ReceivableAmount { get; set; }
            public long PivotCount { get; set; }
            public decimal PivotAmount { get; set; }

            public long NewCount { get; set; }
            public long DeletedCount { get; set; }

            public string DeletionDelayBucketsJson { get; set; }
            public string ReceivablePivotByActionJson { get; set; }
            public string KpiDistributionJson { get; set; }
            public string KpiRiskMatrixJson { get; set; }
            public string CurrencyDistributionJson { get; set; }
            public string ActionDistributionJson { get; set; }

            public DateTime CreatedAtUtc { get; set; }
            public string SourceVersion { get; set; }
        }

        /// <summary>
        /// Compute and save a daily snapshot of HomePage KPIs for the given country and date (insert-only).
        /// </summary>
        public async Task<bool> SaveDailyKpiSnapshotAsync(DateTime date, string countryId, string sourceVersion = null, CancellationToken cancellationToken = default)
        {
            var dto = await BuildKpiDailySnapshotAsync(date.Date, countryId, sourceVersion, cancellationToken);
            return await InsertDailyKpiSnapshotAsync(dto, cancellationToken);
        }

        private async Task<KpiDailySnapshotDto> BuildKpiDailySnapshotAsync(DateTime date, string countryId, string sourceVersion, CancellationToken ct)
        {
            // Load the same data used by HomePage
            var list = await GetReconciliationViewAsync(countryId);

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
                new { Key = ">3m",   Min = 93, Max = int.MaxValue }
            };
            var delayBucketObjs = buckets.Select(b =>
            {
                var inB = durations.Where(d => d >= b.Min && d <= b.Max).ToList();
                return new { bucket = b.Key, avgDays = inB.Any() ? inB.Average() : 0.0, count = inB.Count };
            }).ToList();
            dto.DeletionDelayBucketsJson = JsonSerializer.Serialize(delayBucketObjs);

            // Receivable vs Pivot by Action
            var actionGroups = list.Where(r => r.Action.HasValue).GroupBy(r => r.Action.Value).OrderBy(g => g.Key).ToList();
            var labels = actionGroups.Select(g => EnumHelper.GetActionName(g.Key, _offlineFirstService?.UserFields)).ToList();
            var recvVals = actionGroups.Select(g => g.Count(x => x.Account_ID == receivableId)).ToList();
            var pivVals = actionGroups.Select(g => g.Count(x => x.Account_ID == pivotId)).ToList();
            dto.ReceivablePivotByActionJson = JsonSerializer.Serialize(new { labels, receivable = recvVals, pivot = pivVals });

            // KPI Distribution
            var kpiDist = list.Where(r => r.KPI.HasValue)
                              .GroupBy(r => r.KPI.Value)
                              .Select(g => new { kpi = EnumHelper.GetKPIName(g.Key, _offlineFirstService?.UserFields), count = g.Count() })
                              .OrderByDescending(x => x.count)
                              .ToList();
            dto.KpiDistributionJson = JsonSerializer.Serialize(kpiDist);

            // KPI × RiskyItem (Risky vs Non-Risky counts per KPI)
            var kpiRisk = list.Where(r => r.KPI.HasValue)
                              .GroupBy(r => r.KPI.Value)
                              .OrderBy(g => g.Key)
                              .ToList();
            var kpiLabels = kpiRisk.Select(g => EnumHelper.GetKPIName(g.Key, _offlineFirstService?.UserFields)).ToList();
            var risky = kpiRisk.Select(g => g.Count(x => x.RiskyItem == true)).ToList();
            var nonRisky = kpiRisk.Select(g => g.Count(x => x.RiskyItem != true)).ToList();
            dto.KpiRiskMatrixJson = JsonSerializer.Serialize(new { kpiLabels, series = new[] { "Risky", "Non-Risky" }, values = new[] { risky, nonRisky } });

            // Currency Distribution (top 10 by amount)
            var ccy = list.Where(r => !string.IsNullOrEmpty(r.CCY) && r.SignedAmount != 0)
                          .GroupBy(r => r.CCY)
                          .Select(g => new { currency = g.Key, amount = Math.Abs(g.Sum(x => x.SignedAmount)), count = g.Count() })
                          .OrderByDescending(x => x.amount)
                          .Take(10)
                          .ToList();
            dto.CurrencyDistributionJson = JsonSerializer.Serialize(ccy);

            // Action Distribution (counts per action)
            var act = list.Where(r => r.Action.HasValue)
                          .GroupBy(r => r.Action.Value)
                          .Select(g => new { action = EnumHelper.GetActionName(g.Key, _offlineFirstService?.UserFields), count = g.Count() })
                          .OrderByDescending(x => x.count)
                          .ToList();
            dto.ActionDistributionJson = JsonSerializer.Serialize(act);

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
                foreach (System.Data.DataRow row in schema.Rows)
                {
                    var name = Convert.ToString(row["COLUMN_NAME"]);
                    if (string.IsNullOrEmpty(name)) continue;
                    var dataTypeCode = Convert.ToInt32(row["DATA_TYPE"]); // OLE DB data type code
                    existing[name] = dataTypeCode;
                }

                // Helper factories to map values to column type
                (OleDbType Type, object Value) MapValue(string col, object val)
                {
                    if (!existing.TryGetValue(col, out var code))
                    {
                        // Fallback guesses
                        switch (col)
                        {
                            case "SnapshotDate":
                            case "CreatedAtUtc":
                                return (OleDbType.Date, (object)val ?? DBNull.Value);
                            case "CountryId":
                                return (OleDbType.VarWChar, (object)(val ?? string.Empty));
                            default:
                                return (OleDbType.VarWChar, (object)(val ?? DBNull.Value));
                        }
                    }

                    // OLE DB type codes of interest for Access:
                    // 3=Integer, 4=Single, 5=Double, 6=Currency, 7=Date, 130=VarWChar, 202=VarWChar, 203=LongVarWChar, 131=Decimal, 20=BigInt
                    switch (code)
                    {
                        case 7: // Date/Time
                            return (OleDbType.Date, (object)val ?? DBNull.Value);
                        case 5: // Double
                            if (val is DateTime dtD) return (OleDbType.Double, dtD.ToOADate());
                            return (OleDbType.Double, val ?? DBNull.Value);
                        case 6: // Currency
                            return (OleDbType.Currency, val ?? DBNull.Value);
                        case 3: // Integer (Long Integer)
                            // Convert to int32 where needed
                            return (OleDbType.Integer, val == null ? DBNull.Value : (object)Convert.ToInt32(val));
                        case 20: // BigInt
                            return (OleDbType.BigInt, val ?? DBNull.Value);
                        case 131: // Decimal
                            return (OleDbType.Decimal, val ?? DBNull.Value);
                        case 203: // Long Text (Memo)
                            return (OleDbType.LongVarWChar, val ?? DBNull.Value);
                        case 202: // Text (VarWChar)
                        case 130: // WChar
                            return (OleDbType.VarWChar, (object)(val ?? string.Empty));
                        default:
                            // Safe default to VarWChar as text
                            return (OleDbType.VarWChar, (object)(val ?? string.Empty));
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
                    ("ReceivableCount", dto.ReceivableCount),
                    ("ReceivableAmount", dto.ReceivableAmount),
                    ("PivotCount", dto.PivotCount),
                    ("PivotAmount", dto.PivotAmount),
                    ("NewCount", dto.NewCount),
                    ("DeletedCount", dto.DeletedCount),
                    ("DeletionDelayBucketsJson", (object)dto.DeletionDelayBucketsJson ?? DBNull.Value),
                    ("ReceivablePivotByActionJson", (object)dto.ReceivablePivotByActionJson ?? DBNull.Value),
                    ("KpiDistributionJson", (object)dto.KpiDistributionJson ?? DBNull.Value),
                    ("KpiRiskMatrixJson", (object)dto.KpiRiskMatrixJson ?? DBNull.Value),
                    ("CurrencyDistributionJson", (object)dto.CurrencyDistributionJson ?? DBNull.Value),
                    ("ActionDistributionJson", (object)dto.ActionDistributionJson ?? DBNull.Value),
                    ("CreatedAtUtc", dto.CreatedAtUtc),
                    ("SourceVersion", (object)dto.SourceVersion ?? DBNull.Value)
                };

                var used = new List<(string Name, OleDbType Type, object Value)>();
                foreach (var r in raw)
                {
                    if (!existing.ContainsKey(r.Name)) continue;
                    var mapped = MapValue(r.Name, r.Val);
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
                        insert.Parameters.Add(new OleDbParameter { OleDbType = u.Type, Value = u.Value ?? DBNull.Value });
                    }
                    await insert.ExecuteNonQueryAsync(cancellationToken);
                }
                return true;
            }
        }

        /// <summary>
        /// Mark the latest non-frozen snapshot for a country as frozen by setting FrozenAt.
        /// </summary>
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
                int id;
                if (!int.TryParse(Convert.ToString(obj), out id)) return false;

                var upd = new OleDbCommand("UPDATE KpiDailySnapshot SET FrozenAt = ? WHERE Id = ?", connection);
                upd.Parameters.AddWithValue("@p1", DateTime.UtcNow.ToOADate());
                upd.Parameters.AddWithValue("@p2", id);
                var n = await upd.ExecuteNonQueryAsync(cancellationToken);
                return n > 0;
            }
        }

        /// <summary>
        /// Get one snapshot by date and country. Returns JSON strings as stored.
        /// </summary>
        public async Task<DataTable> GetKpiSnapshotAsync(DateTime date, string countryId, CancellationToken cancellationToken = default)
        {
            using (var connection = new OleDbConnection(GetControlConnectionString()))
            {
                await connection.OpenAsync(cancellationToken);
                await EnsureKpiDailySnapshotTableAsync(connection, cancellationToken);
                var cmd = new OleDbCommand("SELECT TOP 1 * FROM KpiDailySnapshot WHERE SnapshotDate = ? AND CountryId = ? ORDER BY CreatedAtUtc DESC", connection);
                cmd.Parameters.AddWithValue("@p1", date.Date);
                cmd.Parameters.AddWithValue("@p2", countryId ?? string.Empty);
                using (var adapter = new OleDbDataAdapter(cmd))
                {
                    var table = new DataTable();
                    adapter.Fill(table);
                    return table;
                }
            }
        }

        /// <summary>
        /// Get snapshots in a date range for a country (inclusive).
        /// </summary>
        public async Task<DataTable> GetKpiSnapshotsAsync(DateTime from, DateTime to, string countryId, CancellationToken cancellationToken = default)
        {
            using (var connection = new OleDbConnection(GetControlConnectionString()))
            {
                await connection.OpenAsync(cancellationToken);
                await EnsureKpiDailySnapshotTableAsync(connection, cancellationToken);
                var cmd = new OleDbCommand("SELECT * FROM KpiDailySnapshot WHERE CountryId = ? AND SnapshotDate BETWEEN ? AND ? ORDER BY SnapshotDate", connection);
                cmd.Parameters.AddWithValue("@p1", countryId ?? string.Empty);
                cmd.Parameters.AddWithValue("@p2", from.Date);
                cmd.Parameters.AddWithValue("@p3", to.Date);
                using (var adapter = new OleDbDataAdapter(cmd))
                {
                    var table = new DataTable();
                    adapter.Fill(table);
                    return table;
                }
            }
        }

        #endregion

        #region User Fields Preferences (Saved Views)

        /// <summary>
        /// Returns all saved views from T_Ref_User_Fields_Preference (globally visible)
        /// </summary>
        public async Task<List<UserFieldsPreference>> GetUserFieldsPreferencesAsync()
        {
            var query = @"SELECT UPF_id, UPF_Name, UPF_user, UPF_SQL, UPF_ColumnWidths FROM T_Ref_User_Fields_Preference ORDER BY UPF_Name";
            return await ExecuteQueryAsync<UserFieldsPreference>(query, GetReferentialConnectionString());
        }

        /// <summary>
        /// Inserts a new preference row
        /// </summary>
        public async Task<int> InsertUserFieldsPreferenceAsync(string name, string sql, string columnsJson)
        {
            using (var connection = new OleDbConnection(GetReferentialConnectionString()))
            {
                await connection.OpenAsync();
                var cmd = new OleDbCommand(@"INSERT INTO T_Ref_User_Fields_Preference (UPF_Name, UPF_user, UPF_SQL, UPF_ColumnWidths) VALUES (?, ?, ?, ?)", connection);
                cmd.Parameters.AddWithValue("@p1", name ?? (object)DBNull.Value);
                cmd.Parameters.AddWithValue("@p2", _currentUser ?? (object)DBNull.Value);
                cmd.Parameters.AddWithValue("@p3", sql ?? (object)DBNull.Value);
                cmd.Parameters.AddWithValue("@p4", columnsJson ?? (object)DBNull.Value);
                            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                // Retrieve autonumber (Access specific)
                var idCmd = new OleDbCommand("SELECT @@IDENTITY", connection);
                var obj = await idCmd.ExecuteScalarAsync();
                return obj != null && int.TryParse(obj.ToString(), out var id) ? id : 0;
            }
        }

        /// <summary>
        /// Updates an existing preference row
        /// </summary>
        public async Task<bool> UpdateUserFieldsPreferenceAsync(int id, string name, string sql, string columnsJson)
        {
            using (var connection = new OleDbConnection(GetReferentialConnectionString()))
            {
                await connection.OpenAsync();
                var cmd = new OleDbCommand(@"UPDATE T_Ref_User_Fields_Preference SET UPF_Name = ?, UPF_user = ?, UPF_SQL = ?, UPF_ColumnWidths = ? WHERE UPF_id = ?", connection);
                cmd.Parameters.AddWithValue("@p1", name ?? (object)DBNull.Value);
                cmd.Parameters.AddWithValue("@p2", _currentUser ?? (object)DBNull.Value);
                cmd.Parameters.AddWithValue("@p3", sql ?? (object)DBNull.Value);
                cmd.Parameters.AddWithValue("@p4", columnsJson ?? (object)DBNull.Value);
                cmd.Parameters.AddWithValue("@p5", id);
                var n = await cmd.ExecuteNonQueryAsync();
                return n > 0;
            }
        }

        /// <summary>
        /// Upsert by (name, user). If a row exists for current user and name, update it; else insert new.
        /// Returns the UPF_id.
        /// </summary>
        public async Task<int> UpsertUserFieldsPreferenceAsync(string name, string sql, string columnsJson)
        {
            using (var connection = new OleDbConnection(GetReferentialConnectionString()))
            {
                await connection.OpenAsync();
                int? existingId = null;
                var check = new OleDbCommand(@"SELECT TOP 1 UPF_id FROM T_Ref_User_Fields_Preference WHERE UPF_Name = ? AND UPF_user = ?", connection);
                check.Parameters.AddWithValue("@p1", name ?? (object)DBNull.Value);
                check.Parameters.AddWithValue("@p2", _currentUser ?? (object)DBNull.Value);
                var obj = await check.ExecuteScalarAsync();
                if (obj != null && int.TryParse(obj.ToString(), out var idFound)) existingId = idFound;

                if (existingId.HasValue)
                {
                    await UpdateUserFieldsPreferenceAsync(existingId.Value, name, sql, columnsJson);
                    return existingId.Value;
                }
                else
                {
                    return await InsertUserFieldsPreferenceAsync(name, sql, columnsJson);
                }
            }
        }

        /// <summary>
        /// Get a single saved view for current user by name, or null if not found.
        /// </summary>
        public async Task<UserFieldsPreference> GetUserFieldsPreferenceByNameAsync(string name)
        {
            var query = @"SELECT TOP 1 UPF_id, UPF_Name, UPF_user, UPF_SQL, UPF_ColumnWidths
                           FROM T_Ref_User_Fields_Preference
                           WHERE UPF_Name = ? AND UPF_user = ?";
            var list = await ExecuteQueryAsync<UserFieldsPreference>(query, GetReferentialConnectionString(), name, _currentUser);
            return list.FirstOrDefault();
        }

        /// <summary>
        /// List saved view names for current user, optionally filtered by a substring (case-insensitive).
        /// </summary>
        public async Task<List<string>> ListUserFieldsPreferenceNamesAsync(string contains = null)
        {
            string baseQuery = @"SELECT DISTINCT UPF_Name FROM T_Ref_User_Fields_Preference WHERE UPF_user = ?";
            List<string> result;
            if (string.IsNullOrWhiteSpace(contains))
            {
                var rows = await ExecuteScalarListAsync<string>(baseQuery + " ORDER BY UPF_Name ASC", GetReferentialConnectionString(), _currentUser);
                result = rows?.ToList() ?? new List<string>();
            }
            else
            {
                // Use LIKE for filtering in DB
                var rows = await ExecuteScalarListAsync<string>(baseQuery + " AND UPF_Name LIKE ? ORDER BY UPF_Name ASC", GetReferentialConnectionString(), _currentUser, "%" + contains + "%");
                result = rows?.ToList() ?? new List<string>();
            }
            return result;
        }

        /// <summary>
        /// List saved views (Name, Creator) for current user, optionally filtered by a substring.
        /// </summary>
        public async Task<List<(string Name, string Creator)>> ListUserFieldsPreferenceDetailedAsync(string contains = null)
        {
            var list = new List<(string, string)>();
            using (var connection = new OleDbConnection(GetReferentialConnectionString()))
            {
                await connection.OpenAsync();
                OleDbCommand cmd;
                if (string.IsNullOrWhiteSpace(contains))
                {
                    cmd = new OleDbCommand(@"SELECT DISTINCT UPF_Name, UPF_user FROM T_Ref_User_Fields_Preference WHERE UPF_user = ? ORDER BY UPF_Name ASC", connection);
                    cmd.Parameters.AddWithValue("@p1", _currentUser ?? (object)DBNull.Value);
                }
                else
                {
                    cmd = new OleDbCommand(@"SELECT DISTINCT UPF_Name, UPF_user FROM T_Ref_User_Fields_Preference WHERE UPF_user = ? AND UPF_Name LIKE ? ORDER BY UPF_Name ASC", connection);
                    cmd.Parameters.AddWithValue("@p1", _currentUser ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@p2", "%" + contains + "%");
                }

                using (var rdr = await cmd.ExecuteReaderAsync())
                {
                    while (await rdr.ReadAsync())
                    {
                        var name = rdr.IsDBNull(0) ? string.Empty : rdr.GetString(0);
                        var creator = rdr.IsDBNull(1) ? string.Empty : rdr.GetString(1);
                        list.Add((name, creator));
                    }
                }
            }
            return list;
        }

        /// <summary>
        /// Deletes saved view(s) for current user by name.
        /// Returns true if at least one row deleted.
        /// </summary>
        public async Task<bool> DeleteUserFieldsPreferenceByNameAsync(string name)
        {
            using (var connection = new OleDbConnection(GetReferentialConnectionString()))
            {
                await connection.OpenAsync();
                var cmd = new OleDbCommand(@"DELETE FROM T_Ref_User_Fields_Preference WHERE UPF_Name = ? AND UPF_user = ?", connection);
                cmd.Parameters.AddWithValue("@p1", name ?? (object)DBNull.Value);
                cmd.Parameters.AddWithValue("@p2", _currentUser ?? (object)DBNull.Value);
                var n = await cmd.ExecuteNonQueryAsync();
                return n > 0;
            }
        }

        #endregion

        #region Helper Methods

        /// <summary>
        /// Execute a query using the main data connection string.
        /// </summary>
        private async Task<List<T>> ExecuteQueryAsync<T>(string query, params object[] parameters) where T : new()
        {
            return await ExecuteQueryAsync<T>(query, _connectionString, parameters).ConfigureAwait(false);
        }

        /// <summary>
        /// Execute a query using a specific connection string (override).
        /// </summary>
        private async Task<List<T>> ExecuteQueryAsync<T>(string query, string connectionString, params object[] parameters) where T : new()
        {
            var results = new List<T>();
            var swTotal = Stopwatch.StartNew();
            long msOpen = 0, msExecute = 0, msMapPrep = 0, msMapRows = 0;
            int fieldCount = 0, propMapCount = 0;

            using (var connection = new OleDbConnection(connectionString))
            {
                var swOpen = Stopwatch.StartNew();
                await connection.OpenAsync();
                swOpen.Stop();
                msOpen = swOpen.ElapsedMilliseconds;
                using (var command = new OleDbCommand(query, connection))
                {
                    if (parameters != null)
                    {
                        for (int i = 0; i < parameters.Length; i++)
                        {
                            command.Parameters.AddWithValue($"@param{i}", parameters[i] ?? DBNull.Value);
                        }
                    }

                    var swExec = Stopwatch.StartNew();
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        swExec.Stop();
                        msExecute = swExec.ElapsedMilliseconds;
                        // Cache column ordinals once per reader
                        var swPrep = Stopwatch.StartNew();
                        var columnOrdinals = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            var name = reader.GetName(i);
                            if (!columnOrdinals.ContainsKey(name))
                                columnOrdinals.Add(name, i);
                        }
                        fieldCount = reader.FieldCount;

                        // Prepare property maps once per type/query execution
                        var properties = typeof(T).GetProperties();
                        var propMaps = new List<(System.Reflection.PropertyInfo Prop, int Ordinal, Type TargetType)>();
                        foreach (var prop in properties)
                        {
                            try
                            {
                                if (!prop.CanWrite || prop.GetSetMethod(true) == null || prop.GetIndexParameters().Length > 0)
                                    continue;

                                if (!columnOrdinals.TryGetValue(prop.Name, out var ord))
                                    continue;

                                var targetType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
                                propMaps.Add((prop, ord, targetType));
                            }
                            catch
                            {
                                // Ignore property mapping preparation issues
                            }
                        }
                        propMapCount = propMaps.Count;
                        swPrep.Stop();
                        msMapPrep = swPrep.ElapsedMilliseconds;

                        var swMap = Stopwatch.StartNew();
                        while (await reader.ReadAsync())
                        {
                            var item = new T();

                            foreach (var map in propMaps)
                            {
                                try
                                {
                                    if (reader.IsDBNull(map.Ordinal))
                                        continue;

                                    var value = reader.GetValue(map.Ordinal);
                                    if (value == DBNull.Value)
                                        continue;

                                    var targetType = map.TargetType;
                                    object converted = null;

                                    if (targetType == typeof(DateTime))
                                    {
                                        if (value is DateTime dt)
                                        {
                                            converted = dt;
                                        }
                                        else if (value is double d)
                                        {
                                            converted = DateTime.FromOADate(d);
                                        }
                                        else if (value is float f)
                                        {
                                            converted = DateTime.FromOADate(f);
                                        }
                                        else if (value is decimal m)
                                        {
                                            converted = DateTime.FromOADate((double)m);
                                        }
                                        else
                                        {
                                            var sVal = Convert.ToString(value);
                                            if (!string.IsNullOrWhiteSpace(sVal))
                                            {
                                                // Try OADate numeric string first
                                                if (double.TryParse(sVal, NumberStyles.Any, CultureInfo.InvariantCulture, out var d2))
                                                {
                                                    try { converted = DateTime.FromOADate(d2); }
                                                    catch { converted = null; }
                                                }

                                                // Try DWINGS textual date formats like 30-APR-22
                                                if (converted == null && TryParseDwingsDate(sVal, out var parsed))
                                                {
                                                    converted = parsed;
                                                }

                                                // Fallback to broad parsing
                                                if (converted == null)
                                                {
                                                    if (DateTime.TryParse(sVal, CultureInfo.InvariantCulture, DateTimeStyles.None, out var gen))
                                                        converted = gen;
                                                    else if (DateTime.TryParse(sVal, CultureInfo.GetCultureInfo("fr-FR"), DateTimeStyles.None, out gen))
                                                        converted = gen;
                                                    else if (DateTime.TryParse(sVal, CultureInfo.GetCultureInfo("it-IT"), DateTimeStyles.None, out gen))
                                                        converted = gen;
                                                }
                                            }
                                        }
                                    }
                                    else if (targetType == typeof(bool))
                                    {
                                        if (value is bool b)
                                            converted = b;
                                        else if (value is byte by)
                                            converted = by != 0;
                                        else if (value is short s)
                                            converted = s != 0;
                                        else if (value is int i)
                                            converted = i != 0;
                                        else
                                            converted = Convert.ToBoolean(value);
                                    }
                                    else if (targetType == typeof(decimal))
                                    {
                                        if (value is decimal dm)
                                            converted = dm;
                                        else if (value is double dd)
                                            converted = Convert.ToDecimal(dd);
                                        else if (value is float ff)
                                            converted = Convert.ToDecimal(ff);
                                        else
                                            converted = Convert.ChangeType(value, targetType);
                                    }
                                    else
                                    {
                                        converted = Convert.ChangeType(value, targetType);
                                    }

                                    if (converted != null)
                                        map.Prop.SetValue(item, converted);
                                }
                                catch
                                {
                                    // Ignore mapping issues per-property
                                }
                            }

                            // Normalize date-like strings for specific DTOs
                            if (typeof(T) == typeof(ReconciliationViewData))
                            {
                                try { NormalizeDwingsDateStrings(item as ReconciliationViewData); } catch { }
                            }

                            results.Add(item);
                        }
                        swMap.Stop();
                        msMapRows = swMap.ElapsedMilliseconds;
                    }
                }
            }

            swTotal.Stop();
            try
            {
                var dbTag = string.Equals(connectionString, _connectionString, StringComparison.OrdinalIgnoreCase) ? "Main" : "Alt";
            }
            catch { }

            return results;
        }

        /// <summary>
        /// Normalize DWINGS string date fields on ReconciliationViewData to yyyy-MM-dd if parseable.
        /// </summary>
        private static void NormalizeDwingsDateStrings(ReconciliationViewData dto)
        {
            if (dto == null) return;

            string Norm(string s)
            {
                if (string.IsNullOrWhiteSpace(s)) return s;
                if (TryParseDwingsDate(s, out var dt)) return dt.ToString("yyyy-MM-dd");
                if (DateTime.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.None, out dt)) return dt.ToString("yyyy-MM-dd");
                if (DateTime.TryParse(s, CultureInfo.GetCultureInfo("fr-FR"), DateTimeStyles.None, out dt)) return dt.ToString("yyyy-MM-dd");
                if (DateTime.TryParse(s, CultureInfo.GetCultureInfo("it-IT"), DateTimeStyles.None, out dt)) return dt.ToString("yyyy-MM-dd");
                return s;
            }

            // DWINGS Invoice date-like strings
            dto.I_START_DATE = Norm(dto.I_START_DATE);
            dto.I_END_DATE = Norm(dto.I_END_DATE);
            dto.I_REQUESTED_EXECUTION_DATE = Norm(dto.I_REQUESTED_EXECUTION_DATE);

            // DWINGS Guarantee date-like strings
            dto.G_EVENT_EFFECTIVEDATE = Norm(dto.G_EVENT_EFFECTIVEDATE);
            dto.G_ISSUEDATE = Norm(dto.G_ISSUEDATE);
            dto.G_EXPIRYDATE = Norm(dto.G_EXPIRYDATE);
            dto.G_CANCELLATIONDATE = Norm(dto.G_CANCELLATIONDATE);
        }

        /// <summary>
        /// Parse DWINGS textual date formats such as "30-APR-22" or "30-APR-2022" and other common variants.
        /// </summary>
        private static bool TryParseDwingsDate(string input, out DateTime dt)
        {
            dt = default;
            if (string.IsNullOrWhiteSpace(input)) return false;

            var s = input.Trim();
            // Replace multiple spaces and normalize dashes
            s = Regex.Replace(s, "[\u2013\u2014\\-]", "-");
            s = Regex.Replace(s, "\\s+", " ").Trim();

            // Try exact formats with English month abbreviations
            var formats = new[]
            {
                "dd-MMM-yy",
                "dd-MMM-yyyy",
                "d-MMM-yy",
                "d-MMM-yyyy",
                "dd/MM/yy",
                "dd/MM/yyyy",
                "d/M/yy",
                "d/M/yyyy",
                "yyyy-MM-dd",
                "dd.MM.yyyy",
                "d.MM.yyyy",
            };

            if (DateTime.TryParseExact(s, formats, CultureInfo.InvariantCulture, DateTimeStyles.None, out dt))
                return true;

            // Fallback generic parses
            if (DateTime.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.None, out dt)) return true;
            if (DateTime.TryParse(s, CultureInfo.GetCultureInfo("fr-FR"), DateTimeStyles.None, out dt)) return true;
            if (DateTime.TryParse(s, CultureInfo.GetCultureInfo("it-IT"), DateTimeStyles.None, out dt)) return true;

            // Try uppercasing month abbreviations to help parsing
            var up = s.ToUpperInvariant();
            if (!ReferenceEquals(up, s) && DateTime.TryParseExact(up, formats, CultureInfo.InvariantCulture, DateTimeStyles.None, out dt))
                return true;

            return false;
        }
    }
    #endregion
}
