using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.OleDb;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using RecoTool.Models;

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
        /// Execute a query that returns a single column list of values.
        /// </summary>
        private async Task<List<T>> ExecuteScalarListAsync<T>(string query, string connectionString, params object[] parameters)
        {
            var results = new List<T>();
            using (var connection = new OleDbConnection(connectionString))
            {
                await connection.OpenAsync();
                using (var command = new OleDbCommand(query, connection))
                {
                    if (parameters != null)
                    {
                        for (int i = 0; i < parameters.Length; i++)
                        {
                            command.Parameters.AddWithValue($"@param{i}", parameters[i] ?? DBNull.Value);
                        }
                    }

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
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
            // Base de données déjà spécifique au pays: ne pas filtrer par Country
            var query = @"SELECT * FROM T_Data_Ambre";
            if (!includeDeleted)
                query += " WHERE DeleteDate IS NULL";
            query += " ORDER BY Operation_Date DESC";

            return await ExecuteQueryAsync<DataAmbre>(query);
        }

        /// <summary>
        /// Récupère les données de réconciliation pour un pays
        /// </summary>
        public async Task<List<Reconciliation>> GetReconciliationDataAsync(string countryId, bool includeDeleted = false)
        {
            // Base de données déjà spécifique au pays: ne pas filtrer par Country
            var query = @"SELECT r.* FROM T_Reconciliation r 
                         INNER JOIN T_Data_Ambre a ON r.ID = a.ID";
            if (!includeDeleted)
                query += " WHERE r.DeleteDate IS NULL";
            query += " ORDER BY r.LastModified DESC";

            return await ExecuteQueryAsync<Reconciliation>(query);
        }

        /// <summary>
        /// Récupère les données jointes Ambre + Réconciliation
        /// </summary>
        public async Task<List<ReconciliationViewData>> GetReconciliationViewAsync(string countryId, string filterSql = null)
        {
            string dwPath = _offlineFirstService?.GetLocalDWDatabasePath();
            string dwEsc = string.IsNullOrEmpty(dwPath) ? null : dwPath.Replace("'", "''");
            // Build JOIN targets: use IN 'path' subqueries when external, otherwise direct tables
            string dwDataJoinInv = string.IsNullOrEmpty(dwEsc) ? "T_DW_Data AS dInv" : $"(SELECT * FROM [{dwEsc}].T_DW_Data) AS dInv";
            string dwDataJoinCom = string.IsNullOrEmpty(dwEsc) ? "T_DW_Data AS dCom" : $"(SELECT * FROM [{dwEsc}].T_DW_Data) AS dCom";
            string dwGuaranteeJoin = string.IsNullOrEmpty(dwEsc) ? "T_DW_Guarantee AS g" : $"(SELECT * FROM [{dwEsc}].T_DW_Guarantee) AS g";

            var query = $@"SELECT r.*, a.*, 
                                   r.DWINGS_GuaranteeID AS DW_GUARANTEE_ID,
                                   dInv.INVOICE_ID AS DW_INVOICE_ID,
                                   dCom.COMMISSION_ID AS DW_COMMISSION_ID,
                                   r.ModifiedBy AS Reco_ModifiedBy,
                                   g.SYNDICATE, g.AMOUNT AS GUARANTEE_AMOUNT, g.CURRENCY AS GUARANTEE_CURRENCY, g.STATUS AS GUARANTEE_STATUS
                           FROM (((T_Data_Ambre AS a 
                           LEFT JOIN T_Reconciliation AS r ON a.ID = r.ID)
                           LEFT JOIN {dwDataJoinInv} ON r.DWINGS_InvoiceID = dInv.INVOICE_ID)
                           LEFT JOIN {dwDataJoinCom} ON r.DWINGS_CommissionID = dCom.COMMISSION_ID)
                           LEFT JOIN {dwGuaranteeJoin} ON r.DWINGS_GuaranteeID = g.GUARANTEE_ID
                           WHERE a.DeleteDate IS NULL";

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
                if (!string.IsNullOrEmpty(cond))
                    query += $" AND ({cond})";
            }

            query += " ORDER BY a.Operation_Date DESC";

            return await ExecuteQueryAsync<ReconciliationViewData>(query);
        }

        #endregion

        #region Exports (T_param driven)

        /// <summary>
        /// Reads a SQL payload from referential table T_param.Par_Value using a flexible key lookup.
        /// Accepts keys like Export_KPI, Export_PastDUE, Export_IT.
        /// </summary>
        public async Task<string> GetParamValueAsync(string paramKey)
        {
            if (string.IsNullOrWhiteSpace(paramKey)) return null;

            var cs = GetReferentialConnectionString();
            using (var connection = new OleDbConnection(cs))
            {
                await connection.OpenAsync();

                // Try common key column names to avoid coupling to a specific schema naming
                string[] keyColumns = { "Par_Key", "Par_Code", "Par_Name", "PAR_Key", "PAR_Code", "PAR_Name" };
                foreach (var col in keyColumns)
                {
                    try
                    {
                        var cmd = new OleDbCommand($"SELECT TOP 1 Par_Value FROM T_param WHERE {col} = ?", connection);
                        cmd.Parameters.AddWithValue("@p1", paramKey);
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
        public async Task<DataTable> ExecuteExportAsync(string sql, IEnumerable<DbParameter> parameters)
        {
            if (string.IsNullOrWhiteSpace(sql)) throw new ArgumentNullException(nameof(sql));

            using (var connection = new OleDbConnection(_connectionString))
            {
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
            var receivableReco = await GetOrCreateReconciliationAsync(receivableLine.ID);
            
            // Marquer comme matché et ajouter commentaire
            receivableReco.Action = (int)ActionType.Match;
            receivableReco.KPI = (int)KPIType.PaidButNotReconciled;
            receivableReco.Comments = $"Auto-matched with {pivotLines.Count} pivot line(s)";

            foreach (var pivotLine in pivotLines)
            {
                var pivotReco = await GetOrCreateReconciliationAsync(pivotLine.ID);
                pivotReco.Action = (int)ActionType.Match;
                pivotReco.KPI = (int)KPIType.PaidButNotReconciled;
                pivotReco.Comments = $"Auto-matched with receivable line {receivableLine.ID}";
            }

            await SaveReconciliationsAsync(new[] { receivableReco }.Concat(pivotLines.Select(p => GetOrCreateReconciliationAsync(p.ID).Result)));
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
            var existing = await ExecuteQueryAsync<Reconciliation>(query, id);
            
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
                    await connection.OpenAsync();
                    using (var transaction = connection.BeginTransaction())
                    {
                        try
                        {
                            foreach (var reconciliation in reconciliations)
                            {
                                reconciliation.UpdateModification(_currentUser);
                                await SaveSingleReconciliationAsync(connection, transaction, reconciliation);
                            }

                            transaction.Commit();
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
        private async Task SaveSingleReconciliationAsync(OleDbConnection connection, OleDbTransaction transaction, Reconciliation reconciliation)
        {
            // Vérifier si l'enregistrement existe (par ID)
            var checkQuery = "SELECT COUNT(*) FROM T_Reconciliation WHERE ID = ?";
            using (var checkCmd = new OleDbCommand(checkQuery, connection, transaction))
            {
                checkCmd.Parameters.AddWithValue("@ID", reconciliation.ID);
                var exists = (int)await checkCmd.ExecuteScalarAsync() > 0;

                string query;
                if (exists)
                {
                    query = @"UPDATE T_Reconciliation SET 
                             [DWINGS_GuaranteeID] = ?, [DWINGS_InvoiceID] = ?, [DWINGS_CommissionID] = ?,
                             [Action] = ?, [Comments] = ?, [InternalInvoiceReference] = ?,
                             [FirstClaimDate] = ?, [LastClaimDate] = ?, [ToRemind] = ?, [ToRemindDate] = ?,
                             [ACK] = ?, [SwiftCode] = ?, [PaymentReference] = ?, [KPI] = ?,
                             [IncidentType] = ?, [RiskyItem] = ?, [ReasonNonRisky] = ?,
                             [ModifiedBy] = ?, [LastModified] = ?
                             WHERE [ID] = ?";
                }
                else
                {
                    query = @"INSERT INTO T_Reconciliation 
                             ([ID], [DWINGS_GuaranteeID], [DWINGS_InvoiceID], [DWINGS_CommissionID],
                              [Action], [Comments], [InternalInvoiceReference], [FirstClaimDate], [LastClaimDate],
                              [ToRemind], [ToRemindDate], [ACK], [SwiftCode], [PaymentReference], [KPI],
                              [IncidentType], [RiskyItem], [ReasonNonRisky], [CreationDate], [ModifiedBy], [LastModified])
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                }

                using (var cmd = new OleDbCommand(query, connection, transaction))
                {
                    AddReconciliationParameters(cmd, reconciliation, !exists);
                    await cmd.ExecuteNonQueryAsync();
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
            cmd.Parameters.AddWithValue("@Comments", reconciliation.Comments ?? (object)DBNull.Value);
            cmd.Parameters.AddWithValue("@InternalInvoiceReference", reconciliation.InternalInvoiceReference ?? (object)DBNull.Value);
            cmd.Parameters.AddWithValue("@FirstClaimDate", reconciliation.FirstClaimDate.HasValue ? reconciliation.FirstClaimDate.Value.ToOADate() : (object)DBNull.Value);
            cmd.Parameters.AddWithValue("@LastClaimDate", reconciliation.LastClaimDate.HasValue ? reconciliation.LastClaimDate.Value.ToOADate() : (object)DBNull.Value);
            cmd.Parameters.AddWithValue("@ToRemind", reconciliation.ToRemind);
            cmd.Parameters.AddWithValue("@ToRemindDate", reconciliation.ToRemindDate.HasValue ? reconciliation.ToRemindDate.Value.ToOADate() : (object)DBNull.Value);
            cmd.Parameters.AddWithValue("@ACK", reconciliation.ACK);
            cmd.Parameters.AddWithValue("@SwiftCode", reconciliation.SwiftCode ?? (object)DBNull.Value);
            cmd.Parameters.AddWithValue("@PaymentReference", reconciliation.PaymentReference ?? (object)DBNull.Value);
            cmd.Parameters.AddWithValue("@KPI", reconciliation.KPI ?? (object)DBNull.Value);
            cmd.Parameters.AddWithValue("@IncidentType", reconciliation.IncidentType ?? (object)DBNull.Value);
            cmd.Parameters.AddWithValue("@RiskyItem", reconciliation.RiskyItem ?? (object)DBNull.Value);
            cmd.Parameters.AddWithValue("@ReasonNonRisky", reconciliation.ReasonNonRisky ?? (object)DBNull.Value);

            if (isInsert)
                cmd.Parameters.AddWithValue("@CreationDate", reconciliation.CreationDate?.ToOADate());

            cmd.Parameters.AddWithValue("@ModifiedBy", reconciliation.ModifiedBy ?? (object)DBNull.Value);
            cmd.Parameters.AddWithValue("@LastModified", reconciliation.LastModified.HasValue ? reconciliation.LastModified.Value.ToOADate() : (object)DBNull.Value);

            if (!isInsert)
                cmd.Parameters.AddWithValue("@ID", reconciliation.ID);
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
                await cmd.ExecuteNonQueryAsync();
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
            return await ExecuteQueryAsync<T>(query, _connectionString, parameters);
        }

        /// <summary>
        /// Execute a query using a specific connection string (override).
        /// </summary>
        private async Task<List<T>> ExecuteQueryAsync<T>(string query, string connectionString, params object[] parameters) where T : new()
        {
            var results = new List<T>();

            using (var connection = new OleDbConnection(connectionString))
            {
                await connection.OpenAsync();
                using (var command = new OleDbCommand(query, connection))
                {
                    if (parameters != null)
                    {
                        for (int i = 0; i < parameters.Length; i++)
                        {
                            command.Parameters.AddWithValue($"@param{i}", parameters[i] ?? DBNull.Value);
                        }
                    }

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        var properties = typeof(T).GetProperties();

                        while (await reader.ReadAsync())
                        {
                            var item = new T();

                            foreach (var prop in properties)
                            {
                                try
                                {
                                    if (!prop.CanWrite || prop.GetSetMethod(true) == null || prop.GetIndexParameters().Length > 0)
                                        continue;

                                    var columnName = prop.Name;
                                    var columnIndex = GetColumnIndex(reader, columnName);
                                    if (columnIndex >= 0 && !reader.IsDBNull(columnIndex))
                                    {
                                        var value = reader[columnIndex];
                                        if (value == DBNull.Value)
                                            continue;

                                        var targetType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;

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
                                            else if (double.TryParse(Convert.ToString(value), out var d2))
                                            {
                                                converted = DateTime.FromOADate(d2);
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
                                            prop.SetValue(item, converted);
                                    }
                                }
                                catch
                                {
                                    // Ignore mapping issues per-property
                                }
                            }

                            results.Add(item);
                        }
                    }
                }
            }

            return results;
        }

        /// <summary>
        /// Vérifie si une colonne existe dans le reader
        /// </summary>
        /// <param name="reader">DbDataReader</param>
        /// <param name="columnName">Nom de la colonne</param>
        /// <returns>True si la colonne existe</returns>
        private bool HasColumn(DbDataReader reader, string columnName)
        {
            try
            {
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    if (reader.GetName(i).Equals(columnName, StringComparison.OrdinalIgnoreCase))
                        return true;
                }
                return false;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Obtient l'index d'une colonne dans le reader
        /// </summary>
        /// <param name="reader">DbDataReader</param>
        /// <param name="columnName">Nom de la colonne</param>
        /// <returns>Index de la colonne ou -1 si non trouvée</returns>
        private int GetColumnIndex(DbDataReader reader, string columnName)
        {
            try
            {
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    if (reader.GetName(i).Equals(columnName, StringComparison.OrdinalIgnoreCase))
                        return i;
                }
                return -1;
            }
            catch
            {
                return -1;
            }
        }

        #endregion
    }
}
