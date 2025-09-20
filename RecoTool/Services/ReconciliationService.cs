using OfflineFirstAccess.ChangeTracking;
using OfflineFirstAccess.Helpers;
using RecoTool.Models;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Data.OleDb;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using RecoTool.Helpers;
using System.Threading;
using System.Threading.Tasks;
using RecoTool.Services.DTOs;
using RecoTool.Domain.Filters;
using RecoTool.Services.Queries;
using RecoTool.Helpers;
using RecoTool.Services.Rules;
using RecoTool.Services.Helpers;

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
        private DwingsService _dwingsService;

        public ReconciliationService(string connectionString, string currentUser, IEnumerable<Country> countries)
        {
            _connectionString = connectionString;
            _currentUser = currentUser;
            _countries = countries?.ToDictionary(c => c.CNT_Id, c => c) ?? new Dictionary<string, Country>();
        }

        // Expose for infrastructure wiring (e.g., exports). Keep read-only.
        public string MainConnectionString => _connectionString;

        

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
            var ambreCs = _offlineFirstService?.GetAmbreConnectionString(countryId);

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

        public ReconciliationService(string connectionString, string currentUser, IEnumerable<Country> countries, OfflineFirstService offlineFirstService)
            : this(connectionString, currentUser, countries)
        {
            _offlineFirstService = offlineFirstService;
        }

        public string CurrentUser => _currentUser;

        #region Data Retrieval

        // Simple in-memory DWINGS caches for UI assistance searches (DTOs moved to Services/DTOs)

        // DWINGS access delegated to DwingsService

        // Cache for reconciliation view queries (task coalescing)
        private static readonly ConcurrentDictionary<string, Lazy<Task<List<ReconciliationViewData>>>> _recoViewCache
            = new ConcurrentDictionary<string, Lazy<Task<List<ReconciliationViewData>>>>();

        // Materialized data cache to allow incremental updates after saves without reloading
        private static readonly ConcurrentDictionary<string, List<ReconciliationViewData>> _recoViewDataCache
            = new ConcurrentDictionary<string, List<ReconciliationViewData>>();

        /// <summary>
        /// Clears all reconciliation view caches (both task and materialized data caches).
        /// Call after external mutations (e.g., pull from network) to force a reload on next request.
        /// </summary>
        public static void InvalidateReconciliationViewCache()
        {
            try
            {
                _recoViewDataCache.Clear();
            }
            catch { }
            try
            {
                foreach (var key in _recoViewCache.Keys)
                {
                    _recoViewCache.TryRemove(key, out _);
                }
            }
            catch { }
        }

        /// <summary>
        /// Clears reconciliation view caches for a specific country by prefix match on the cache key.
        /// Key format is "{countryId}|{dashboardOnly}|{normalizedFilter}".
        /// </summary>
        public static void InvalidateReconciliationViewCache(string countryId)
        {
            if (string.IsNullOrWhiteSpace(countryId)) { InvalidateReconciliationViewCache(); return; }
            try
            {
                var prefix = countryId + "|";
                foreach (var kv in _recoViewDataCache.ToArray())
                {
                    if (kv.Key.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                        _recoViewDataCache.TryRemove(kv.Key, out _);
                }
            }
            catch { }
            try
            {
                var prefix = countryId + "|";
                foreach (var kv in _recoViewCache.ToArray())
                {
                    if (kv.Key.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                        _recoViewCache.TryRemove(kv.Key, out _);
                }
            }
            catch { }
        }

        

        // Shared loader used by the shared cache
      
        

        public async Task<IReadOnlyList<DwingsInvoiceDto>> GetDwingsInvoicesAsync()
        {
            if (_dwingsService == null) _dwingsService = new DwingsService(_offlineFirstService);
            return await _dwingsService.GetInvoicesAsync().ConfigureAwait(false);
        }

        public async Task<IReadOnlyList<DwingsGuaranteeDto>> GetDwingsGuaranteesAsync()
        {
            if (_dwingsService == null) _dwingsService = new DwingsService(_offlineFirstService);
            return await _dwingsService.GetGuaranteesAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Récupère toutes les données Ambre pour un pays
        /// </summary>
        public async Task<List<DataAmbre>> GetAmbreDataAsync(string countryId, bool includeDeleted = false)
        {
            // Ambre est désormais dans une base séparée par pays
            var ambrePath = _offlineFirstService?.GetLocalAmbreDatabasePath(countryId);
            if (string.IsNullOrWhiteSpace(ambrePath))
                throw new InvalidOperationException("Chemin de la base AMBRE introuvable pour le pays courant.");
            var ambreCs = _offlineFirstService?.GetAmbreConnectionString(countryId);

            var query = @"SELECT * FROM T_Data_Ambre";
            if (!includeDeleted)
                query += " WHERE DeleteDate IS NULL";
            query += " ORDER BY Operation_Date DESC";

            return await ExecuteQueryAsync<DataAmbre>(query, ambreCs);
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

        // JSON filter preset is defined in Domain/Filters/FilterPreset

        /// <summary>
        /// Récupère les données jointes Ambre + Réconciliation
        /// </summary>
        public async Task<List<ReconciliationViewData>> GetReconciliationViewAsync(string countryId, string filterSql = null, bool dashboardOnly = false)
        {
            var key = $"{countryId ?? string.Empty}|{dashboardOnly}|{NormalizeFilterForCache(filterSql)}";
            if (_recoViewCache.TryGetValue(key, out var existing))
            {
                var cached = await existing.Value.ConfigureAwait(false);
                return cached;
            }
            var lazy = new Lazy<Task<List<ReconciliationViewData>>>(() => BuildReconciliationViewAsyncCore(countryId, filterSql, dashboardOnly, key));
            var entry = _recoViewCache.GetOrAdd(key, lazy);
            var result = await entry.Value.ConfigureAwait(false);
            return result;
        }

        private async Task<List<ReconciliationViewData>> BuildReconciliationViewAsyncCore(string countryId, string filterSql, bool dashboardOnly, string cacheKey)
        {
            var swBuild = Stopwatch.StartNew();
            string dwPath = _offlineFirstService?.GetLocalDWDatabasePath();
            string ambrePath = _offlineFirstService?.GetLocalAmbreDatabasePath(countryId);
            string dwEsc = string.IsNullOrEmpty(dwPath) ? null : dwPath.Replace("'", "''");
            string ambreEsc = string.IsNullOrEmpty(ambrePath) ? null : ambrePath.Replace("'", "''");

            // Detect PotentialDuplicates flag from optional JSON comment prefix (centralized helper)
            bool dupOnly = FilterSqlHelper.TryExtractPotentialDuplicatesFlag(filterSql);

            // Build the base query via centralized builder
            string query = ReconciliationViewQueryBuilder.Build(dwEsc, ambreEsc, filterSql, dashboardOnly);

            // Apply Potential Duplicates predicate if requested via JSON
            if (dupOnly)
            {
                query += " AND (dup.DupCount) > 1";
            }

            var predicate = FilterSqlHelper.ExtractNormalizedPredicate(filterSql);
            if (!string.IsNullOrEmpty(predicate))
            {
                query += $" AND ({predicate})";
            }

            query += " ORDER BY a.Operation_Date DESC";

            swBuild.Stop();
            var swExec = Stopwatch.StartNew();
            var list = await ExecuteQueryAsync<ReconciliationViewData>(query);

            // Enrich invoice fields via centralized helper (no SQL join to T_DW_Data)
            try
            {
                var invoices = await GetDwingsInvoicesAsync().ConfigureAwait(false);
                ReconciliationViewEnricher.EnrichWithDwingsInvoices(list, invoices);
            }
            catch { /* best-effort enrichment */ }

            // Compute transient UI flags for new/updated based on reconciliation timestamps
            try
            {
                var today = DateTime.Today;
                foreach (var row in list)
                {
                    // New if reconciliation CreationDate is today
                    if (row.Reco_CreationDate.HasValue && row.Reco_CreationDate.Value.Date == today)
                    {
                        row.IsNewlyAdded = true;
                    }
                    // Updated if reconciliation LastModified is today and differs from CreationDate
                    if (row.Reco_LastModified.HasValue && row.Reco_LastModified.Value.Date == today)
                    {
                        if (!row.Reco_CreationDate.HasValue || row.Reco_LastModified.Value > row.Reco_CreationDate.Value)
                        {
                            row.IsUpdated = true;
                        }
                    }
                }
            }
            catch { }
            
            // Compute AccountSide and Matched-across-accounts flag (instance context; use _offlineFirstService)
            try
            {
                var currentCountry = _offlineFirstService?.CurrentCountry;
                var pivotId = currentCountry?.CNT_AmbrePivot?.Trim();
                var recvId = currentCountry?.CNT_AmbreReceivable?.Trim();
                foreach (var row in list)
                {
                    var acc = row.Account_ID?.Trim();
                    if (!string.IsNullOrWhiteSpace(pivotId) && string.Equals(acc, pivotId, StringComparison.OrdinalIgnoreCase))
                        row.AccountSide = "P";
                    else if (!string.IsNullOrWhiteSpace(recvId) && string.Equals(acc, recvId, StringComparison.OrdinalIgnoreCase))
                        row.AccountSide = "R";
                    else row.AccountSide = null;
                }

                // Group by DWINGS_InvoiceID first; if empty, group by InternalInvoiceReference
                var byInvoice = list.Where(r => !string.IsNullOrWhiteSpace(r.DWINGS_InvoiceID))
                                    .GroupBy(r => r.DWINGS_InvoiceID, StringComparer.OrdinalIgnoreCase);
                foreach (var g in byInvoice)
                {
                    bool hasP = g.Any(x => string.Equals(x.AccountSide, "P", StringComparison.OrdinalIgnoreCase));
                    bool hasR = g.Any(x => string.Equals(x.AccountSide, "R", StringComparison.OrdinalIgnoreCase));
                    if (hasP && hasR)
                    {
                        foreach (var row in g) row.IsMatchedAcrossAccounts = true;
                    }
                }
                var byInternal = list.Where(r => string.IsNullOrWhiteSpace(r.DWINGS_InvoiceID) && !string.IsNullOrWhiteSpace(r.InternalInvoiceReference))
                                      .GroupBy(r => r.InternalInvoiceReference, StringComparer.OrdinalIgnoreCase);
                foreach (var g in byInternal)
                {
                    bool hasP = g.Any(x => string.Equals(x.AccountSide, "P", StringComparison.OrdinalIgnoreCase));
                    bool hasR = g.Any(x => string.Equals(x.AccountSide, "R", StringComparison.OrdinalIgnoreCase));
                    if (hasP && hasR)
                    {
                        foreach (var row in g) row.IsMatchedAcrossAccounts = true;
                    }
                }

                // Fallback: if DWINGS_InvoiceID is not populated yet, try matching by BGI token present in AMBRE fields
                // This helps when users manually link BGI in receivable/pivot text fields but enrichment did not resolve DWINGS_InvoiceID for both sides.
                string Norm(string s) => string.IsNullOrWhiteSpace(s) ? null : s.Trim().ToUpperInvariant();
                string KeyFromRow(RecoTool.Services.DTOs.ReconciliationViewData r)
                {
                    // Priority order: DWINGS_InvoiceID -> Receivable_InvoiceFromAmbre -> extracted BGI from common text fields -> InternalInvoiceReference
                    var k = Norm(r.DWINGS_InvoiceID);
                    if (!string.IsNullOrWhiteSpace(k)) return k;
                    k = Norm(r.Receivable_InvoiceFromAmbre);
                    if (!string.IsNullOrWhiteSpace(k)) return k;
                    // Try extract BGI token heuristically from available texts
                    var probe = r.Reconciliation_Num;
                    var token = DwingsLinkingHelper.ExtractBgiToken(probe)
                               ?? DwingsLinkingHelper.ExtractBgiToken(r.Comments)
                               ?? DwingsLinkingHelper.ExtractBgiToken(r.RawLabel)
                               ?? DwingsLinkingHelper.ExtractBgiToken(r.Receivable_DWRefFromAmbre);
                    if (!string.IsNullOrWhiteSpace(token)) return Norm(token);
                    // Lastly, fall back to internal reference if present
                    return Norm(r.InternalInvoiceReference);
                }

                var byAnyBgi = list
                    .Select(r => new { Row = r, Key = KeyFromRow(r) })
                    .Where(x => !string.IsNullOrWhiteSpace(x.Key))
                    .GroupBy(x => x.Key, StringComparer.OrdinalIgnoreCase);

                foreach (var g in byAnyBgi)
                {
                    bool hasP = g.Any(x => string.Equals(x.Row.AccountSide, "P", StringComparison.OrdinalIgnoreCase));
                    bool hasR = g.Any(x => string.Equals(x.Row.AccountSide, "R", StringComparison.OrdinalIgnoreCase));
                    if (hasP && hasR)
                    {
                        foreach (var it in g) it.Row.IsMatchedAcrossAccounts = true;
                    }
                }
            }
            catch { }

            // Enrich guarantee fields from in-memory DWINGS cache (avoid heavy SQL joins and keep UI robust)
            try
            {
                var guarantees = await GetDwingsGuaranteesAsync().ConfigureAwait(false);
                var byGuaranteeId = guarantees.Where(g => !string.IsNullOrWhiteSpace(g.GUARANTEE_ID))
                                              .GroupBy(g => g.GUARANTEE_ID, StringComparer.OrdinalIgnoreCase)
                                              .ToDictionary(g => g.Key, g => g.First(), StringComparer.OrdinalIgnoreCase);

                foreach (var row in list)
                {
                    // Resolve guarantee id heuristically if missing
                    string gid = row.DWINGS_GuaranteeID;
                    if (string.IsNullOrWhiteSpace(gid))
                    {
                        // Try extract from various fields
                        gid = DwingsLinkingHelper.ExtractGuaranteeId(row.Reconciliation_Num)
                              ?? DwingsLinkingHelper.ExtractGuaranteeId(row.Comments)
                              ?? DwingsLinkingHelper.ExtractGuaranteeId(row.Receivable_DWRefFromAmbre)
                              ?? DwingsLinkingHelper.ExtractGuaranteeId(row.RawLabel)
                              ?? row.I_BUSINESS_CASE_REFERENCE // populated above from invoice if found
                              ?? row.I_BUSINESS_CASE_ID;

                        if (!string.IsNullOrWhiteSpace(gid))
                        {
                            row.DWINGS_GuaranteeID = gid; // backfill for consistency
                        }
                    }

                    if (string.IsNullOrWhiteSpace(row.DWINGS_GuaranteeID)) continue;
                    if (!byGuaranteeId.TryGetValue(row.DWINGS_GuaranteeID, out var g)) continue;

                    row.GUARANTEE_ID = g.GUARANTEE_ID;
                    row.GUARANTEE_STATUS = g.GUARANTEE_STATUS ?? row.GUARANTEE_STATUS;
                    row.GUARANTEE_TYPE = g.GUARANTEE_TYPE ?? row.GUARANTEE_TYPE;
                    // Also hydrate the prefixed field used directly by the grid binding
                    row.G_GUARANTEE_TYPE = g.GUARANTEE_TYPE ?? row.G_GUARANTEE_TYPE;

                    // Prefixed G_* extended fields
                    row.G_NATURE = g.NATURE ?? row.G_NATURE;
                    row.G_EVENT_STATUS = g.EVENT_STATUS ?? row.G_EVENT_STATUS;
                    row.G_EVENT_EFFECTIVEDATE = g.EVENT_EFFECTIVEDATE?.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) ?? row.G_EVENT_EFFECTIVEDATE;
                    row.G_ISSUEDATE = g.ISSUEDATE?.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) ?? row.G_ISSUEDATE;
                    row.G_OFFICIALREF = g.OFFICIALREF ?? row.G_OFFICIALREF;
                    row.G_UNDERTAKINGEVENT = g.UNDERTAKINGEVENT ?? row.G_UNDERTAKINGEVENT;
                    row.G_PROCESS = g.PROCESS ?? row.G_PROCESS;
                    row.G_EXPIRYDATETYPE = g.EXPIRYDATETYPE ?? row.G_EXPIRYDATETYPE;
                    row.G_EXPIRYDATE = g.EXPIRYDATE?.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) ?? row.G_EXPIRYDATE;
                    row.G_PARTY_ID = g.PARTY_ID ?? row.G_PARTY_ID;
                    row.G_PARTY_REF = g.PARTY_REF ?? row.G_PARTY_REF;
                    row.G_SECONDARY_OBLIGOR = g.SECONDARY_OBLIGOR ?? row.G_SECONDARY_OBLIGOR;
                    row.G_SECONDARY_OBLIGOR_NATURE = g.SECONDARY_OBLIGOR_NATURE ?? row.G_SECONDARY_OBLIGOR_NATURE;
                    row.G_ROLE = g.ROLE ?? row.G_ROLE;
                    row.G_COUNTRY = g.COUNTRY ?? row.G_COUNTRY;
                    row.G_CENTRAL_PARTY_CODE = g.CENTRAL_PARTY_CODE ?? row.G_CENTRAL_PARTY_CODE;
                    row.G_NAME1 = g.NAME1 ?? row.G_NAME1;
                    row.G_NAME2 = g.NAME2 ?? row.G_NAME2;
                    row.G_GROUPE = g.GROUPE ?? row.G_GROUPE;
                    row.G_PREMIUM = g.PREMIUM ?? row.G_PREMIUM;
                    row.G_BRANCH_CODE = g.BRANCH_CODE ?? row.G_BRANCH_CODE;
                    row.G_BRANCH_NAME = g.BRANCH_NAME ?? row.G_BRANCH_NAME;
                    row.G_OUTSTANDING_AMOUNT_IN_BOOKING_CURRENCY = g.OUTSTANDING_AMOUNT_IN_BOOKING_CURRENCY ?? row.G_OUTSTANDING_AMOUNT_IN_BOOKING_CURRENCY;
                    row.G_CANCELLATIONDATE = g.CANCELLATIONDATE?.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) ?? row.G_CANCELLATIONDATE;
                    row.G_CONTROLER = g.CONTROLER ?? row.G_CONTROLER;
                    row.G_AUTOMATICBOOKOFF = g.AUTOMATICBOOKOFF ?? row.G_AUTOMATICBOOKOFF;
                    row.G_NATUREOFDEAL = g.NATUREOFDEAL ?? row.G_NATUREOFDEAL;
                }
            }
            catch { /* best-effort enrichment */ }
            swExec.Stop();

            // Store materialized list for incremental updates
            _recoViewDataCache[cacheKey] = list;
            return list;
        }

        private static string NormalizeFilterForCache(string filterSql)
        {
            if (string.IsNullOrWhiteSpace(filterSql)) return string.Empty;
            var cond = filterSql.Trim();
            // Strip optional JSON header
            var m = Regex.Match(cond, @"^/\*JSON:(.*?)\*/\s*(.*)$", RegexOptions.Singleline);
            if (m.Success) cond = m.Groups[2].Value?.Trim();
            // Strip leading WHERE
            if (cond.StartsWith("WHERE ", StringComparison.OrdinalIgnoreCase)) cond = cond.Substring(6).Trim();
            // Collapse whitespace
            cond = Regex.Replace(cond, @"\s+", " ").Trim();
            return cond;
        }

        /// <summary>
        /// Compute the automatic Action to apply after an AMBRE import for a reconciliation item, according to business rules.
        /// IMPORTANT: this must NOT override user-forced actions; call this only when r.Action is null.
        /// </summary>
        /// <param name="transactionType">Detected transaction type (from AMBRE label/category)</param>
        /// <param name="a">AMBRE row</param>
        /// <param name="r">Reconciliation row (same ID)</param>
        /// <param name="country">Country (to resolve Pivot/Receivable)</param>
        /// <param name="paymentMethod">Optional DWINGS payment method (MANUAL_OUTGOING/OUTGOING_PAYMENT/DIRECT_DEBIT/INCOMING_PAYMENT)</param>
        /// <param name="today">Business current date (DateTime.Today recommended)</param>
        /// <returns>ActionType to set, or null if no automatic action applies</returns>
        public ActionType? ComputeAutoAction(TransactionType? transactionType, DataAmbre a, Reconciliation r, Country country, string paymentMethod, DateTime today)
        {
            if (a == null || r == null || country == null) return null;
            // Never override a user-forced action
            if (r.Action.HasValue) return null;

            bool isPivot = a.IsPivotAccount(country.CNT_AmbrePivot);
            bool isReceivable = !isPivot; // Per model: receivable is the opposite of pivot for our 2-account scope

            // Helpers
            bool IsPm(string code) => !string.IsNullOrWhiteSpace(paymentMethod) && paymentMethod.Equals(code, StringComparison.OrdinalIgnoreCase);
            bool TxIs(TransactionType t) => transactionType.HasValue && transactionType.Value == t;

            // Rule 1: If category = COLLECTION and TriggerDate = blank THEN TRIGGER
            if (TxIs(TransactionType.COLLECTION) && !r.TriggerDate.HasValue)
                return ActionType.Trigger;

            // Rule 2: ELSE IF transitory = "Y" AND Op.date < D-1 THEN INVESTIGATE (Pivot only)
            // Transitory means Reconciliation_Num contains BGPMT
            bool transitory = !string.IsNullOrWhiteSpace(a.Reconciliation_Num) && a.Reconciliation_Num.IndexOf("BGPMT", StringComparison.OrdinalIgnoreCase) >= 0;
            if (isPivot && transitory)
            {
                DateTime? op = a.Operation_Date?.Date;
                if (op.HasValue && op.Value < today.AddDays(-1))
                    return ActionType.Investigate;
            }

            // Rule 3: ELSE IF TriggerDate <= D-1 AND matched = "N" AND manual match date = blank THEN MATCH
            // matched = DeleteDate != null (AMBRE or Reconciliation base entities). We use AMBRE deletion to reflect record matched/removed.
            bool matched = a.DeleteDate.HasValue; // per spec "Matched = DeleteDate is not null"
            bool manualMatchBlank = r.Action.GetValueOrDefault() != (int)ActionType.Match; // no explicit manual match set
            if (r.TriggerDate.HasValue && r.TriggerDate.Value.Date <= today.AddDays(-1) && !matched && manualMatchBlank)
                return ActionType.Match;

            // Rule 4: ELSE IF Account = receivable THEN
            if (isReceivable)
            {
                // If Payment method = MANUAL_OUTGOING THEN TRIGGER
                if (IsPm("MANUAL_OUTGOING") || TxIs(TransactionType.MANUAL_OUTGOING))
                    return ActionType.Trigger;

                // If Payment method = OUTGOING_PAYMENT THEN EXECUTE
                if (IsPm("OUTGOING_PAYMENT") || TxIs(TransactionType.OUTGOING_PAYMENT))
                    return ActionType.Execute;

                // If Payment method = DIRECT_DEBIT: no explicit rule here for auto action beyond table; leave null (let KPI mapping handle later)
                if (IsPm("DIRECT_DEBIT") || TxIs(TransactionType.DIRECT_DEBIT))
                    return null;

                // If Payment method = Incoming THEN
                // Interpret Incoming as INCOMING_PAYMENT per unified enum/tag
                if (IsPm("INCOMING_PAYMENT") || TxIs(TransactionType.INCOMING_PAYMENT))
                {
                    // IF first request = blank THEN REQUEST
                    // FirstRequest mapped to CreationDate (all tables, AMBRE). If never requested before -> use AMBRE creation being today as first appearance.
                    // If CreationDate is null or equals today, consider it first request.
                    var ambreCreated = a.CreationDate?.Date;
                    if (!ambreCreated.HasValue || ambreCreated.Value == today.Date)
                        return ActionType.Request;

                    // ELSE IF last reminder > 30 days THEN REMIND
                    if (r.ToRemindDate.HasValue && r.ToRemindDate.Value.Date <= today.AddDays(-30).Date)
                        return ActionType.Remind;
                }
            }

            // ELSE BLANK (no automatic action)
            return null;
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
                    bool isReceivable = !isPivot; // Per model: receivable is the opposite of pivot for our 2-account scope

                    if (isPivot)
                    {
                        ReconciliationRules.ApplyPivotRules(reconciliation, data);
                    }
                    else if (isReceivable)
                    {
                        ReconciliationRules.ApplyReceivableRules(reconciliation, data);
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
                            var updatedRows = new List<Reconciliation>();
                            foreach (var reconciliation in reconciliations)
                            {
                                var op = await SaveSingleReconciliationAsync(connection, transaction, reconciliation).ConfigureAwait(false);
                                if (!string.Equals(op, "NOOP", StringComparison.OrdinalIgnoreCase))
                                {
                                    changeTuples.Add(("T_Reconciliation", reconciliation.ID, op));
                                    updatedRows.Add(reconciliation);
                                }
                            }

                            transaction.Commit();

                            // Invalidate caches so next view refresh recomputes flags (e.g., IsMatchedAcrossAccounts)
                            try
                            {
                                var countryId = _offlineFirstService?.CurrentCountryId;
                                if (!string.IsNullOrWhiteSpace(countryId))
                                    InvalidateReconciliationViewCache(countryId);
                                else
                                    InvalidateReconciliationViewCache();
                            }
                            catch { }

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
                            catch (Exception ex)
                            {
                                // Swallow change-log errors to not block user saves
                                // Diagnostic only: log once here to help track missing pushes (background sync reads ChangeLog)
                                try { LogManager.Warning("ChangeLog recording failed in SaveReconciliationsAsync; background sync will skip these rows unless reconstructed."); } catch { }
                            }

                            // Invalidate view cache so next loads fetch fresh data
                            //try { _recoViewCache.Clear(); } catch { }

                            // Incrementally update all cached view lists with the modified reconciliation fields
                            try { UpdateRecoViewCaches(updatedRows); } catch { }

                            // Synchronization is handled by background services (e.g., SyncMonitor),
                            // which read pending items from ChangeLog and then perform PUSH followed by PULL.
                            // No direct push is triggered here.
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

        private void UpdateRecoViewCaches(IEnumerable<Reconciliation> updated)
        {
            if (updated == null) return;
            foreach (var kv in _recoViewDataCache)
            {
                var list = kv.Value;
                if (list == null) continue;
                // Update in place by ID (AMBRE row always exists; reconcile fields are nullable)
                foreach (var r in updated)
                {
                    var row = list.FirstOrDefault(x => string.Equals(x.ID, r.ID, StringComparison.OrdinalIgnoreCase));
                    if (row == null) continue;
                    row.DWINGS_GuaranteeID = r.DWINGS_GuaranteeID;
                    row.DWINGS_InvoiceID = r.DWINGS_InvoiceID;
                    row.DWINGS_CommissionID = r.DWINGS_CommissionID;
                    row.Action = r.Action;
                    row.ActionStatus = r.ActionStatus;
                    row.ActionDate = r.ActionDate;
                    row.Assignee = r.Assignee;
                    row.Comments = r.Comments;
                    row.InternalInvoiceReference = r.InternalInvoiceReference;
                    row.FirstClaimDate = r.FirstClaimDate;
                    row.LastClaimDate = r.LastClaimDate;
                    row.ToRemind = r.ToRemind;
                    row.ToRemindDate = r.ToRemindDate;
                    row.ACK = r.ACK;
                    row.SwiftCode = r.SwiftCode;
                    row.PaymentReference = r.PaymentReference;
                    row.KPI = r.KPI;
                    row.IncidentType = r.IncidentType;
                    row.RiskyItem = r.RiskyItem == true;
                    row.ReasonNonRisky = r.ReasonNonRisky;
                    row.MbawData = r.MbawData;
                    row.SpiritData = r.SpiritData;
                    row.TriggerDate = r.TriggerDate;
                }
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
                                [Action], [ActionStatus], [ActionDate], [Assignee], [Comments], [InternalInvoiceReference],
                                [FirstClaimDate], [LastClaimDate], [ToRemind], [ToRemindDate],
                                [ACK], [SwiftCode], [PaymentReference], [KPI],
                                [IncidentType], [RiskyItem], [ReasonNonRisky],
                                [MbawData], [SpiritData], [TriggerDate]
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
                            if (!Equal(DbVal(4), (object)reconciliation.ActionStatus)) changed.Add("ActionStatus");
                            if (!Equal(DbVal(5), reconciliation.ActionDate.HasValue ? (object)reconciliation.ActionDate.Value : null)) changed.Add("ActionDate");
                            if (!Equal(DbVal(6), (object)reconciliation.Assignee)) changed.Add("Assignee");
                            if (!Equal(DbVal(7), (object)reconciliation.Comments)) changed.Add("Comments");
                            if (!Equal(DbVal(8), (object)reconciliation.InternalInvoiceReference)) changed.Add("InternalInvoiceReference");
                            if (!Equal(DbVal(9), reconciliation.FirstClaimDate.HasValue ? (object)reconciliation.FirstClaimDate.Value : null)) changed.Add("FirstClaimDate");
                            if (!Equal(DbVal(10), reconciliation.LastClaimDate.HasValue ? (object)reconciliation.LastClaimDate.Value : null)) changed.Add("LastClaimDate");
                            if (!Equal(DbBool(DbVal(11)), (object)reconciliation.ToRemind)) changed.Add("ToRemind");
                            if (!Equal(DbVal(12), reconciliation.ToRemindDate.HasValue ? (object)reconciliation.ToRemindDate.Value : null)) changed.Add("ToRemindDate");
                            if (!Equal(DbBool(DbVal(13)), (object)reconciliation.ACK)) changed.Add("ACK");
                            if (!Equal(DbVal(14), (object)reconciliation.SwiftCode)) changed.Add("SwiftCode");
                            if (!Equal(DbVal(15), (object)reconciliation.PaymentReference)) changed.Add("PaymentReference");
                            if (!Equal(DbVal(16), (object)reconciliation.KPI)) changed.Add("KPI");
                            if (!Equal(DbVal(17), (object)reconciliation.IncidentType)) changed.Add("IncidentType");
                            if (!Equal(DbBool(DbVal(18)), (object)reconciliation.RiskyItem)) changed.Add("RiskyItem");
                            if (!Equal(DbVal(19), (object)reconciliation.ReasonNonRisky)) changed.Add("ReasonNonRisky");
                            if (!Equal(DbVal(20), (object)reconciliation.MbawData)) changed.Add("MbawData");
                            if (!Equal(DbVal(21), (object)reconciliation.SpiritData)) changed.Add("SpiritData");
                            if (!Equal(DbVal(22), reconciliation.TriggerDate.HasValue ? (object)reconciliation.TriggerDate.Value : null)) changed.Add("TriggerDate");

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
                                case "ActionStatus":
                                    {
                                        var p = cmd.Parameters.Add("@ActionStatus", OleDbType.Boolean);
                                        p.Value = reconciliation.ActionStatus.HasValue ? (object)reconciliation.ActionStatus.Value : DBNull.Value;
                                        break;
                                    }
                                case "ActionDate":
                                    {
                                        var p = cmd.Parameters.Add("@ActionDate", OleDbType.Date);
                                        p.Value = reconciliation.ActionDate.HasValue ? (object)reconciliation.ActionDate.Value : DBNull.Value;
                                        break;
                                    }
                                case "Assignee":
                                    cmd.Parameters.AddWithValue("@Assignee", string.IsNullOrWhiteSpace(reconciliation.Assignee) ? (object)DBNull.Value : reconciliation.Assignee);
                                    break;
                                case "Comments":
                                    {
                                        var p = cmd.Parameters.Add("@Comments", OleDbType.LongVarWChar);
                                        p.Value = reconciliation.Comments ?? (object)DBNull.Value;
                                        break;
                                    }
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
                                    {
                                        var p = cmd.Parameters.Add("@MbawData", OleDbType.LongVarWChar);
                                        p.Value = reconciliation.MbawData ?? (object)DBNull.Value;
                                        break;
                                    }
                                case "SpiritData":
                                    {
                                        var p = cmd.Parameters.Add("@SpiritData", OleDbType.LongVarWChar);
                                        p.Value = reconciliation.SpiritData ?? (object)DBNull.Value;
                                        break;
                                    }
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
                                case "TriggerDate":
                                    {
                                        var p = cmd.Parameters.Add("@TriggerDate", OleDbType.Date);
                                        p.Value = reconciliation.TriggerDate.HasValue ? (object)reconciliation.TriggerDate.Value : DBNull.Value;
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
                              [Action], [ActionStatus], [ActionDate], [Assignee], [Comments], [InternalInvoiceReference], [FirstClaimDate], [LastClaimDate],
                              [ToRemind], [ToRemindDate], [ACK], [SwiftCode], [PaymentReference], [MbawData], [SpiritData], [KPI],
                              [IncidentType], [RiskyItem], [ReasonNonRisky], [TriggerDate], [CreationDate], [ModifiedBy], [LastModified])
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

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
            var pActionStatus = cmd.Parameters.Add("@ActionStatus", OleDbType.Boolean);
            pActionStatus.Value = reconciliation.ActionStatus.HasValue ? (object)reconciliation.ActionStatus.Value : DBNull.Value;
            var pActionDate = cmd.Parameters.Add("@ActionDate", OleDbType.Date);
            pActionDate.Value = reconciliation.ActionDate.HasValue ? (object)reconciliation.ActionDate.Value : DBNull.Value;
            cmd.Parameters.AddWithValue("@Assignee", string.IsNullOrWhiteSpace(reconciliation.Assignee) ? (object)DBNull.Value : reconciliation.Assignee);
            var pComments = cmd.Parameters.Add("@Comments", OleDbType.LongVarWChar);
            pComments.Value = reconciliation.Comments ?? (object)DBNull.Value;
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
            var pMbaw = cmd.Parameters.Add("@MbawData", OleDbType.LongVarWChar);
            pMbaw.Value = reconciliation.MbawData ?? (object)DBNull.Value;
            var pSpirit = cmd.Parameters.Add("@SpiritData", OleDbType.LongVarWChar);
            pSpirit.Value = reconciliation.SpiritData ?? (object)DBNull.Value;
            var pKpi = cmd.Parameters.Add("@KPI", OleDbType.Integer);
            pKpi.Value = reconciliation.KPI.HasValue ? (object)reconciliation.KPI.Value : DBNull.Value;
            var pInc = cmd.Parameters.Add("@IncidentType", OleDbType.Integer);
            pInc.Value = reconciliation.IncidentType.HasValue ? (object)reconciliation.IncidentType.Value : DBNull.Value;
            var pRisky = cmd.Parameters.Add("@RiskyItem", OleDbType.Boolean);
            pRisky.Value = reconciliation.RiskyItem.HasValue ? (object)reconciliation.RiskyItem.Value : DBNull.Value;
            var pReason = cmd.Parameters.Add("@ReasonNonRisky", OleDbType.Integer);
            pReason.Value = reconciliation.ReasonNonRisky.HasValue ? (object)reconciliation.ReasonNonRisky.Value : DBNull.Value;
            var pTrigDate = cmd.Parameters.Add("@TriggerDate", OleDbType.Date);
            pTrigDate.Value = reconciliation.TriggerDate.HasValue ? (object)reconciliation.TriggerDate.Value : DBNull.Value;

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
                await connection.OpenAsync().ConfigureAwait(false);
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
                    using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                    {
                        swExec.Stop();
                        msExecute = swExec.ElapsedMilliseconds;
                        // Cache column ordinals once per reader
                        var swPrep = Stopwatch.StartNew();
                        var columnOrdinals = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            var name = reader.GetName(i);
                            // Overwrite on duplicate names so later SELECT items (e.g., r.Comments) win over earlier ones (e.g., a.Comments)
                            columnOrdinals[name] = i;
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
                        while (await reader.ReadAsync().ConfigureAwait(false))
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
                                        else if (value is DateTimeOffset dto)
                                        {
                                            converted = dto.UtcDateTime;
                                        }
                                        else
                                        {
                                            var sVal = Convert.ToString(value);
                                            if (!string.IsNullOrWhiteSpace(sVal))
                                            {
                                                // Prefer DWINGS textual date formats like 30-APR-22
                                                if (DwingsDateHelper.TryParseDwingsDate(sVal, out var parsed))
                                                {
                                                    converted = parsed;
                                                }
                                                else if (DateTime.TryParse(sVal, CultureInfo.InvariantCulture, DateTimeStyles.None, out var gen))
                                                {
                                                    converted = gen;
                                                }
                                                else if (DateTime.TryParse(sVal, CultureInfo.GetCultureInfo("fr-FR"), DateTimeStyles.None, out gen))
                                                {
                                                    converted = gen;
                                                }
                                                else if (DateTime.TryParse(sVal, CultureInfo.GetCultureInfo("it-IT"), DateTimeStyles.None, out gen))
                                                {
                                                    converted = gen;
                                                }
                                                else
                                                {
                                                    try { converted = Convert.ToDateTime(value, CultureInfo.InvariantCulture); } catch { converted = null; }
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
                                    else if (targetType == typeof(string))
                                    {
                                        // Ensure Memo/LongText (OleDb LongVarWChar) map reliably to .NET string
                                        try { converted = Convert.ToString(value); } catch { converted = value?.ToString(); }
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
                                try { DwingsDateHelper.NormalizeDwingsDateStrings(item as ReconciliationViewData); } catch { }
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
    }
    #endregion
}
