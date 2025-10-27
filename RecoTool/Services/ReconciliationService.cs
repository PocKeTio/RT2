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
using RecoTool.Infrastructure.Logging;
using RecoTool.Services.Cache;

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
        private RulesEngine _rulesEngine;

        #region Events
        public sealed class RuleAppliedEventArgs : EventArgs
        {
            public string Origin { get; set; } // import | edit | run-now
            public string CountryId { get; set; }
            public string ReconciliationId { get; set; }
            public string RuleId { get; set; }
            public string Outputs { get; set; }
            public string Message { get; set; }
        }

        /// <summary>
        /// Returns total absolute amounts by currency for Live rows matching the provided backend filter.
        /// Uses the same base query as GetReconciliationCountAsync and groups by CCY.
        /// OPTIMIZED: Cached based on countryId + filterSql (AMBRE data rarely changes)
        /// </summary>
        public async Task<Dictionary<string, double>> GetCurrencySumsAsync(string countryId, string filterSql = null)
        {
            // OPTIMIZATION: Cache currency sums (only changes on AMBRE import)
            var cacheKey = $"CurrencySums_{countryId}_{NormalizeFilterForCache(filterSql)}";
            return await CacheService.Instance.GetOrLoadAsync(cacheKey, async () =>
            {
                try
                {
                    string dwPath = _offlineFirstService?.GetLocalDWDatabasePath();
                    string ambrePath = _offlineFirstService?.GetLocalAmbreDatabasePath(countryId);
                    string dwEsc = string.IsNullOrEmpty(dwPath) ? null : dwPath.Replace("'", "''");
                    string ambreEsc = string.IsNullOrEmpty(ambrePath) ? null : ambrePath.Replace("'", "''");

                    // Detect duplicates-only flag from optional JSON header (ignored for sums)
                    // Build base query
                    string query = ReconciliationViewQueryBuilder.Build(dwEsc, ambreEsc, filterSql);

                    // Always enforce Live scope
                    query += " AND a.DeleteDate IS NULL AND (r.DeleteDate IS NULL)";

                    var predicate = FilterSqlHelper.ExtractNormalizedPredicate(filterSql);
                    if (!string.IsNullOrEmpty(predicate))
                    {
                        query += $" AND ({predicate})";
                    }

                    var sumsSql = $"SELECT CCY, SUM(ABS(SignedAmount)) AS Amount FROM ({query}) AS q GROUP BY CCY";

                    var result = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);
                    using (var connection = new OleDbConnection(_connectionString))
                    {
                        await connection.OpenAsync().ConfigureAwait(false);
                        using (var cmd = new OleDbCommand(sumsSql, connection))
                        using (var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false))
                        {
                            while (await reader.ReadAsync().ConfigureAwait(false))
                            {
                                string ccy = reader.IsDBNull(0) ? null : Convert.ToString(reader[0]);
                                if (string.IsNullOrWhiteSpace(ccy)) continue;
                                double amount = 0;
                                try { amount = reader.IsDBNull(1) ? 0 : Convert.ToDouble(reader[1]); } catch { amount = 0; }
                                if (result.ContainsKey(ccy)) result[ccy] += amount; else result[ccy] = amount;
                            }
                        }
                    }
                    return result;
                }
                catch
                {
                    return new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);
                }
            }, TimeSpan.FromHours(24)).ConfigureAwait(false); // Cache for 24 hours (AMBRE changes only on import)
        }
        public event EventHandler<RuleAppliedEventArgs> RuleApplied;
        private void RaiseRuleApplied(string origin, string countryId, string recoId, string ruleId, string outputs, string message)
        {
            try { RuleApplied?.Invoke(this, new RuleAppliedEventArgs { Origin = origin, CountryId = countryId, ReconciliationId = recoId, RuleId = ruleId, Outputs = outputs, Message = message }); } catch { }
        }
        #endregion

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
            try { _rulesEngine = new RulesEngine(_offlineFirstService); } catch { _rulesEngine = null; }
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
        /// OPTIMIZATION: Also clears CacheService entries for StatusCounts, RecoCount, and CurrencySums
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
            
            // OPTIMIZATION: Invalidate CacheService entries for counts and sums
            try
            {
                CacheService.Instance.InvalidateByPrefix("StatusCounts_");
                CacheService.Instance.InvalidateByPrefix("RecoCount_");
                CacheService.Instance.InvalidateByPrefix("CurrencySums_");
            }
            catch { }
        }

        /// <summary>
        /// Clears reconciliation view caches for a specific country by prefix match on the cache key.
        /// Key format is "{countryId}|{dashboardOnly}|{normalizedFilter}".
        /// OPTIMIZATION: Also clears CacheService entries for StatusCounts, RecoCount, and CurrencySums for this country
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
            
            // OPTIMIZATION: Invalidate CacheService entries for this country
            try
            {
                CacheService.Instance.InvalidateByPrefix($"StatusCounts_{countryId}_");
                CacheService.Instance.InvalidateByPrefix($"RecoCount_{countryId}_");
                CacheService.Instance.InvalidateByPrefix($"CurrencySums_{countryId}_");
            }
            catch { }
        }

        

        // Shared loader used by the shared cache
      
        

        public async Task<IReadOnlyList<DwingsInvoiceDto>> GetDwingsInvoicesAsync()
        {
            // OPTIMIZATION: Cache DWINGS invoices permanently per country (never expires)
            var cacheKey = $"DWINGS_Invoices_{_offlineFirstService?.CurrentCountryId}";
            return await CacheService.Instance.GetOrLoadAsync(cacheKey, async () =>
            {
                if (_dwingsService == null) _dwingsService = new DwingsService(_offlineFirstService);
                return await _dwingsService.GetInvoicesAsync().ConfigureAwait(false);
            }).ConfigureAwait(false);
        }

        public async Task<IReadOnlyList<DwingsGuaranteeDto>> GetDwingsGuaranteesAsync()
        {
            // OPTIMIZATION: Cache DWINGS guarantees permanently per country (never expires)
            var cacheKey = $"DWINGS_Guarantees_{_offlineFirstService?.CurrentCountryId}";
            return await CacheService.Instance.GetOrLoadAsync(cacheKey, async () =>
            {
                if (_dwingsService == null) _dwingsService = new DwingsService(_offlineFirstService);
                return await _dwingsService.GetGuaranteesAsync().ConfigureAwait(false);
            }).ConfigureAwait(false);
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

            // Filter by Action = Trigger and only RECEIVABLE account side
            var country = _countries.ContainsKey(countryId) ? _countries[countryId] : null;
            var receivableId = country?.CNT_AmbreReceivable;

            var query = $@"SELECT r.* FROM (T_Reconciliation AS r
                             INNER JOIN {ambreJoin} ON r.ID = a.ID)
                           WHERE r.DeleteDate IS NULL AND r.Action = ? AND a.Account_ID = ?
                           ORDER BY r.LastModified DESC";

            return await ExecuteQueryAsync<Reconciliation>(query, _connectionString, (int)ActionType.Trigger, receivableId);
        }

        // JSON filter preset is defined in Domain/Filters/FilterPreset

        /// <summary>
        /// Clears the reconciliation view cache to force fresh data reload
        /// </summary>
        public void ClearViewCache()
        {
            try
            {
                _recoViewCache.Clear();
                
                // Reset DWINGS cache initialization flag (will be reinitialized on next load)
                lock (_dwingsCacheLock)
                {
                    _dwingsCachesInitialized = false;
                }
                ReconciliationViewData.ClearDwingsCaches();
                
                System.Diagnostics.Debug.WriteLine("ReconciliationService: View cache cleared");
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"ReconciliationService: Error clearing cache: {ex.Message}");
            }
        }

        /// <summary>
        /// Récupère les données jointes Ambre + Réconciliation (Live only - DeleteDate IS NULL)
        /// REMOVED: dashboardOnly parameter - was incomplete and prevented cache reuse
        /// </summary>
        public async Task<List<ReconciliationViewData>> GetReconciliationViewAsync(string countryId, string filterSql = null)
        {
            return await GetReconciliationViewAsync(countryId, filterSql, includeDeleted: false).ConfigureAwait(false);
        }
        
        public List<ReconciliationViewData> TryGetCachedReconciliationView(string countryId, string filterSql, bool includeDeleted = false)
        {
            var key = $"{countryId ?? string.Empty}|{includeDeleted}|{NormalizeFilterForCache(filterSql)}";
            if (_recoViewDataCache.TryGetValue(key, out var list)) return list;
            return null;
        }
        
        /// <summary>
        /// Récupère les données jointes Ambre + Réconciliation avec option d'inclure les lignes supprimées
        /// Used by HomePage for historical charts (Deletion Delay, New vs Deleted Daily)
        /// </summary>
        public async Task<List<ReconciliationViewData>> GetReconciliationViewAsync(string countryId, string filterSql, bool includeDeleted)
        {
            // CRITICAL: Always ensure DWINGS caches are initialized for lazy loading
            await EnsureDwingsCachesInitializedAsync().ConfigureAwait(false);
            
            var key = $"{countryId ?? string.Empty}|{includeDeleted}|{NormalizeFilterForCache(filterSql)}";
            if (_recoViewCache.TryGetValue(key, out var existing))
            {
                var cached = await existing.Value.ConfigureAwait(false);
                
                // CRITICAL: Re-apply enrichments for cached data (linking may have been lost)
                // This ensures DWINGS_InvoiceID, MissingAmount, etc. are always calculated
                await ReapplyEnrichmentsAsync(cached, countryId).ConfigureAwait(false);
                
                return cached;
            }
            var lazy = new Lazy<Task<List<ReconciliationViewData>>>(() => BuildReconciliationViewAsyncCore(countryId, filterSql, includeDeleted, key));
            var entry = _recoViewCache.GetOrAdd(key, lazy);
            var result = await entry.Value.ConfigureAwait(false);
            return result;
        }
        
        /// <summary>
        /// Re-applies critical enrichments to a list of ReconciliationViewData
        /// Needed because DWINGS caches may have been cleared between cache creation and retrieval
        /// Also used for preloaded data to ensure enrichments are always applied
        /// </summary>
        public async Task ReapplyEnrichmentsToListAsync(List<ReconciliationViewData> list, string countryId)
        {
            await ReapplyEnrichmentsAsync(list, countryId).ConfigureAwait(false);
        }
        
        /// <summary>
        /// Re-applies critical enrichments to cached data (internal implementation)
        /// OPTIMIZATION: Skip if data is already fully enriched (has pre-calculated DWINGS properties)
        /// </summary>
        private async Task ReapplyEnrichmentsAsync(List<ReconciliationViewData> list, string countryId)
        {
            if (list == null || list.Count == 0) return;
            
            // OPTIMIZATION: Check if data is already enriched by sampling first row
            // If I_RECEIVER_NAME is populated, assume all DWINGS properties are already calculated
            if (list.Count > 0 && !string.IsNullOrWhiteSpace(list[0].I_RECEIVER_NAME))
            {
                // Data is already fully enriched, skip expensive re-enrichment
                return;
            }
            
            try
            {
                // Re-link DWINGS invoices (may have been cleared)
                var invoices = await GetDwingsInvoicesAsync().ConfigureAwait(false);
                ReconciliationViewEnricher.EnrichWithDwingsInvoices(list, invoices);
                
                // PRE-CALCULATE all DWINGS properties (same as initial load)
                // NOTE: INVOICE_ID is NOT unique - use GroupBy and take first match
                var invoiceDict = invoices?
                    .Where(i => !string.IsNullOrWhiteSpace(i.INVOICE_ID))
                    .GroupBy(i => i.INVOICE_ID, StringComparer.OrdinalIgnoreCase)
                    .ToDictionary(g => g.Key, g => g.First(), StringComparer.OrdinalIgnoreCase)
                    ?? new Dictionary<string, DwingsInvoiceDto>();
                    
                var guarantees = await GetDwingsGuaranteesAsync().ConfigureAwait(false);
                var guaranteeDict = guarantees?
                    .Where(g => !string.IsNullOrWhiteSpace(g.GUARANTEE_ID))
                    .GroupBy(g => g.GUARANTEE_ID, StringComparer.OrdinalIgnoreCase)
                    .ToDictionary(g => g.Key, g => g.First(), StringComparer.OrdinalIgnoreCase)
                    ?? new Dictionary<string, DwingsGuaranteeDto>();
                
                foreach (var row in list)
                {
                    // Populate invoice properties
                    if (!string.IsNullOrWhiteSpace(row.DWINGS_InvoiceID) && invoiceDict.TryGetValue(row.DWINGS_InvoiceID, out var invoice))
                    {
                        row.PopulateInvoiceProperties(invoice);
                    }

                    // Do NOT heuristically backfill DWINGS_GuaranteeID here. Only populate G_* when an explicit link exists.
                    if (!string.IsNullOrWhiteSpace(row.DWINGS_GuaranteeID) && guaranteeDict.TryGetValue(row.DWINGS_GuaranteeID, out var guarantee))
                    {
                        row.PopulateGuaranteeProperties(guarantee);
                    }
                }
                
                // Recalculate MissingAmount
                var country = _countries?.TryGetValue(countryId, out var c) == true ? c : null;
                if (country != null)
                {
                    ReconciliationViewEnricher.CalculateMissingAmounts(list, country.CNT_AmbreReceivable, country.CNT_AmbrePivot);
                }
            }
            catch { /* best-effort */ }
        }
        
        private bool _dwingsCachesInitialized = false;
        private readonly object _dwingsCacheLock = new object();
        
        /// <summary>
        /// Ensures DWINGS caches are initialized once per country session
        /// Called before returning cached data to guarantee lazy loading works
        /// Public to allow ReconciliationView to initialize caches for preloaded data
        /// </summary>
        public async Task EnsureDwingsCachesInitializedAsync()
        {
            if (_dwingsCachesInitialized) return;
            
            lock (_dwingsCacheLock)
            {
                if (_dwingsCachesInitialized) return;
                _dwingsCachesInitialized = true;
            }
            
            try
            {
                var invoices = await GetDwingsInvoicesAsync().ConfigureAwait(false);
                var guarantees = await GetDwingsGuaranteesAsync().ConfigureAwait(false);
                ReconciliationViewData.InitializeDwingsCaches(invoices, guarantees);
            }
            catch { /* best-effort */ }
        }

        private async Task<List<ReconciliationViewData>> BuildReconciliationViewAsyncCore(string countryId, string filterSql, bool includeDeleted, string cacheKey)
        {
            var swBuild = Stopwatch.StartNew();
            string dwPath = _offlineFirstService?.GetLocalDWDatabasePath();
            string ambrePath = _offlineFirstService?.GetLocalAmbreDatabasePath(countryId);
            string dwEsc = string.IsNullOrEmpty(dwPath) ? null : dwPath.Replace("'", "''");
            string ambreEsc = string.IsNullOrEmpty(ambrePath) ? null : ambrePath.Replace("'", "''");

            // Detect PotentialDuplicates flag from optional JSON comment prefix (centralized helper)
            bool dupOnly = FilterSqlHelper.TryExtractPotentialDuplicatesFlag(filterSql);

            // Build the base query via centralized builder
            string query = ReconciliationViewQueryBuilder.Build(dwEsc, ambreEsc, filterSql);

            // CRITICAL: Filter out deleted records (unless includeDeleted=true for historical charts)
            if (!includeDeleted)
            {
                query += " AND a.DeleteDate IS NULL AND (r.DeleteDate IS NULL)";
            }

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

            // OPTIMIZED: Pre-calculate all DWINGS properties once during load
            try
            {
                var invoices = await GetDwingsInvoicesAsync().ConfigureAwait(false);
                var guarantees = await GetDwingsGuaranteesAsync().ConfigureAwait(false);
                
                // Initialize static caches in ReconciliationViewData
                ReconciliationViewData.InitializeDwingsCaches(invoices, guarantees);
                
                // Link rows to DWINGS invoices (sets DWINGS_InvoiceID only)
                ReconciliationViewEnricher.EnrichWithDwingsInvoices(list, invoices);
                
                // PRE-CALCULATE all DWINGS properties for each row (eliminates lazy loading during scroll)
                // NOTE: INVOICE_ID is NOT unique - use GroupBy and take first match
                var invoiceDict = invoices?
                    .Where(i => !string.IsNullOrWhiteSpace(i.INVOICE_ID))
                    .GroupBy(i => i.INVOICE_ID, StringComparer.OrdinalIgnoreCase)
                    .ToDictionary(g => g.Key, g => g.First(), StringComparer.OrdinalIgnoreCase)
                    ?? new Dictionary<string, DwingsInvoiceDto>();
                    
                var guaranteeDict = guarantees?
                    .Where(g => !string.IsNullOrWhiteSpace(g.GUARANTEE_ID))
                    .GroupBy(g => g.GUARANTEE_ID, StringComparer.OrdinalIgnoreCase)
                    .ToDictionary(g => g.Key, g => g.First(), StringComparer.OrdinalIgnoreCase)
                    ?? new Dictionary<string, DwingsGuaranteeDto>();
                
                foreach (var row in list)
                {
                    // Populate invoice properties
                    if (!string.IsNullOrWhiteSpace(row.DWINGS_InvoiceID) && invoiceDict.TryGetValue(row.DWINGS_InvoiceID, out var invoice))
                    {
                        row.PopulateInvoiceProperties(invoice);
                    }
                    
                    
                    
                    if (!string.IsNullOrWhiteSpace(row.DWINGS_GuaranteeID) && guaranteeDict.TryGetValue(row.DWINGS_GuaranteeID, out var guarantee))
                    {
                        row.PopulateGuaranteeProperties(guarantee);
                    }
                }
            }
            catch { /* best-effort enrichment */ }
            
            // Calculate missing amounts for grouped lines (Receivable vs Pivot)
            try
            {
                var country = _countries?.TryGetValue(countryId, out var c) == true ? c : null;
                if (country != null)
                {
                    ReconciliationViewEnricher.CalculateMissingAmounts(list, country.CNT_AmbreReceivable, country.CNT_AmbrePivot);
                }
            }
            catch { /* best-effort calculation */ }

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
                    // BUT only if modified by SYSTEM/AUTO (not by user manual edit)
                    if (row.Reco_LastModified.HasValue && row.Reco_LastModified.Value.Date == today)
                    {
                        if (!row.Reco_CreationDate.HasValue || row.Reco_LastModified.Value > row.Reco_CreationDate.Value)
                        {
                            // Only mark as Updated if ModifiedBy is empty/null (import/rule) or equals current user during import
                            // User manual edits should NOT trigger the "U" indicator
                            // The "U" should only appear for automatic changes (import, rules)
                            // For now, we rely on the SyncAndTimers.cs logic which sets IsUpdated after import
                            // So we don't set it here to avoid showing "U" for user edits
                            // row.IsUpdated = true;  // REMOVED - handled by SyncAndTimers after import
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

                // Group by DWINGS_InvoiceID first
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
                
                // Group by InternalInvoiceReference (INDEPENDENTLY - can coexist with DWINGS_InvoiceID)
                // This allows a line to belong to multiple groups (BGI group + Internal ref group)
                var byInternal = list.Where(r => !string.IsNullOrWhiteSpace(r.InternalInvoiceReference))
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

            // OPTIMIZED: Link guarantee IDs heuristically (G_* fields are now lazy-loaded)
            try
            {
                
                
                swExec.Stop();

                // Store materialized list for incremental updates
                _recoViewDataCache[cacheKey] = list;
                return list;
            }
            catch { /* best-effort linking */ }

            return null;
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
        /// DTO for status counts used in TodoCard display
        /// </summary>
        public class StatusCountsDto
        {
            public int NewCount { get; set; }
            public int UpdatedCount { get; set; }
            public int NotLinkedCount { get; set; }
            public int NotGroupedCount { get; set; }
            public int DiscrepancyCount { get; set; }
            public int BalancedCount { get; set; }
        }

        /// <summary>
        /// Lightweight DTO for status count calculation (loads only essential columns)
        /// </summary>
        private class StatusCountRow
        {
            public string ID { get; set; }
            public string Account_ID { get; set; }
            public double SignedAmount { get; set; }
            public string DWINGS_InvoiceID { get; set; }
            public string InternalInvoiceReference { get; set; }
            public DateTime? Reco_CreationDate { get; set; }
            public DateTime? Reco_LastModified { get; set; }
            public string Reco_ModifiedBy { get; set; }
            public bool IsNewlyAdded { get; set; }
            public bool IsUpdated { get; set; }
            public bool IsMatchedAcrossAccounts { get; set; }
            public double? MissingAmount { get; set; }
        }

        /// <summary>
        /// Returns status indicator counts for a filter (optimized with minimal data loading)
        /// OPTIMIZATION: Loads only essential columns instead of full ReconciliationViewData
        /// </summary>
        public async Task<StatusCountsDto> GetStatusCountsAsync(string countryId, string filterSql = null)
        {
            // Cache status counts
            var cacheKey = $"StatusCounts_{countryId}_{NormalizeFilterForCache(filterSql)}";
            return await CacheService.Instance.GetOrLoadAsync(cacheKey, async () =>
            {
                try
                {
                    string dwPath = _offlineFirstService?.GetLocalDWDatabasePath();
                    string ambrePath = _offlineFirstService?.GetLocalAmbreDatabasePath(countryId);
                    string dwEsc = string.IsNullOrEmpty(dwPath) ? null : dwPath.Replace("'", "''");
                    string ambreEsc = string.IsNullOrEmpty(ambrePath) ? null : ambrePath.Replace("'", "''");

                    // Build minimal query with only columns needed for status calculation
                    string ambreJoin = string.IsNullOrEmpty(ambreEsc) ? "T_Data_Ambre AS a" : $"(SELECT * FROM [{ambreEsc}].T_Data_Ambre) AS a";
                    
                    string query = $@"
                        SELECT 
                            a.ID,
                            a.Account_ID,
                            a.SignedAmount,
                            r.DWINGS_InvoiceID,
                            r.InternalInvoiceReference,
                            r.CreationDate AS Reco_CreationDate,
                            r.LastModified AS Reco_LastModified,
                            r.ModifiedBy AS Reco_ModifiedBy
                        FROM {ambreJoin}
                        LEFT JOIN T_Reconciliation AS r ON a.ID = r.ID
                        WHERE a.DeleteDate IS NULL AND (r.DeleteDate IS NULL)";

                    var predicate = FilterSqlHelper.ExtractNormalizedPredicate(filterSql);
                    if (!string.IsNullOrEmpty(predicate))
                    {
                        query += $" AND ({predicate})";
                    }

                    var rows = await ExecuteQueryAsync<StatusCountRow>(query).ConfigureAwait(false);
                    if (rows == null || rows.Count == 0)
                        return new StatusCountsDto();

                    // Enrich rows with computed flags
                    var today = DateTime.Today;
                    var country = _countries?.TryGetValue(countryId, out var c) == true ? c : null;
                    string pivotId = country?.CNT_AmbrePivot?.Trim();
                    string receivableId = country?.CNT_AmbreReceivable?.Trim();

                    // Set IsNewlyAdded and IsUpdated flags
                    foreach (var row in rows)
                    {
                        // New if reconciliation CreationDate is today
                        if (row.Reco_CreationDate.HasValue && row.Reco_CreationDate.Value.Date == today)
                            row.IsNewlyAdded = true;
                        
                        // Updated if LastModified is today AND differs from CreationDate
                        // AND ModifiedBy is empty/null (automatic update, not user edit)
                        if (row.Reco_LastModified.HasValue && row.Reco_LastModified.Value.Date == today)
                        {
                            if (!row.Reco_CreationDate.HasValue || row.Reco_LastModified.Value > row.Reco_CreationDate.Value)
                            {
                                // Only mark as Updated if ModifiedBy is empty/null (import/rule)
                                // User manual edits should NOT trigger the "U" indicator
                                if (string.IsNullOrWhiteSpace(row.Reco_ModifiedBy))
                                    row.IsUpdated = true;
                            }
                        }
                    }

                    // Calculate IsMatchedAcrossAccounts by grouping
                    if (!string.IsNullOrWhiteSpace(pivotId) && !string.IsNullOrWhiteSpace(receivableId))
                    {
                        // Set AccountSide helper (not stored, just for grouping logic)
                        var accountSides = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                        foreach (var row in rows)
                        {
                            var acc = row.Account_ID?.Trim();
                            if (string.Equals(acc, pivotId, StringComparison.OrdinalIgnoreCase))
                                accountSides[row.ID] = "P";
                            else if (string.Equals(acc, receivableId, StringComparison.OrdinalIgnoreCase))
                                accountSides[row.ID] = "R";
                        }

                        // Group by DWINGS_InvoiceID
                        var byInvoice = rows.Where(r => !string.IsNullOrWhiteSpace(r.DWINGS_InvoiceID))
                                            .GroupBy(r => r.DWINGS_InvoiceID, StringComparer.OrdinalIgnoreCase);
                        foreach (var g in byInvoice)
                        {
                            bool hasP = g.Any(x => accountSides.TryGetValue(x.ID, out var side) && side == "P");
                            bool hasR = g.Any(x => accountSides.TryGetValue(x.ID, out var side) && side == "R");
                            if (hasP && hasR)
                            {
                                foreach (var row in g) row.IsMatchedAcrossAccounts = true;
                            }
                        }

                        // Group by InternalInvoiceReference
                        var byInternal = rows.Where(r => !string.IsNullOrWhiteSpace(r.InternalInvoiceReference))
                                             .GroupBy(r => r.InternalInvoiceReference, StringComparer.OrdinalIgnoreCase);
                        foreach (var g in byInternal)
                        {
                            bool hasP = g.Any(x => accountSides.TryGetValue(x.ID, out var side) && side == "P");
                            bool hasR = g.Any(x => accountSides.TryGetValue(x.ID, out var side) && side == "R");
                            if (hasP && hasR)
                            {
                                foreach (var row in g) row.IsMatchedAcrossAccounts = true;
                            }
                        }

                        // Calculate MissingAmount for matched groups
                        var groupsToCalculate = rows
                            .Where(r => r.IsMatchedAcrossAccounts && 
                                       (!string.IsNullOrWhiteSpace(r.DWINGS_InvoiceID) || !string.IsNullOrWhiteSpace(r.InternalInvoiceReference)))
                            .GroupBy(r => 
                            {
                                var key = !string.IsNullOrWhiteSpace(r.DWINGS_InvoiceID)
                                    ? r.DWINGS_InvoiceID.Trim().ToUpperInvariant()
                                    : r.InternalInvoiceReference?.Trim().ToUpperInvariant();
                                return key;
                            })
                            .Where(g => !string.IsNullOrWhiteSpace(g.Key));

                        foreach (var group in groupsToCalculate)
                        {
                            var receivableLines = group.Where(r => r.Account_ID == receivableId).ToList();
                            var pivotLines = group.Where(r => r.Account_ID == pivotId).ToList();

                            if (receivableLines.Count > 0 && pivotLines.Count > 0)
                            {
                                var receivableTotal = receivableLines.Sum(r => r.SignedAmount);
                                var pivotTotal = pivotLines.Sum(r => r.SignedAmount);
                                var missing = receivableTotal + pivotTotal;

                                foreach (var r in receivableLines) r.MissingAmount = missing;
                                foreach (var p in pivotLines) p.MissingAmount = missing;
                            }
                        }
                    }

                    // Calculate status counts based on status color logic
                    var result = new StatusCountsDto
                    {
                        NewCount = rows.Count(r => r.IsNewlyAdded),
                        UpdatedCount = rows.Count(r => r.IsUpdated),
                        NotLinkedCount = rows.Count(r => 
                            string.IsNullOrWhiteSpace(r.DWINGS_InvoiceID) && 
                            string.IsNullOrWhiteSpace(r.InternalInvoiceReference)), // Red: No DWINGS link
                        NotGroupedCount = rows.Count(r => 
                            (!string.IsNullOrWhiteSpace(r.DWINGS_InvoiceID) || !string.IsNullOrWhiteSpace(r.InternalInvoiceReference)) &&
                            !r.IsMatchedAcrossAccounts), // Orange: Has link but not grouped
                        DiscrepancyCount = rows.Count(r => 
                            r.IsMatchedAcrossAccounts && 
                            r.MissingAmount.HasValue && 
                            r.MissingAmount.Value != 0), // Yellow/Amber: Grouped but has discrepancy
                        BalancedCount = rows.Count(r => 
                            r.IsMatchedAcrossAccounts && 
                            (!r.MissingAmount.HasValue || r.MissingAmount.Value == 0)) // Green: Balanced
                    };

                    return result;
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine($"GetStatusCountsAsync error: {ex.Message}");
                    return new StatusCountsDto();
                }
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// Returns the number of Live reconciliation rows matching the provided backend filter for the given country.
        /// Live means a.DeleteDate IS NULL and r.DeleteDate IS NULL.
        /// The filterSql may optionally include a JSON preset header; only the predicate is applied.
        /// OPTIMIZED: Cached based on countryId + filterSql (AMBRE data rarely changes)
        /// </summary>
        public async Task<int> GetReconciliationCountAsync(string countryId, string filterSql = null)
        {
            // OPTIMIZATION: Cache reconciliation counts (only changes on AMBRE import or status updates)
            var cacheKey = $"RecoCount_{countryId}_{NormalizeFilterForCache(filterSql)}";
            return await CacheService.Instance.GetOrLoadAsync(cacheKey, async () =>
            {
                try
                {
                    string dwPath = _offlineFirstService?.GetLocalDWDatabasePath();
                    string ambrePath = _offlineFirstService?.GetLocalAmbreDatabasePath(countryId);
                    string dwEsc = string.IsNullOrEmpty(dwPath) ? null : dwPath.Replace("'", "''");
                    string ambreEsc = string.IsNullOrEmpty(ambrePath) ? null : ambrePath.Replace("'", "''");

                    // Detect duplicates-only flag from optional JSON header
                    bool dupOnly = FilterSqlHelper.TryExtractPotentialDuplicatesFlag(filterSql);

                    // Base SELECT ... WHERE 1=1 with joins and dup subquery
                    string query = ReconciliationViewQueryBuilder.Build(dwEsc, ambreEsc, filterSql);

                    // Always enforce Live scope
                    query += " AND a.DeleteDate IS NULL AND (r.DeleteDate IS NULL)";

                    var predicate = FilterSqlHelper.ExtractNormalizedPredicate(filterSql);
                    if (!string.IsNullOrEmpty(predicate))
                    {
                        query += $" AND ({predicate})";
                    }

                    if (dupOnly)
                    {
                        query += " AND (dup.DupCount) > 1";
                    }

                    var countSql = $"SELECT COUNT(*) FROM ({query}) AS q";

                    using (var connection = new OleDbConnection(_connectionString))
                    {
                        await connection.OpenAsync().ConfigureAwait(false);
                        using (var cmd = new OleDbCommand(countSql, connection))
                        {
                            var obj = await cmd.ExecuteScalarAsync().ConfigureAwait(false);
                            if (obj == null || obj == DBNull.Value) return 0;
                            int n;
                            return int.TryParse(Convert.ToString(obj), out n) ? n : 0;
                        }
                    }
                }
                catch
                {
                    return 0;
                }
            }, TimeSpan.FromHours(2)).ConfigureAwait(false); // Cache for 2 hours (only changes on AMBRE import or status updates)
        }

        // REMOVED: ComputeAutoAction - Legacy method replaced by AmbreReconciliationUpdater.ApplyTruthTableRulesAsync

        /// <summary>
        /// Preview rules for a single reconciliation ID (Edit scope) without applying them.
        /// Used by UI to show what rules would apply before saving.
        /// </summary>
        public async Task<RuleEvaluationResult> PreviewRulesForEditAsync(string id)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(id)) return null;

                var currentCountryId = _offlineFirstService?.CurrentCountryId;
                if (string.IsNullOrWhiteSpace(currentCountryId) || _rulesEngine == null) return null;

                Country country = null;
                if (_countries != null && !_countries.TryGetValue(currentCountryId, out country)) return null;
                if (country == null) return null;

                var amb = await GetAmbreRowByIdAsync(currentCountryId, id).ConfigureAwait(false);
                if (amb == null) return null;

                var reconciliation = await GetOrCreateReconciliationAsync(id).ConfigureAwait(false);
                if (reconciliation == null) return null;

                bool isPivot = amb.IsPivotAccount(country.CNT_AmbrePivot);
                var ctx = await BuildRuleContextAsync(amb, reconciliation, country, currentCountryId, isPivot).ConfigureAwait(false);
                var res = await _rulesEngine.EvaluateAsync(ctx, RuleScope.Edit).ConfigureAwait(false);

                return res;
            }
            catch
            {
                return null;
            }
        }

        /// <summary>
        /// Exécute immédiatement les règles (scope Edit) pour les IDs donnés.
        /// N'applique que les règles en Auto-apply; les autres peuvent ajouter un message.
        /// </summary>
        public async Task<int> ApplyRulesNowAsync(IEnumerable<string> ids)
        {
            try
            {
                if (ids == null) return 0;
                // Ensure latest rules are loaded now
                try { _rulesEngine?.InvalidateCache(); } catch { }
                var distinct = ids.Where(id => !string.IsNullOrWhiteSpace(id))
                                  .Distinct(StringComparer.OrdinalIgnoreCase)
                                  .ToList();
                if (distinct.Count == 0) return 0;

                var recos = new List<Reconciliation>(distinct.Count);
                var currentCountryId = _offlineFirstService?.CurrentCountryId;
                Country country = null;
                if (!string.IsNullOrWhiteSpace(currentCountryId) && _countries != null)
                    _countries.TryGetValue(currentCountryId, out country);

                foreach (var id in distinct)
                {
                    // Skip archived rows (IsDeleted == true on Ambre row)
                    DataAmbre amb = null;
                    try
                    {
                        if (!string.IsNullOrWhiteSpace(currentCountryId))
                        {
                            amb = await GetAmbreRowByIdAsync(currentCountryId, id).ConfigureAwait(false);
                            if (amb == null || amb.IsDeleted) continue;
                        }
                    }
                    catch { }

                    var r = await GetOrCreateReconciliationAsync(id).ConfigureAwait(false);
                    if (r == null) continue;

                    // Evaluate and apply outputs in EDIT scope unconditionally when running now
                    try
                    {
                        if (country != null && _rulesEngine != null && amb != null)
                        {
                            bool isPivot = amb.IsPivotAccount(country.CNT_AmbrePivot);
                            var ctx = await BuildRuleContextAsync(amb, r, country, currentCountryId, isPivot).ConfigureAwait(false);
                            var res = await _rulesEngine.EvaluateAsync(ctx, RuleScope.Edit).ConfigureAwait(false);
                            if (res != null && res.Rule != null && res.Rule.AutoApply)
                            {
                                if (res.NewActionIdSelf.HasValue) { r.Action = res.NewActionIdSelf.Value; EnsureActionDefaults(r); }
                                if (res.NewKpiIdSelf.HasValue) r.KPI = res.NewKpiIdSelf.Value;
                                if (res.NewIncidentTypeIdSelf.HasValue) r.IncidentType = res.NewIncidentTypeIdSelf.Value;
                                if (res.NewRiskyItemSelf.HasValue) r.RiskyItem = res.NewRiskyItemSelf.Value;
                                if (res.NewReasonNonRiskyIdSelf.HasValue) r.ReasonNonRisky = res.NewReasonNonRiskyIdSelf.Value;
                                if (res.NewToRemindSelf.HasValue) r.ToRemind = res.NewToRemindSelf.Value;
                                if (res.NewToRemindDaysSelf.HasValue)
                                {
                                    try { r.ToRemindDate = DateTime.Today.AddDays(res.NewToRemindDaysSelf.Value); } catch { }
                                }
                                if (res.NewFirstClaimTodaySelf == true)
                                {
                                    try
                                    {
                                        if (r.FirstClaimDate.HasValue)
                                            r.LastClaimDate = DateTime.Today;
                                        else
                                            r.FirstClaimDate = DateTime.Today;
                                    }
                                    catch { }
                                }

                                if (!string.IsNullOrWhiteSpace(res.UserMessage))
                                {
                                    try
                                    {
                                        var prefix = $"[{DateTime.Now:yyyy-MM-dd HH:mm}] {_currentUser}: ";
                                        var msg = prefix + $"[Rule {res.Rule.RuleId ?? "(unnamed)"}] {res.UserMessage}";
                                        if (string.IsNullOrWhiteSpace(r.Comments))
                                            r.Comments = msg;
                                        else if (!r.Comments.Contains(msg))
                                            r.Comments = r.Comments + Environment.NewLine + msg;
                                    }
                                    catch { }
                                }

                                // File log + UI event
                                try
                                {
                                    var outs = new List<string>();
                                    if (res.NewActionIdSelf.HasValue) outs.Add($"Action={res.NewActionIdSelf.Value}");
                                    if (res.NewKpiIdSelf.HasValue) outs.Add($"KPI={res.NewKpiIdSelf.Value}");
                                    if (res.NewIncidentTypeIdSelf.HasValue) outs.Add($"IncidentType={res.NewIncidentTypeIdSelf.Value}");
                                    if (res.NewRiskyItemSelf.HasValue) outs.Add($"RiskyItem={res.NewRiskyItemSelf.Value}");
                                    if (res.NewReasonNonRiskyIdSelf.HasValue) outs.Add($"ReasonNonRisky={res.NewReasonNonRiskyIdSelf.Value}");
                                    if (res.NewToRemindSelf.HasValue) outs.Add($"ToRemind={res.NewToRemindSelf.Value}");
                                    if (res.NewToRemindDaysSelf.HasValue) outs.Add($"ToRemindDays={res.NewToRemindDaysSelf.Value}");
                                    if (res.NewFirstClaimTodaySelf == true) outs.Add("FirstClaimDate=Today");
                                    var outsStr = string.Join("; ", outs);
                                    LogHelper.WriteRuleApplied("run-now", currentCountryId, r.ID, res.Rule.RuleId, outsStr, res.UserMessage);
                                    RaiseRuleApplied("run-now", currentCountryId, r.ID, res.Rule.RuleId, outsStr, res.UserMessage);
                                }
                                catch { }
                            }
                        }
                    }
                    catch { }

                    recos.Add(r);
                }
                if (recos.Count == 0) return 0;

                await SaveReconciliationsAsync(recos).ConfigureAwait(false);
                return recos.Count;
            }
            catch { return 0; }
        }

        private void EnsureActionDefaults(Reconciliation r)
        {
            try
            {
                if (r == null) return;
                var all = _offlineFirstService?.UserFields;
                bool isNa = !r.Action.HasValue || UserFieldUpdateService.IsActionNA(r.Action, all);
                if (isNa)
                {
                    // FIX: N/A action should be marked as DONE, not null
                    r.ActionStatus = true;
                    r.ActionDate = DateTime.Now;
                }
                else
                {
                    if (!r.ActionStatus.HasValue) r.ActionStatus = false; // PENDING
                    // FIX: ALWAYS update ActionDate when Action changes
                    r.ActionDate = DateTime.Now;
                }
            }
            catch { }
        }
        #endregion

        

        

        #region Automatic Rules and Actions

        // REMOVED: ApplyAutomaticRulesAsync - Replaced by AmbreReconciliationUpdater.ApplyTruthTableRulesAsync during import

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
                        // Créer ou mettre à jour les réconciliations (table de vérité)
                        await CreateMatchingReconciliationsAsync(country, receivableLine, matchingPivotLines);
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
        private async Task CreateMatchingReconciliationsAsync(Country country, DataAmbre receivableLine, List<DataAmbre> pivotLines)
        {
            var receivableReco = await GetOrCreateReconciliationAsync(receivableLine.ID).ConfigureAwait(false);

            // Get pivot reconciliations
            var pivotTasks = pivotLines.Select(p => GetOrCreateReconciliationAsync(p.ID));
            var pivotReconciliations = await Task.WhenAll(pivotTasks).ConfigureAwait(false);

            // Add informational comments (not rule logic)
            try
            {
                receivableReco.Comments = string.IsNullOrWhiteSpace(receivableReco.Comments)
                    ? $"Auto-matched with {pivotLines.Count} pivot line(s)"
                    : ($"Auto-matched with {pivotLines.Count} pivot line(s)" + Environment.NewLine + receivableReco.Comments);
                foreach (var pivotReco in pivotReconciliations)
                {
                    pivotReco.Comments = string.IsNullOrWhiteSpace(pivotReco.Comments)
                        ? $"Auto-matched with receivable line {receivableLine.ID}"
                        : ($"Auto-matched with receivable line {receivableLine.ID}" + Environment.NewLine + pivotReco.Comments);
                }
            }
            catch { }

            await SaveReconciliationsAsync(new[] { receivableReco }.Concat(pivotReconciliations)).ConfigureAwait(false);
        }

        /// <summary>
        /// Applique la règle spéciale MANUAL_OUTGOING: trouve les paires de lignes pivot avec même guarantee ID
        /// et montants qui somment à 0, puis les marque comme MATCH / PAID BUT NOT RECONCILED.
        /// </summary>
        public async Task<int> ApplyManualOutgoingRuleAsync(string countryId)
        {
            var timer = System.Diagnostics.Stopwatch.StartNew();
            try
            {
                var country = _countries.ContainsKey(countryId) ? _countries[countryId] : null;
                if (country == null) return 0;

                // Get Action and KPI IDs for "MATCH" and "PAID BUT NOT RECONCILED"
                var allUserFields = _offlineFirstService?.UserFields;
                if (allUserFields == null) return 0;

                var toMatchAction = allUserFields.FirstOrDefault(uf => 
                    string.Equals(uf.USR_Category, "Action", StringComparison.OrdinalIgnoreCase) &&
                    string.Equals(uf.USR_FieldName, "MATCH", StringComparison.OrdinalIgnoreCase));
                var payButNotReconciledKpi = allUserFields.FirstOrDefault(uf => 
                    string.Equals(uf.USR_Category, "KPI", StringComparison.OrdinalIgnoreCase) &&
                    string.Equals(uf.USR_FieldName, "PAID BUT NOT RECONCILED", StringComparison.OrdinalIgnoreCase));

                if (toMatchAction == null || payButNotReconciledKpi == null)
                {
                    LogManager.Warning("MANUAL_OUTGOING rule: Required Action or KPI not found (MATCH or PAID BUT NOT RECONCILED)");
                    return 0;
                }

                int actionId = toMatchAction.USR_ID;
                int kpiId = payButNotReconciledKpi.USR_ID;

                // Load PIVOT lines only
                var loadTimer = System.Diagnostics.Stopwatch.StartNew();
                var ambreData = await GetAmbreDataAsync(countryId);
                var pivotLines = ambreData.Where(d => 
                    d.IsPivotAccount(country.CNT_AmbrePivot) && 
                    !d.IsDeleted
                ).ToList();
                loadTimer.Stop();
                LogManager.Info($"[PERF] MANUAL_OUTGOING rule: Loaded {pivotLines.Count} pivot lines in {loadTimer.ElapsedMilliseconds}ms");

                int matchCount = 0;

                // Pre-load all reconciliations for PIVOT lines
                var reconciliations = new Dictionary<string, Reconciliation>(StringComparer.OrdinalIgnoreCase);
                foreach (var line in pivotLines)
                {
                    var reco = await GetOrCreateReconciliationAsync(line.ID).ConfigureAwait(false);
                    if (reco != null)
                    {
                        reconciliations[line.ID] = reco;
                    }
                }

                // SIMPLIFIED RULE: Find pairs in PIVOT with same guarantee ID and amounts that sum to 0
                // Group by guarantee ID
                var linesByGuarantee = new Dictionary<string, List<DataAmbre>>(StringComparer.OrdinalIgnoreCase);
                foreach (var line in pivotLines)
                {
                    if (!reconciliations.TryGetValue(line.ID, out var reco) || reco == null)
                        continue;
                    
                    var guaranteeId = reco.DWINGS_GuaranteeID?.Trim();
                    if (string.IsNullOrWhiteSpace(guaranteeId))
                        continue;
                    
                    if (!linesByGuarantee.ContainsKey(guaranteeId))
                        linesByGuarantee[guaranteeId] = new List<DataAmbre>();
                    
                    linesByGuarantee[guaranteeId].Add(line);
                }

                // For each guarantee group, find pairs that sum to 0
                foreach (var kvp in linesByGuarantee)
                {
                    var guaranteeId = kvp.Key;
                    var lines = kvp.Value;
                    
                    // Need at least 2 lines
                    if (lines.Count < 2)
                        continue;
                    
                    // Find all pairs that sum to 0
                    for (int i = 0; i < lines.Count; i++)
                    {
                        for (int j = i + 1; j < lines.Count; j++)
                        {
                            var line1 = lines[i];
                            var line2 = lines[j];
                            
                            // Check if amounts sum to 0 (within 0.01 tolerance)
                            if (Math.Abs(line1.SignedAmount + line2.SignedAmount) >= 0.01m)
                                continue;
                            
                            // Get reconciliations
                            if (!reconciliations.TryGetValue(line1.ID, out var reco1) || reco1 == null)
                                continue;
                            if (!reconciliations.TryGetValue(line2.ID, out var reco2) || reco2 == null)
                                continue;
                            
                            // Apply rule: set both to MATCH + PAID BUT NOT RECONCILED
                            reco1.Action = actionId;
                            reco1.KPI = kpiId;
                            EnsureActionDefaults(reco1);
                            
                            reco2.Action = actionId;
                            reco2.KPI = kpiId;
                            EnsureActionDefaults(reco2);
                            
                            // Add comment: Same guarantee Pair detected in pivot => to match in Ambre
                            var timestamp = $"[{DateTime.Now:yyyy-MM-dd HH:mm}] {_currentUser}: ";
                            var msg = $"Same guarantee Pair detected in pivot => to match in Ambre (GuaranteeID={guaranteeId})";
                            
                            if (string.IsNullOrWhiteSpace(reco1.Comments))
                                reco1.Comments = timestamp + msg;
                            else if (!reco1.Comments.Contains("Same guarantee Pair detected"))
                                reco1.Comments = reco1.Comments + Environment.NewLine + timestamp + msg;
                            
                            if (string.IsNullOrWhiteSpace(reco2.Comments))
                                reco2.Comments = timestamp + msg;
                            else if (!reco2.Comments.Contains("Same guarantee Pair detected"))
                                reco2.Comments = reco2.Comments + Environment.NewLine + timestamp + msg;
                            
                            await SaveReconciliationsAsync(new List<Reconciliation> { reco1, reco2 }).ConfigureAwait(false);
                            
                            LogHelper.WriteRuleApplied("MANUAL_OUTGOING", countryId, line1.ID, "GUARANTEE_PAIR", 
                                $"Action={actionId}; KPI={kpiId}", $"Matched with {line2.ID} (GuaranteeID={guaranteeId})");
                            LogHelper.WriteRuleApplied("MANUAL_OUTGOING", countryId, line2.ID, "GUARANTEE_PAIR", 
                                $"Action={actionId}; KPI={kpiId}", $"Matched with {line1.ID} (GuaranteeID={guaranteeId})");
                            
                            matchCount++;
                        }
                    }
                }

                /* OLD COMPLEX RULE - COMMENTED OUT
                // Build BGPMT index for receivable lines (for fast lookup)
                var receivableBgpmtIndex = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
                foreach (var recLine in receivableLines)
                {
                    if (reconciliations.TryGetValue(recLine.ID, out var recReco) && 
                        !string.IsNullOrWhiteSpace(recReco.DWINGS_BGPMT))
                    {
                        var bgpmt = recReco.DWINGS_BGPMT.Trim().ToUpperInvariant();
                        if (!receivableBgpmtIndex.ContainsKey(bgpmt))
                        {
                            receivableBgpmtIndex[bgpmt] = new List<string>();
                        }
                        receivableBgpmtIndex[bgpmt].Add(recLine.ID);
                    }
                }

                // Group by account number to only search within same account
                var pivotsByAccount = pivotLines.GroupBy(p => p.Account_ID ?? string.Empty).ToList();

                foreach (var accountGroup in pivotsByAccount)
                {
                    var linesInAccount = accountGroup.ToList();

                    // Find MANUAL_OUTGOING lines (positive amount)
                    var manualOutgoingLines = linesInAccount.Where(line =>
                        line.Category == (int)TransactionType.MANUAL_OUTGOING &&
                        line.SignedAmount > 0).ToList();

                    foreach (var manualLine in manualOutgoingLines)
                    {
                        // Check if line is NOT grouped (no receivable counterpart with same BGPMT)
                        if (!reconciliations.TryGetValue(manualLine.ID, out var manualReco) || manualReco == null)
                            continue;
                        
                        // CRITICAL: Only apply rule if Action is NA or empty (don't overwrite manual actions)
                        var naAction = allUserFields?.FirstOrDefault(uf => 
                            string.Equals(uf.USR_Category, "Action", StringComparison.OrdinalIgnoreCase) &&
                            string.Equals(uf.USR_FieldName, "NA", StringComparison.OrdinalIgnoreCase));
                        var naActionId = naAction?.USR_ID;
                        
                        if (manualReco.Action.HasValue && manualReco.Action.Value != naActionId)
                        {
                            // Action already set to something other than NA, skip this line
                            continue;
                        }
                        
                        // Skip if line has a BGPMT and there's a receivable line with the same BGPMT
                        if (!string.IsNullOrWhiteSpace(manualReco.DWINGS_BGPMT))
                        {
                            var bgpmt = manualReco.DWINGS_BGPMT.Trim().ToUpperInvariant();
                            if (receivableBgpmtIndex.ContainsKey(bgpmt))
                            {
                                // Line is grouped, skip it
                                continue;
                            }
                        }

                        // Extract alphanumeric reconciliation number (case-insensitive)
                        var recoNum = ExtractAlphanumeric(manualLine.Reconciliation_Num);
                        var hasRecoNum = !string.IsNullOrWhiteSpace(recoNum);
                        
                        // Get DWINGS_GuaranteeID from manual line's reconciliation
                        var manualGuaranteeId = manualReco.DWINGS_GuaranteeID?.Trim();
                        var hasGuaranteeId = !string.IsNullOrWhiteSpace(manualGuaranteeId);
                        
                        // Skip if neither reconciliation number nor guarantee ID exists
                        if (!hasRecoNum && !hasGuaranteeId) continue;

                        // Find matching PAYMENT line with negative amount and same reconciliation number OR guarantee ID
                        var matchingPayment = linesInAccount.FirstOrDefault(line =>
                        {
                            if (line.ID == manualLine.ID) return false;
                            if (line.Category != (int)TransactionType.PAYMENT) return false;
                            if (line.SignedAmount >= 0) return false;
                            if (Math.Abs(line.SignedAmount + manualLine.SignedAmount) >= 0.01m) return false; // Amounts must sum to zero
                            
                            // Check reconciliation number match (if exists)
                            if (hasRecoNum && string.Equals(ExtractAlphanumeric(line.Reconciliation_Num), recoNum, StringComparison.OrdinalIgnoreCase))
                                return true;
                            
                            // Check DWINGS_GuaranteeID match (if exists)
                            if (hasGuaranteeId && reconciliations.TryGetValue(line.ID, out var lineReco) && lineReco != null)
                            {
                                var lineGuaranteeId = lineReco.DWINGS_GuaranteeID?.Trim();
                                if (!string.IsNullOrWhiteSpace(lineGuaranteeId) && 
                                    string.Equals(lineGuaranteeId, manualGuaranteeId, StringComparison.OrdinalIgnoreCase))
                                    return true;
                            }
                            
                            return false;
                        });

                        if (matchingPayment != null)
                        {
                            // Apply rule: set both to TO MATCH + PAY BUT NOT RECONCILIED
                            // manualReco already retrieved above
                            if (!reconciliations.TryGetValue(matchingPayment.ID, out var paymentReco) || paymentReco == null)
                                continue;

                            // Also check payment line - only apply if Action is NA or empty
                            if (paymentReco.Action.HasValue && paymentReco.Action.Value != naActionId)
                            {
                                // Payment line already has a manual action, skip this pair
                                continue;
                            }

                            manualReco.Action = actionId;
                            manualReco.KPI = kpiId;
                            EnsureActionDefaults(manualReco);

                            paymentReco.Action = actionId;
                            paymentReco.KPI = kpiId;
                            EnsureActionDefaults(paymentReco);

                            // Determine match criteria for logging
                            var matchCriteria = new List<string>();
                            if (hasRecoNum && string.Equals(ExtractAlphanumeric(matchingPayment.Reconciliation_Num), recoNum, StringComparison.OrdinalIgnoreCase))
                                matchCriteria.Add($"RecoNum={recoNum}");
                            if (hasGuaranteeId)
                            {
                                var paymentGuaranteeId = paymentReco.DWINGS_GuaranteeID?.Trim();
                                if (!string.IsNullOrWhiteSpace(paymentGuaranteeId) && 
                                    string.Equals(paymentGuaranteeId, manualGuaranteeId, StringComparison.OrdinalIgnoreCase))
                                    matchCriteria.Add($"GuaranteeID={manualGuaranteeId}");
                            }
                            var matchInfo = string.Join(" OR ", matchCriteria);

                            // Add comment
                            var timestamp = $"[{DateTime.Now:yyyy-MM-dd HH:mm}] {_currentUser}: ";
                            var msg = $"MANUAL_OUTGOING rule: matched with counterpart payment ({matchInfo})";
                            
                            if (string.IsNullOrWhiteSpace(manualReco.Comments))
                                manualReco.Comments = timestamp + msg;
                            else if (!manualReco.Comments.Contains("MANUAL_OUTGOING rule"))
                                manualReco.Comments = manualReco.Comments + Environment.NewLine + timestamp + msg;

                            if (string.IsNullOrWhiteSpace(paymentReco.Comments))
                                paymentReco.Comments = timestamp + msg;
                            else if (!paymentReco.Comments.Contains("MANUAL_OUTGOING rule"))
                                paymentReco.Comments = paymentReco.Comments + Environment.NewLine + timestamp + msg;

                            await SaveReconciliationsAsync(new List<Reconciliation> { manualReco, paymentReco }).ConfigureAwait(false);

                            LogHelper.WriteRuleApplied("MANUAL_OUTGOING", countryId, manualLine.ID, "MANUAL_OUTGOING_PAIR", 
                                $"Action={actionId}; KPI={kpiId}", $"Matched with {matchingPayment.ID} ({matchInfo})");
                            LogHelper.WriteRuleApplied("MANUAL_OUTGOING", countryId, matchingPayment.ID, "MANUAL_OUTGOING_PAIR", 
                                $"Action={actionId}; KPI={kpiId}", $"Matched with {manualLine.ID} ({matchInfo})");

                            matchCount++;
                        }
                    }
                }
                */

                timer.Stop();
                LogManager.Info($"[PERF] MANUAL_OUTGOING rule completed: {matchCount} pair(s) matched in {timer.ElapsedMilliseconds}ms");
                return matchCount;
            }
            catch (Exception ex)
            {
                timer.Stop();
                LogManager.Error($"ApplyManualOutgoingRuleAsync failed after {timer.ElapsedMilliseconds}ms", ex);
                return 0;
            }
        }

        /// <summary>
        /// Extrait uniquement les caractères alphanumériques d'une chaîne
        /// </summary>
        private static string ExtractAlphanumeric(string input)
        {
            if (string.IsNullOrWhiteSpace(input)) return string.Empty;
            return new string(input.Where(c => char.IsLetterOrDigit(c)).ToArray());
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
        /// Récupère une réconciliation par ID (sans créer si inexistante)
        /// </summary>
        public async Task<Reconciliation> GetReconciliationByIdAsync(string countryId, string id)
        {
            var query = "SELECT * FROM T_Reconciliation WHERE ID = ? AND DeleteDate IS NULL";
            var existing = await ExecuteQueryAsync<Reconciliation>(query, _connectionString, id).ConfigureAwait(false);
            return existing.FirstOrDefault();
        }

        /// <summary>
        /// Get detailed debug information about all rules and their evaluation for a specific line.
        /// Used for debugging UI to show which conditions passed/failed.
        /// </summary>
        public async Task<(RuleContext Context, List<RuleDebugEvaluation> Evaluations)> GetRuleDebugInfoAsync(string reconciliationId)
        {
            try
            {
                var currentCountryId = _offlineFirstService?.CurrentCountryId;
                if (string.IsNullOrWhiteSpace(currentCountryId) || _rulesEngine == null) 
                    return (null, null);
                if (_countries == null || !_countries.TryGetValue(currentCountryId, out var countryCtx) || countryCtx == null) 
                    return (null, null);

                var amb = await GetAmbreRowByIdAsync(currentCountryId, reconciliationId).ConfigureAwait(false);
                var r = await GetOrCreateReconciliationAsync(reconciliationId).ConfigureAwait(false);
                if (amb == null || r == null) return (null, null);

                bool isPivot = amb.IsPivotAccount(countryCtx.CNT_AmbrePivot);
                var ctx = await BuildRuleContextAsync(amb, r, countryCtx, currentCountryId, isPivot).ConfigureAwait(false);
                var evaluations = await _rulesEngine.EvaluateAllForDebugAsync(ctx, RuleScope.Edit).ConfigureAwait(false);
                return (ctx, evaluations);
            }
            catch { return (null, null); }
        }

        /// <summary>
        /// Sauvegarde une réconciliation
        /// </summary>
        public async Task<bool> SaveReconciliationAsync(Reconciliation reconciliation)
        {
            return await SaveReconciliationsAsync(new[] { reconciliation });
        }

        /// <summary>
        /// Sauvegarde une réconciliation avec option pour appliquer (ou non) les règles côté édition.
        /// </summary>
        public async Task<bool> SaveReconciliationAsync(Reconciliation reconciliation, bool applyRulesOnEdit)
        {
            return await SaveReconciliationsAsync(new[] { reconciliation }, applyRulesOnEdit);
        }

        /// <summary>
        /// Sauvegarde plusieurs réconciliations en batch
        /// </summary>
        public async Task<bool> SaveReconciliationsAsync(IEnumerable<Reconciliation> reconciliations, bool applyRulesOnEdit = true)
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
                                if (applyRulesOnEdit)
                                {
                                    // Evaluate truth-table rules on edit (self updates + user message). Counterpart updates are planned separately.
                                    try
                                    {
                                        var currentCountryId = _offlineFirstService?.CurrentCountryId;
                                        if (!string.IsNullOrWhiteSpace(currentCountryId) && _rulesEngine != null)
                                        {
                                            Country countryCtx = null;
                                            if (_countries != null && _countries.TryGetValue(currentCountryId, out var c)) countryCtx = c;
                                            if (countryCtx != null)
                                            {
                                                var amb = await GetAmbreRowByIdAsync(currentCountryId, reconciliation.ID).ConfigureAwait(false);
                                                if (amb != null)
                                                {
                                                    bool isPivot = amb.IsPivotAccount(countryCtx.CNT_AmbrePivot);
                                                    var ctx = await BuildRuleContextAsync(amb, reconciliation, countryCtx, currentCountryId, isPivot).ConfigureAwait(false);
                                                    var res = await _rulesEngine.EvaluateAsync(ctx, RuleScope.Edit).ConfigureAwait(false);
                                                    if (res != null && res.Rule != null)
                                                    {
                                                        if (res.Rule.AutoApply)
                                                        {
                                                            // Edit scope: always apply rules (overwrite existing values)
                                                            if (res.NewActionIdSelf.HasValue)
                                                            {
                                                                reconciliation.Action = res.NewActionIdSelf.Value;
                                                                EnsureActionDefaults(reconciliation);
                                                            }
                                                            if (res.NewActionStatusSelf.HasValue)
                                                            {
                                                                reconciliation.ActionStatus = res.NewActionStatusSelf.Value;
                                                                try { reconciliation.ActionDate = DateTime.Now; } catch { }
                                                            }
                                                            if (res.NewKpiIdSelf.HasValue)
                                                                reconciliation.KPI = res.NewKpiIdSelf.Value;
                                                            if (res.NewIncidentTypeIdSelf.HasValue)
                                                                reconciliation.IncidentType = res.NewIncidentTypeIdSelf.Value;
                                                            if (res.NewRiskyItemSelf.HasValue)
                                                                reconciliation.RiskyItem = res.NewRiskyItemSelf.Value;
                                                            if (res.NewReasonNonRiskyIdSelf.HasValue)
                                                                reconciliation.ReasonNonRisky = res.NewReasonNonRiskyIdSelf.Value;
                                                            if (res.NewToRemindSelf.HasValue)
                                                                reconciliation.ToRemind = res.NewToRemindSelf.Value;
                                                            if (res.NewToRemindDaysSelf.HasValue)
                                                            {
                                                                try { reconciliation.ToRemindDate = DateTime.Today.AddDays(res.NewToRemindDaysSelf.Value); } catch { }
                                                            }
                                                        }
                                                        if (!string.IsNullOrWhiteSpace(res.UserMessage))
                                                        {
                                                            try
                                                            {
                                                                var prefix = $"[{DateTime.Now:yyyy-MM-dd HH:mm}] {_currentUser}: ";
                                                                var msg = prefix + $"[Rule {res.Rule.RuleId ?? "(unnamed)"}] {res.UserMessage}";
                                                                if (string.IsNullOrWhiteSpace(reconciliation.Comments))
                                                                    reconciliation.Comments = msg;
                                                                else if (!reconciliation.Comments.Contains(msg))
                                                                    reconciliation.Comments = msg + Environment.NewLine + reconciliation.Comments;
                                                            }
                                                            catch { }
                                                        }

                                                        // Log to file and raise UI event for edit path (even if fields were already set)
                                                        try
                                                        {
                                                            var outs = new List<string>();
                                                            if (res.NewActionIdSelf.HasValue) outs.Add($"Action={res.NewActionIdSelf.Value}");
                                                            if (res.NewKpiIdSelf.HasValue) outs.Add($"KPI={res.NewKpiIdSelf.Value}");
                                                            if (res.NewIncidentTypeIdSelf.HasValue) outs.Add($"IncidentType={res.NewIncidentTypeIdSelf.Value}");
                                                            if (res.NewRiskyItemSelf.HasValue) outs.Add($"RiskyItem={res.NewRiskyItemSelf.Value}");
                                                            if (res.NewReasonNonRiskyIdSelf.HasValue) outs.Add($"ReasonNonRisky={res.NewReasonNonRiskyIdSelf.Value}");
                                                            if (res.NewToRemindSelf.HasValue) outs.Add($"ToRemind={res.NewToRemindSelf.Value}");
                                                            if (res.NewToRemindDaysSelf.HasValue) outs.Add($"ToRemindDays={res.NewToRemindDaysSelf.Value}");
                                                            if (res.NewFirstClaimTodaySelf == true) outs.Add("First/LastClaimDate=Today");
                                                            var outsStr = string.Join("; ", outs);
                                                            LogHelper.WriteRuleApplied("edit", currentCountryId, reconciliation.ID, res.Rule.RuleId, outsStr, res.UserMessage);
                                                            RaiseRuleApplied("edit", currentCountryId, reconciliation.ID, res.Rule.RuleId, outsStr, res.UserMessage);
                                                        }
                                                        catch { }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    catch { /* do not block user saves on rules engine errors */ }
                                }

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
                    row.DWINGS_BGPMT = r.DWINGS_BGPMT;
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
                    row.IncNumber = r.IncNumber;
                    row.MbawData = r.MbawData;
                    row.SpiritData = r.SpiritData;
                    row.TriggerDate = r.TriggerDate;
                    // row.ReviewDate = r.ReviewDate; // DEPRECATED
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
                                [DWINGS_GuaranteeID], [DWINGS_InvoiceID], [DWINGS_BGPMT],
                                [Action], [ActionStatus], [ActionDate], [Assignee], [Comments], [InternalInvoiceReference],
                                [FirstClaimDate], [LastClaimDate], [ToRemind], [ToRemindDate],
                                [ACK], [SwiftCode], [PaymentReference], [KPI],
                                [IncidentType], [RiskyItem], [ReasonNonRisky], [IncNumber],
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
                            if (!Equal(DbVal(2), (object)reconciliation.DWINGS_BGPMT)) changed.Add("DWINGS_BGPMT");
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
                            if (!Equal(DbVal(20), (object)reconciliation.IncNumber)) changed.Add("IncNumber");
                            if (!Equal(DbVal(21), (object)reconciliation.MbawData)) changed.Add("MbawData");
                            if (!Equal(DbVal(22), (object)reconciliation.SpiritData)) changed.Add("SpiritData");
                            if (!Equal(DbVal(23), reconciliation.TriggerDate.HasValue ? (object)reconciliation.TriggerDate.Value : null)) changed.Add("TriggerDate");
                            // if (!Equal(DbVal(23), reconciliation.ReviewDate.HasValue ? (object)reconciliation.ReviewDate.Value : null)) changed.Add("ReviewDate"); // DEPRECATED

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
                                case "DWINGS_BGPMT":
                                    cmd.Parameters.AddWithValue("@DWINGS_BGPMT", reconciliation.DWINGS_BGPMT ?? (object)DBNull.Value);
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
                                case "IncNumber":
                                    cmd.Parameters.AddWithValue("@IncNumber", reconciliation.IncNumber ?? (object)DBNull.Value);
                                    break;
                                case "TriggerDate":
                                    {
                                        var p = cmd.Parameters.Add("@TriggerDate", OleDbType.Date);
                                        p.Value = reconciliation.TriggerDate.HasValue ? (object)reconciliation.TriggerDate.Value : DBNull.Value;
                                        break;
                                    }
                                // DEPRECATED: ReviewDate removed
                                // case "ReviewDate":
                                //     {
                                //         var p = cmd.Parameters.Add("@ReviewDate", OleDbType.Date);
                                //         p.Value = reconciliation.ReviewDate.HasValue ? (object)reconciliation.ReviewDate.Value : DBNull.Value;
                                //         break;
                                //     }
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
                             ([ID], [DWINGS_GuaranteeID], [DWINGS_InvoiceID], [DWINGS_BGPMT],
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
            cmd.Parameters.AddWithValue("@DWINGS_BGPMT", reconciliation.DWINGS_BGPMT ?? (object)DBNull.Value);
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
            // DEPRECATED: ReviewDate removed
            // var pReview = cmd.Parameters.Add("@ReviewDate", OleDbType.Date);
            // pReview.Value = reconciliation.ReviewDate.HasValue ? (object)reconciliation.ReviewDate.Value : DBNull.Value;

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

        #region Truth Table Helpers
        private async Task<RuleContext> BuildRuleContextAsync(DataAmbre a, Reconciliation r, Country country, string countryId, bool isPivot, bool? isGrouped = null, bool? isAmountMatch = null)
        {
            // Determine transaction type enum name
            var transformationService = new TransformationService(new List<Country> { country });
            TransactionType? tx;
            
            if (isPivot)
            {
                // For PIVOT: use Category field (enum TransactionType)
                tx = transformationService.DetermineTransactionType(a.RawLabel, isPivot, a.Category);
            }
            else
            {
                // For RECEIVABLE: use PAYMENT_METHOD from DWINGS invoice if available
                string paymentMethod = null;
                try
                {
                    if (!string.IsNullOrWhiteSpace(r?.DWINGS_InvoiceID))
                    {
                        var invoices = await GetDwingsInvoicesAsync().ConfigureAwait(false);
                        var inv = invoices?.FirstOrDefault(i => string.Equals(i?.INVOICE_ID, r.DWINGS_InvoiceID, StringComparison.OrdinalIgnoreCase));
                        paymentMethod = inv?.PAYMENT_METHOD;
                    }
                }
                catch { }
                
                // Map PAYMENT_METHOD to TransactionType enum
                if (!string.IsNullOrWhiteSpace(paymentMethod))
                {
                    var upperMethod = paymentMethod.Trim().ToUpperInvariant().Replace(' ', '_');
                    if (Enum.TryParse<TransactionType>(upperMethod, true, out var parsed))
                    {
                        tx = parsed;
                    }
                    else
                    {
                        // Fallback to label-based detection
                        tx = transformationService.DetermineTransactionType(a.RawLabel, isPivot, null);
                    }
                }
                else
                {
                    // No PAYMENT_METHOD available, use label-based detection
                    tx = transformationService.DetermineTransactionType(a.RawLabel, isPivot, null);
                }
            }
            
            string txName = tx.HasValue ? Enum.GetName(typeof(TransactionType), tx.Value) : null;

            // Guarantee type from DWINGS (requires a DWINGS_GuaranteeID link)
            string guaranteeType = null;
            if (!isPivot && !string.IsNullOrWhiteSpace(r?.DWINGS_GuaranteeID))
            {
                try
                {
                    var guarantees = await GetDwingsGuaranteesAsync().ConfigureAwait(false);
                    var guar = guarantees?.FirstOrDefault(g => string.Equals(g?.GUARANTEE_ID, r.DWINGS_GuaranteeID, StringComparison.OrdinalIgnoreCase));
                    guaranteeType = guar?.GUARANTEE_TYPE;
                }
                catch { }
            }

            // Sign from signed amount
            var sign = a.SignedAmount >= 0 ? "C" : "D";

            // Presence of DWINGS links or internal reference (any of Invoice/Guarantee/BGPMT/InternalInvoiceReference)
            bool? hasDw = (!string.IsNullOrWhiteSpace(r?.DWINGS_InvoiceID)
                          || !string.IsNullOrWhiteSpace(r?.DWINGS_GuaranteeID)
                          || !string.IsNullOrWhiteSpace(r?.DWINGS_BGPMT)
                          || !string.IsNullOrWhiteSpace(r?.InternalInvoiceReference));

            // Calculate IsGrouped, IsAmountMatch, and MissingAmount if not provided
            decimal? missingAmount = null;
            if (!isGrouped.HasValue || !isAmountMatch.HasValue)
            {
                var flags = await CalculateGroupingFlagsAsync(a, r, country, countryId).ConfigureAwait(false);
                isGrouped = flags.isGrouped ?? isGrouped;
                isAmountMatch = flags.isAmountMatch ?? isAmountMatch;
                missingAmount = flags.missingAmount;
            }

            // Extended time/state inputs
            var today = DateTime.Today;
            
            // FIXED: Nullable boolean logic - only set to bool value if we can determine it, otherwise keep null
            bool? triggerDateIsNull = r?.TriggerDate.HasValue == true ? (bool?)false : (r != null ? (bool?)true : null);
            
            int? daysSinceTrigger = r?.TriggerDate.HasValue == true
                ? (int?)(today - r.TriggerDate.Value.Date).TotalDays
                : null;
            
            int? operationDaysAgo = a?.Operation_Date.HasValue == true
                ? (int?)(today - a.Operation_Date.Value.Date).TotalDays
                : null;
            
            bool? isMatched = hasDw; // consider matched when any DWINGS link is present
            bool? hasManualMatch = null; // unknown on edit path
            
            // FIXED: IsFirstRequest should be null if we don't have reconciliation data
            bool? isFirstRequest = r?.FirstClaimDate.HasValue == true ? (bool?)false : (r != null ? (bool?)true : null);
            
            int? daysSinceReminder = r?.LastClaimDate.HasValue == true
                ? (int?)(today - r.LastClaimDate.Value.Date).TotalDays
                : null;

            // Resolve DWINGS invoice to compute new inputs (best-effort, from cache)
            string mtStatus = null;
            bool? hasCommEmail = null;
            bool? bgiInitiated = null;
            try
            {
                if (!string.IsNullOrWhiteSpace(r?.DWINGS_InvoiceID))
                {
                    var invoices = await GetDwingsInvoicesAsync().ConfigureAwait(false);
                    var inv = invoices?.FirstOrDefault(i => string.Equals(i?.INVOICE_ID, r.DWINGS_InvoiceID, StringComparison.OrdinalIgnoreCase));
                    if (inv != null)
                    {
                        mtStatus = inv.MT_STATUS;
                        hasCommEmail = inv.COMM_ID_EMAIL;
                        if (!string.IsNullOrWhiteSpace(inv.T_INVOICE_STATUS))
                            bgiInitiated = string.Equals(inv.T_INVOICE_STATUS, "INITIATED", StringComparison.OrdinalIgnoreCase);
                    }
                }
            }
            catch { }

            // Determine IsNewLine flag (tri-state): true if created today, false if known and not today, null if unknown
            bool? isNewLineFlag = null;
            try { if (r != null && r.CreationDate.HasValue) isNewLineFlag = r.CreationDate.Value.Date == today; } catch { }

            return new RuleContext
            {
                CountryId = countryId,
                IsPivot = isPivot,
                GuaranteeType = guaranteeType,
                TransactionType = txName,
                HasDwingsLink = hasDw,
                IsGrouped = isGrouped,
                IsAmountMatch = isAmountMatch,
                MissingAmount = missingAmount,
                Sign = sign,
                Bgi = r?.DWINGS_InvoiceID,
                // Extended fields
                TriggerDateIsNull = triggerDateIsNull,
                DaysSinceTrigger = daysSinceTrigger,
                OperationDaysAgo = operationDaysAgo,
                IsMatched = isMatched,
                HasManualMatch = hasManualMatch,
                IsFirstRequest = isFirstRequest,
                IsNewLine = isNewLineFlag,
                DaysSinceReminder = daysSinceReminder,
                CurrentActionId = r?.Action,
                // New DWINGS-derived
                MtStatus = mtStatus,
                HasCommIdEmail = hasCommEmail,
                IsBgiInitiated = bgiInitiated
            };
        }

        private async Task<DataAmbre> GetAmbreRowByIdAsync(string countryId, string id)
        {
            if (string.IsNullOrWhiteSpace(countryId) || string.IsNullOrWhiteSpace(id)) return null;
            try
            {
                var ambreCs = _offlineFirstService?.GetAmbreConnectionString(countryId);
                if (string.IsNullOrWhiteSpace(ambreCs)) return null;
                var list = await ExecuteQueryAsync<DataAmbre>("SELECT TOP 1 * FROM T_Data_Ambre WHERE ID = ? AND DeleteDate IS NULL", ambreCs, id).ConfigureAwait(false);
                return list?.FirstOrDefault();
            }
            catch { return null; }
        }
        
        /// <summary>
        /// Calculate IsGrouped, IsAmountMatch, and MissingAmount flags for all lines in batch (optimized for bulk operations).
        /// Returns a dictionary mapping each Ambre ID to its (isGrouped, isAmountMatch, missingAmount) flags.
        /// </summary>
        private async Task<Dictionary<string, (bool isGrouped, bool isAmountMatch, decimal? missingAmount)>> CalculateGroupingFlagsBatchAsync(
            List<DataAmbre> ambreLines, Country country, string countryId)
        {
            var result = new Dictionary<string, (bool isGrouped, bool isAmountMatch, decimal? missingAmount)>(StringComparer.OrdinalIgnoreCase);
            
            try
            {
                var reconCs = _offlineFirstService?.GetCountryConnectionString(countryId);
                if (string.IsNullOrWhiteSpace(reconCs) || ambreLines == null || ambreLines.Count == 0)
                    return result;
                
                // Step 1: Load all reconciliations for these lines
                var reconDict = new Dictionary<string, Reconciliation>(StringComparer.OrdinalIgnoreCase);
                using (var conn = new System.Data.OleDb.OleDbConnection(reconCs))
                {
                    await conn.OpenAsync().ConfigureAwait(false);
                    foreach (var ambre in ambreLines)
                    {
                        using (var cmd = conn.CreateCommand())
                        {
                            cmd.CommandText = "SELECT * FROM T_Reconciliation WHERE ID = ?";
                            cmd.Parameters.Add("@id", System.Data.OleDb.OleDbType.VarWChar).Value = ambre.ID;
                            
                            using (var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false))
                            {
                                if (await reader.ReadAsync().ConfigureAwait(false))
                                {
                                    var recon = new Reconciliation
                                    {
                                        ID = reader["ID"]?.ToString(),
                                        DWINGS_InvoiceID = reader["DWINGS_InvoiceID"]?.ToString(),
                                        DWINGS_GuaranteeID = reader["DWINGS_GuaranteeID"]?.ToString(),
                                        DWINGS_BGPMT = reader["DWINGS_BGPMT"]?.ToString(),
                                        InternalInvoiceReference = reader["InternalInvoiceReference"]?.ToString()
                                    };
                                    reconDict[recon.ID] = recon;
                                }
                            }
                        }
                    }
                }
                
                // Step 2: Group lines by reference (priority: BGPMT > InvoiceID > GuaranteeID > InternalInvoiceReference)
                var groupsByRef = new Dictionary<string, List<DataAmbre>>(StringComparer.OrdinalIgnoreCase);
                foreach (var ambre in ambreLines)
                {
                    if (!reconDict.TryGetValue(ambre.ID, out var recon))
                    {
                        result[ambre.ID] = (false, false, null); // No reconciliation = not grouped
                        continue;
                    }
                    
                    string groupingRef = null;
                    if (!string.IsNullOrWhiteSpace(recon.DWINGS_BGPMT))
                        groupingRef = recon.DWINGS_BGPMT.Trim().ToUpperInvariant();
                    else if (!string.IsNullOrWhiteSpace(recon.DWINGS_InvoiceID))
                        groupingRef = recon.DWINGS_InvoiceID.Trim().ToUpperInvariant();
                    else if (!string.IsNullOrWhiteSpace(recon.DWINGS_GuaranteeID))
                        groupingRef = recon.DWINGS_GuaranteeID.Trim().ToUpperInvariant();
                    else if (!string.IsNullOrWhiteSpace(recon.InternalInvoiceReference))
                        groupingRef = recon.InternalInvoiceReference.Trim().ToUpperInvariant();
                    
                    if (string.IsNullOrWhiteSpace(groupingRef))
                    {
                        result[ambre.ID] = (false, false, null); // No grouping reference = not grouped
                        continue;
                    }
                    
                    if (!groupsByRef.ContainsKey(groupingRef))
                        groupsByRef[groupingRef] = new List<DataAmbre>();
                    groupsByRef[groupingRef].Add(ambre);
                }
                
                // Step 3: Calculate flags for each group
                var pivotAccount = country?.CNT_AmbrePivot;
                var receivableAccount = country?.CNT_AmbreReceivable;
                
                foreach (var group in groupsByRef.Values)
                {
                    bool hasPivot = group.Any(l => string.Equals(l.Account_ID, pivotAccount, StringComparison.OrdinalIgnoreCase));
                    bool hasReceivable = group.Any(l => string.Equals(l.Account_ID, receivableAccount, StringComparison.OrdinalIgnoreCase));
                    
                    bool isGrouped = hasPivot && hasReceivable;
                    bool isAmountMatch = false;
                    decimal? missingAmount = null;
                    
                    if (isGrouped)
                    {
                        // Calculate MissingAmount (Receivable + Pivot, should be 0 when balanced)
                        decimal receivableTotal = group.Where(l => string.Equals(l.Account_ID, receivableAccount, StringComparison.OrdinalIgnoreCase)).Sum(l => l.SignedAmount);
                        decimal pivotTotal = group.Where(l => string.Equals(l.Account_ID, pivotAccount, StringComparison.OrdinalIgnoreCase)).Sum(l => l.SignedAmount);
                        missingAmount = receivableTotal + pivotTotal;
                        isAmountMatch = Math.Abs(missingAmount.Value) < 0.01m; // tolerance for floating point
                    }
                    
                    // Set flags for all lines in this group
                    foreach (var line in group)
                    {
                        result[line.ID] = (isGrouped, isAmountMatch, missingAmount);
                    }
                }
            }
            catch { }
            
            return result;
        }
        
        /// <summary>
        /// Calculate IsGrouped, IsAmountMatch, and MissingAmount for a single line (used on edit path).
        /// </summary>
        private async Task<(bool? isGrouped, bool? isAmountMatch, decimal? missingAmount)> CalculateGroupingFlagsAsync(
            DataAmbre a, Reconciliation r, Country country, string countryId)
        {
            try
            {
                // IMPORTANT: To calculate IsGrouped correctly, we need to load ALL lines with the same grouping reference
                // (DWINGS or InternalInvoiceReference - not just the single line being edited)
                if (r == null || a == null) return (null, null, null);
                
                // Determine the grouping reference for this line (priority: BGPMT > InvoiceID > GuaranteeID > InternalInvoiceReference)
                string groupingRef = null;
                if (!string.IsNullOrWhiteSpace(r.DWINGS_BGPMT))
                    groupingRef = r.DWINGS_BGPMT;
                else if (!string.IsNullOrWhiteSpace(r.DWINGS_InvoiceID))
                    groupingRef = r.DWINGS_InvoiceID;
                else if (!string.IsNullOrWhiteSpace(r.DWINGS_GuaranteeID))
                    groupingRef = r.DWINGS_GuaranteeID;
                else if (!string.IsNullOrWhiteSpace(r.InternalInvoiceReference))
                    groupingRef = r.InternalInvoiceReference;
                
                if (string.IsNullOrWhiteSpace(groupingRef))
                    return (false, false, null); // No grouping reference = not grouped
                
                // Load all Ambre lines that share this DWINGS reference
                var ambreCs = _offlineFirstService?.GetAmbreConnectionString(countryId);
                var reconCs = _offlineFirstService?.GetCountryConnectionString(countryId);
                if (string.IsNullOrWhiteSpace(ambreCs) || string.IsNullOrWhiteSpace(reconCs))
                    return (null, null, null);
                
                var relatedLines = new List<DataAmbre>();
                using (var reconConn = new System.Data.OleDb.OleDbConnection(reconCs))
                using (var ambreConn = new System.Data.OleDb.OleDbConnection(ambreCs))
                {
                    await reconConn.OpenAsync().ConfigureAwait(false);
                    await ambreConn.OpenAsync().ConfigureAwait(false);
                    
                    // Find all reconciliations with the same grouping reference
                    var relatedIds = new List<string>();
                    using (var cmd = reconConn.CreateCommand())
                    {
                        cmd.CommandText = @"SELECT ID FROM T_Reconciliation 
                                          WHERE (DWINGS_BGPMT = ? OR DWINGS_InvoiceID = ? OR DWINGS_GuaranteeID = ? OR InternalInvoiceReference = ?)";
                        cmd.Parameters.Add("@ref1", System.Data.OleDb.OleDbType.VarWChar).Value = groupingRef;
                        cmd.Parameters.Add("@ref2", System.Data.OleDb.OleDbType.VarWChar).Value = groupingRef;
                        cmd.Parameters.Add("@ref3", System.Data.OleDb.OleDbType.VarWChar).Value = groupingRef;
                        cmd.Parameters.Add("@ref4", System.Data.OleDb.OleDbType.VarWChar).Value = groupingRef;
                        
                        using (var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false))
                        {
                            while (await reader.ReadAsync().ConfigureAwait(false))
                            {
                                var id = reader["ID"]?.ToString();
                                if (!string.IsNullOrWhiteSpace(id))
                                    relatedIds.Add(id);
                            }
                        }
                    }
                    
                    // Load corresponding Ambre lines
                    foreach (var id in relatedIds)
                    {
                        using (var cmd = ambreConn.CreateCommand())
                        {
                            cmd.CommandText = "SELECT * FROM T_Data_Ambre WHERE ID = ? AND DeleteDate IS NULL";
                            cmd.Parameters.Add("@id", System.Data.OleDb.OleDbType.VarWChar).Value = id;
                            
                            using (var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false))
                            {
                                if (await reader.ReadAsync().ConfigureAwait(false))
                                {
                                    var line = new DataAmbre
                                    {
                                        ID = reader["ID"]?.ToString(),
                                        Account_ID = reader["Account_ID"]?.ToString(),
                                        SignedAmount = reader["SignedAmount"] as decimal? ?? 0m
                                    };
                                    relatedLines.Add(line);
                                }
                            }
                        }
                    }
                }
                
                // Now calculate grouping with all related lines
                var batch = await CalculateGroupingFlagsBatchAsync(relatedLines, country, countryId).ConfigureAwait(false);
                if (batch.TryGetValue(a.ID, out var flags))
                    return (flags.isGrouped, flags.isAmountMatch, flags.missingAmount);
                return (null, null, null);
            }
            catch
            {
                return (null, null, null);
            }
        }
        #endregion
    }
    #endregion
}
