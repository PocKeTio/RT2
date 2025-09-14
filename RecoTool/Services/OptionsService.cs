using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace RecoTool.Services
{
    /// <summary>
    /// Thin wrapper over ReconciliationService to provide cached option lists
    /// for UI ComboBoxes (users, currencies, guarantee types/statuses).
    /// Keeps UI code-behind lean and avoids repeated identical DB calls.
    /// </summary>
    public class OptionsService
    {
        private readonly ReconciliationService _reconciliationService;
        private readonly ReferentialService _referentialService;
        private readonly LookupService _lookupService;

        public OptionsService(ReconciliationService reconciliationService, ReferentialService referentialService, LookupService lookupService)
        {
            _reconciliationService = reconciliationService ?? throw new ArgumentNullException(nameof(reconciliationService));
            _referentialService = referentialService ?? throw new ArgumentNullException(nameof(referentialService));
            _lookupService = lookupService ?? throw new ArgumentNullException(nameof(lookupService));
        }

        // Users are global (referential DB), cache across instances
        private static Lazy<Task<List<(string Id, string Name)>>> _usersCache;

        public Task<List<(string Id, string Name)>> GetUsersAsync()
        {
            var lazy = _usersCache;
            if (lazy == null)
            {
                lazy = new Lazy<Task<List<(string Id, string Name)>>>(async () =>
                {
                    var list = await _referentialService.GetUsersAsync().ConfigureAwait(false);
                    // Normalize names: fall back to Id if Name missing
                    return list?.Select(u => (u.Id, string.IsNullOrWhiteSpace(u.Name) ? u.Id : u.Name)).ToList()
                           ?? new List<(string, string)>();
                }, System.Threading.LazyThreadSafetyMode.ExecutionAndPublication);
                System.Threading.Interlocked.CompareExchange(ref _usersCache, lazy, null);
            }
            return lazy.Value;
        }

        // Currencies vary by country
        private static readonly ConcurrentDictionary<string, Lazy<Task<List<string>>>> _currenciesByCountry
            = new ConcurrentDictionary<string, Lazy<Task<List<string>>>>(StringComparer.OrdinalIgnoreCase);

        public Task<List<string>> GetCurrenciesAsync(string countryId)
        {
            if (string.IsNullOrWhiteSpace(countryId))
                return Task.FromResult(new List<string>());

            var entry = _currenciesByCountry.GetOrAdd(countryId, new Lazy<Task<List<string>>>(async () =>
            {
                var list = await _lookupService.GetCurrenciesAsync(countryId).ConfigureAwait(false);
                return (list ?? new List<string>()).Distinct(StringComparer.OrdinalIgnoreCase).OrderBy(s => s).ToList();
            }, System.Threading.LazyThreadSafetyMode.ExecutionAndPublication));
            return entry.Value;
        }

        // Guarantee statuses and types are global lists
        private static Lazy<Task<List<string>>> _guaranteeStatuses;
        private static Lazy<Task<List<string>>> _guaranteeTypes;

        public Task<List<string>> GetGuaranteeStatusesAsync()
        {
            var lazy = _guaranteeStatuses;
            if (lazy == null)
            {
                lazy = new Lazy<Task<List<string>>>(async () =>
                {
                    var list = await _lookupService.GetGuaranteeStatusesAsync().ConfigureAwait(false);
                    return (list ?? new List<string>()).Distinct(StringComparer.OrdinalIgnoreCase).OrderBy(s => s).ToList();
                }, System.Threading.LazyThreadSafetyMode.ExecutionAndPublication);
                System.Threading.Interlocked.CompareExchange(ref _guaranteeStatuses, lazy, null);
            }
            return lazy.Value;
        }

        public Task<List<string>> GetGuaranteeTypesAsync()
        {
            var lazy = _guaranteeTypes;
            if (lazy == null)
            {
                lazy = new Lazy<Task<List<string>>>(async () =>
                {
                    var list = await _lookupService.GetGuaranteeTypesAsync().ConfigureAwait(false);
                    return (list ?? new List<string>()).Distinct(StringComparer.OrdinalIgnoreCase).OrderBy(s => s).ToList();
                }, System.Threading.LazyThreadSafetyMode.ExecutionAndPublication);
                System.Threading.Interlocked.CompareExchange(ref _guaranteeTypes, lazy, null);
            }
            return lazy.Value;
        }

        // Invalidation hooks (optional use after big imports or referential changes)
        public static void InvalidateAll()
        {
            _usersCache = null;
            _guaranteeStatuses = null;
            _guaranteeTypes = null;
            _currenciesByCountry.Clear();
        }

        public static void InvalidateUsers() => _usersCache = null;
        public static void InvalidateCurrencies(string countryId)
        {
            if (!string.IsNullOrWhiteSpace(countryId))
                _currenciesByCountry.TryRemove(countryId, out _);
        }
        public static void InvalidateGuaranteeStatuses() => _guaranteeStatuses = null;
        public static void InvalidateGuaranteeTypes() => _guaranteeTypes = null;
    }
}
