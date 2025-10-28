using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace RecoTool.Services.External
{
    // Production-friendly wrapper that handles authentication, throttling and caching.
    // It delegates actual HTTP calls to an inner client (mock or real) implementing IFreeApiClient.
    public sealed class FreeApiService : IFreeApiClient
    {
        private readonly IFreeApiClient _inner;
        private readonly SemaphoreSlim _gate = new SemaphoreSlim(3, 3); // max 3 concurrent
        private volatile bool _isAuthenticated;
        private readonly ConcurrentDictionary<string, Lazy<Task<string>>> _cache =
            new ConcurrentDictionary<string, Lazy<Task<string>>>(StringComparer.OrdinalIgnoreCase);

        public FreeApiService() : this(new MockFreeApiClient()) { }
        public FreeApiService(IFreeApiClient inner)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        }

        public async Task<bool> AuthenticateAsync()
        {
            try
            {
                // Delegate to inner if it supports auth; otherwise just mark as true
                var ok = true;
                try { ok = await _inner.AuthenticateAsync().ConfigureAwait(false); } catch { ok = true; }
                _isAuthenticated = ok;
                return ok;
            }
            catch
            {
                _isAuthenticated = false;
                return false;
            }
        }
        public bool IsAuthenticated => _isAuthenticated;

        public async Task<string> SearchAsync(DateTime day, string reference, string cntServiceCode)
        {
            // Ensure authenticated once
            if (!_isAuthenticated)
            {
                try { await AuthenticateAsync().ConfigureAwait(false); } catch { }
                if (!_isAuthenticated)
                {
                    // Authentication failed: per requirement, do not call Free API, return null
                    return null;
                }
            }

            var key = BuildKey(day, reference, cntServiceCode);
            var lazy = _cache.GetOrAdd(key, k => new Lazy<Task<string>>(() => ExecuteThrottledAsync(day, reference, cntServiceCode)));
            try
            {
                return await lazy.Value.ConfigureAwait(false);
            }
            catch
            {
                // On failure, drop cache entry to allow retry later
                _cache.TryRemove(key, out _);
                throw;
            }
        }

        private async Task<string> ExecuteThrottledAsync(DateTime day, string reference, string cntServiceCode)
        {
            await _gate.WaitAsync().ConfigureAwait(false);
            try
            {
                return await _inner.SearchAsync(day, reference, cntServiceCode).ConfigureAwait(false);
            }
            finally
            {
                _gate.Release();
            }
        }

        private static string BuildKey(DateTime day, string reference, string cntServiceCode)
        {
            var d = day.Date.ToString("yyyy-MM-dd");
            var r = reference?.Trim() ?? string.Empty;
            var s = cntServiceCode?.Trim() ?? string.Empty;
            return d + "|" + r + "|" + s;
        }
    }
}
