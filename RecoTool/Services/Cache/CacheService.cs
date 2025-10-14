using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace RecoTool.Services.Cache
{
    /// <summary>
    /// Service de cache global avec invalidation explicite
    /// Thread-safe et optimisé pour les accès concurrents
    /// Le cache n'expire jamais automatiquement - il doit être invalidé explicitement
    /// lors d'un import AMBRE ou d'un changement de pays
    /// </summary>
    public sealed class CacheService
    {
        private static readonly Lazy<CacheService> _instance = new Lazy<CacheService>(() => new CacheService(), LazyThreadSafetyMode.ExecutionAndPublication);
        
        public static CacheService Instance => _instance.Value;

        private readonly ConcurrentDictionary<string, CacheEntry> _cache = new ConcurrentDictionary<string, CacheEntry>();
        private readonly TimeSpan _defaultExpiration = TimeSpan.MaxValue; // Never expire by default
        private Timer _cleanupTimer;

        private CacheService()
        {
            // Note: Cache entries never expire automatically
            // They must be explicitly invalidated via InvalidateAll() or InvalidateByPrefix()
            // No cleanup timer needed
        }

        private class CacheEntry
        {
            public object Value { get; set; }
            public DateTime ExpiresAt { get; set; }
            public TimeSpan Expiration { get; set; }

            // Only check expiration if it's not set to MaxValue (never expire)
            public bool IsExpired => Expiration != TimeSpan.MaxValue && DateTime.UtcNow > ExpiresAt;
        }

        /// <summary>
        /// Récupère une valeur du cache ou la charge si absente/expirée
        /// </summary>
        public async Task<T> GetOrLoadAsync<T>(string key, Func<Task<T>> loader, TimeSpan? expiration = null)
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentException("Cache key cannot be null or empty", nameof(key));

            if (loader == null)
                throw new ArgumentNullException(nameof(loader));

            var exp = expiration ?? _defaultExpiration;

            // Try get from cache
            if (_cache.TryGetValue(key, out var entry))
            {
                if (!entry.IsExpired)
                {
                    return (T)entry.Value;
                }
                // Expired, remove it
                _cache.TryRemove(key, out _);
            }

            // Load the value
            var value = await loader().ConfigureAwait(false);

            // Store in cache
            var newEntry = new CacheEntry
            {
                Value = value,
                ExpiresAt = exp == TimeSpan.MaxValue ? DateTime.MaxValue : DateTime.UtcNow.Add(exp),
                Expiration = exp
            };
            _cache[key] = newEntry;

            return value;
        }

        /// <summary>
        /// Récupère une valeur du cache (synchrone) ou la charge si absente/expirée
        /// </summary>
        public T GetOrLoad<T>(string key, Func<T> loader, TimeSpan? expiration = null)
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentException("Cache key cannot be null or empty", nameof(key));

            if (loader == null)
                throw new ArgumentNullException(nameof(loader));

            var exp = expiration ?? _defaultExpiration;

            // Try get from cache
            if (_cache.TryGetValue(key, out var entry))
            {
                if (!entry.IsExpired)
                {
                    return (T)entry.Value;
                }
                // Expired, remove it
                _cache.TryRemove(key, out _);
            }

            // Load the value
            var value = loader();

            // Store in cache
            var newEntry = new CacheEntry
            {
                Value = value,
                ExpiresAt = exp == TimeSpan.MaxValue ? DateTime.MaxValue : DateTime.UtcNow.Add(exp),
                Expiration = exp
            };
            _cache[key] = newEntry;

            return value;
        }

        /// <summary>
        /// Tente de récupérer une valeur du cache
        /// </summary>
        public bool TryGet<T>(string key, out T value)
        {
            value = default;
            
            if (string.IsNullOrWhiteSpace(key))
                return false;

            if (_cache.TryGetValue(key, out var entry))
            {
                if (!entry.IsExpired)
                {
                    value = (T)entry.Value;
                    return true;
                }
                // Expired, remove it
                _cache.TryRemove(key, out _);
            }

            return false;
        }

        /// <summary>
        /// Met en cache une valeur
        /// </summary>
        public void Set<T>(string key, T value, TimeSpan? expiration = null)
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentException("Cache key cannot be null or empty", nameof(key));

            var exp = expiration ?? _defaultExpiration;
            var entry = new CacheEntry
            {
                Value = value,
                ExpiresAt = exp == TimeSpan.MaxValue ? DateTime.MaxValue : DateTime.UtcNow.Add(exp),
                Expiration = exp
            };
            _cache[key] = entry;
        }

        /// <summary>
        /// Invalide une entrée du cache
        /// </summary>
        public void Invalidate(string key)
        {
            if (!string.IsNullOrWhiteSpace(key))
            {
                _cache.TryRemove(key, out _);
            }
        }

        /// <summary>
        /// Invalide toutes les entrées du cache correspondant au préfixe
        /// </summary>
        public void InvalidateByPrefix(string prefix)
        {
            if (string.IsNullOrWhiteSpace(prefix))
                return;

            foreach (var key in _cache.Keys)
            {
                if (key.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                {
                    _cache.TryRemove(key, out _);
                }
            }
        }

        /// <summary>
        /// Invalide toutes les entrées du cache
        /// </summary>
        public void InvalidateAll()
        {
            _cache.Clear();
        }

        /// <summary>
        /// Nettoie les entrées expirées du cache (only for entries with explicit expiration)
        /// Note: Most entries never expire and must be explicitly invalidated
        /// </summary>
        private void CleanupExpiredEntries(object state)
        {
            try
            {
                var now = DateTime.UtcNow;
                var expiredKeys = new System.Collections.Generic.List<string>();

                foreach (var kvp in _cache)
                {
                    if (kvp.Value.IsExpired)
                    {
                        expiredKeys.Add(kvp.Key);
                    }
                }

                foreach (var key in expiredKeys)
                {
                    _cache.TryRemove(key, out _);
                }

                if (expiredKeys.Count > 0)
                {
                    System.Diagnostics.Debug.WriteLine($"CacheService: Cleaned up {expiredKeys.Count} expired entries");
                }
            }
            catch
            {
                // Best effort cleanup
            }
        }

        /// <summary>
        /// Obtient les statistiques du cache
        /// </summary>
        public CacheStats GetStats()
        {
            int totalEntries = _cache.Count;
            int expiredEntries = 0;
            long totalSize = 0;

            foreach (var entry in _cache.Values)
            {
                if (entry.IsExpired)
                {
                    expiredEntries++;
                }
                // Rough size estimation (this is a simplification)
                totalSize += 100; // Base overhead
            }

            return new CacheStats
            {
                TotalEntries = totalEntries,
                FreshEntries = totalEntries - expiredEntries,
                ExpiredEntries = expiredEntries,
                EstimatedSizeBytes = totalSize
            };
        }

        public class CacheStats
        {
            public int TotalEntries { get; set; }
            public int FreshEntries { get; set; }
            public int ExpiredEntries { get; set; }
            public long EstimatedSizeBytes { get; set; }
        }
    }
}
