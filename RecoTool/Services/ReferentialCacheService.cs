using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using RecoTool.Models;

namespace RecoTool.Services
{
    /// <summary>
    /// Centralized cache for referential data (Users, Param values)
    /// Provides fast read access with automatic invalidation on modifications
    /// </summary>
    public class ReferentialCacheService
    {
        private readonly ReferentialService _referentialService;
        private readonly object _lock = new object();

        // Cache dictionaries
        private List<(string Id, string Name)> _usersCache = null;
        private Dictionary<string, string> _paramCache = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        
        // Cache timestamps
        private DateTime? _usersCacheTime = null;

        // Cache lifetime (optional - set to null for infinite cache with manual invalidation only)
        private static readonly TimeSpan? CacheLifetime = null; // Infinite cache, invalidate manually

        public ReferentialCacheService(ReferentialService referentialService)
        {
            _referentialService = referentialService ?? throw new ArgumentNullException(nameof(referentialService));
        }

        #region Users Cache

        /// <summary>
        /// Get users list (cached)
        /// </summary>
        public async Task<List<(string Id, string Name)>> GetUsersAsync(CancellationToken cancellationToken = default)
        {
            lock (_lock)
            {
                if (_usersCache != null && IsCacheValid(_usersCacheTime))
                {
                    return new List<(string Id, string Name)>(_usersCache);
                }
            }

            // Cache miss or expired - fetch from DB
            var users = await _referentialService.GetUsersAsync(cancellationToken).ConfigureAwait(false);
            
            lock (_lock)
            {
                _usersCache = users.ToList();
                _usersCacheTime = DateTime.UtcNow;
            }

            return users.ToList();
        }

        /// <summary>
        /// Invalidate users cache
        /// </summary>
        public void InvalidateUsersCache()
        {
            lock (_lock)
            {
                _usersCache = null;
                _usersCacheTime = null;
            }
        }

        #endregion

        #region Param Cache

        /// <summary>
        /// Get param value (cached)
        /// </summary>
        public async Task<string> GetParamValueAsync(string paramKey, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(paramKey)) return null;

            lock (_lock)
            {
                if (_paramCache.TryGetValue(paramKey, out var cached))
                {
                    return cached;
                }
            }

            // Cache miss - fetch from DB
            var value = await _referentialService.GetParamValueAsync(paramKey, cancellationToken).ConfigureAwait(false);
            
            lock (_lock)
            {
                _paramCache[paramKey] = value;
            }

            return value;
        }

        /// <summary>
        /// Invalidate param cache for a specific key
        /// </summary>
        public void InvalidateParamCache(string paramKey)
        {
            if (string.IsNullOrWhiteSpace(paramKey)) return;

            lock (_lock)
            {
                _paramCache.Remove(paramKey);
            }
        }

        /// <summary>
        /// Invalidate all param cache
        /// </summary>
        public void InvalidateAllParamCache()
        {
            lock (_lock)
            {
                _paramCache.Clear();
            }
        }

        #endregion

        #region Cache Utilities

        /// <summary>
        /// Check if global cache is valid (not expired)
        /// </summary>
        private bool IsCacheValid(DateTime? cacheTime)
        {
            if (CacheLifetime == null) return true; // Infinite cache
            
            if (!cacheTime.HasValue) return false;
            
            return DateTime.UtcNow - cacheTime.Value < CacheLifetime.Value;
        }

        /// <summary>
        /// Clear all caches globally
        /// </summary>
        public void InvalidateAllCachesGlobally()
        {
            lock (_lock)
            {
                _usersCache = null;
                _usersCacheTime = null;
                _paramCache.Clear();
            }
        }

        #endregion
    }
}
