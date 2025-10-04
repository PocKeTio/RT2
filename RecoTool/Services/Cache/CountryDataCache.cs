using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using RecoTool.Models;
using RecoTool.Services.DTOs;

namespace RecoTool.Services.Cache
{
    /// <summary>
    /// Cache multi-niveau pour les données de country (UserFields, etc.)
    /// Évite de recharger les mêmes données à chaque changement de country
    /// </summary>
    public class CountryDataCache
    {
        private readonly ConcurrentDictionary<string, CountryData> _cache = new ConcurrentDictionary<string, CountryData>();
        private readonly TimeSpan _cacheLifetime;
        
        public CountryDataCache(TimeSpan? cacheLifetime = null)
        {
            _cacheLifetime = cacheLifetime ?? TimeSpan.FromMinutes(30);
        }
        
        /// <summary>
        /// Données cachées par country
        /// </summary>
        public class CountryData
        {
            public string CountryId { get; set; }
            public List<UserField> UserFields { get; set; }
            public DateTime LoadedAt { get; set; }
            
            public bool IsStale(TimeSpan lifetime) => DateTime.UtcNow - LoadedAt > lifetime;
        }
        
        /// <summary>
        /// Récupère les données depuis le cache ou appelle le loader si nécessaire
        /// </summary>
        public async Task<CountryData> GetOrLoadAsync(string countryId, Func<Task<CountryData>> loader)
        {
            if (string.IsNullOrWhiteSpace(countryId))
            {
                return null;
            }
            
            // Vérifier le cache
            if (_cache.TryGetValue(countryId, out var cached))
            {
                if (!cached.IsStale(_cacheLifetime))
                {
                    // Cache hit !
                    return cached;
                }
                
                // Cache expiré, on le supprime
                _cache.TryRemove(countryId, out _);
            }
            
            // Cache miss, charger les données
            var data = await loader();
            if (data != null)
            {
                data.CountryId = countryId;
                data.LoadedAt = DateTime.UtcNow;
                _cache[countryId] = data;
            }
            
            return data;
        }
        
        /// <summary>
        /// Invalide le cache pour une country spécifique
        /// </summary>
        public void Invalidate(string countryId)
        {
            if (!string.IsNullOrWhiteSpace(countryId))
            {
                _cache.TryRemove(countryId, out _);
            }
        }
        
        /// <summary>
        /// Invalide tout le cache
        /// </summary>
        public void InvalidateAll()
        {
            _cache.Clear();
        }
        
        /// <summary>
        /// Récupère les statistiques du cache
        /// </summary>
        public CacheStats GetStats()
        {
            int totalEntries = _cache.Count;
            int staleEntries = 0;
            
            foreach (var entry in _cache.Values)
            {
                if (entry.IsStale(_cacheLifetime))
                {
                    staleEntries++;
                }
            }
            
            return new CacheStats
            {
                TotalEntries = totalEntries,
                FreshEntries = totalEntries - staleEntries,
                StaleEntries = staleEntries
            };
        }
        
        public class CacheStats
        {
            public int TotalEntries { get; set; }
            public int FreshEntries { get; set; }
            public int StaleEntries { get; set; }
        }
    }
}
