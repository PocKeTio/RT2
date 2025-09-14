using OfflineFirstAccess.Helpers;
using OfflineFirstAccess.Models;
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace RecoTool.Services
{
    // Partial: per-country sync gates and background sync scheduler
    public partial class OfflineFirstService
    {
        // Serialize SynchronizeAsync per country to avoid overlapping syncs
        private static readonly ConcurrentDictionary<string, SemaphoreSlim> _syncSemaphores = new ConcurrentDictionary<string, SemaphoreSlim>(StringComparer.OrdinalIgnoreCase);
        // Coalesce concurrent SynchronizeAsync calls: if one is already running, return the same Task
        private static readonly ConcurrentDictionary<string, Task<SyncResult>> _activeSyncs = new ConcurrentDictionary<string, Task<SyncResult>>(StringComparer.OrdinalIgnoreCase);
        // Debounce background sync requests per country
        private static readonly ConcurrentDictionary<string, DateTime> _lastBgSyncRequestUtc = new ConcurrentDictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Schedule a background synchronization if conditions are met.
        /// - Optional debounce via minInterval
        /// - Optionally only if there are pending local changes
        /// Uses the coalesced SynchronizeAsync internally, so multiple concurrent calls will share the same work.
        /// </summary>
        public async Task ScheduleSyncIfNeededAsync(string countryId, TimeSpan? minInterval = null, bool onlyIfPending = true, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(countryId)) return;
            if (!IsInitialized) return;
            if (!IsNetworkSyncAvailable) return;

            // Debounce
            var now = DateTime.UtcNow;
            var cooldown = minInterval ?? TimeSpan.FromMilliseconds(500);
            var last = _lastBgSyncRequestUtc.GetOrAdd(countryId, DateTime.MinValue);
            if (now - last < cooldown)
            {
                return; // too soon
            }

            // Check pending only if requested
            if (onlyIfPending)
            {
                try
                {
                    var pending = await GetUnsyncedChangeCountAsync(countryId).ConfigureAwait(false);
                    if (pending <= 0)
                    {
                        return; // nothing to do
                    }
                }
                catch (Exception ex)
                {
                    // Don't block scheduling on a transient count error; log and continue
                    try { LogManager.Warn($"[BG-SYNC] Unable to query pending count for {countryId}: {ex.Message}"); } catch { }
                }
            }

            _lastBgSyncRequestUtc[countryId] = now;

            // Enqueue background sync work instead of spinning a separate thread
            BackgroundTaskQueue.Instance.Enqueue(async () =>
            {
                try
                {
                    await SynchronizeAsync(countryId, cancellationToken, null).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    try { LogManager.Error($"[BG-SYNC] Background synchronization failed for {countryId}: {ex}", ex); } catch { }
                }
            });
        }
    }
}
