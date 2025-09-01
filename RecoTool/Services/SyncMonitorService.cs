using System;
using System.Threading.Tasks;
using System.Timers;
using System.IO;
using System.Collections.Concurrent;
using System.Threading;
using OfflineFirstAccess.Helpers;

namespace RecoTool.Services
{
    /// <summary>
    /// Central service that polls global lock and network availability once and publishes events.
    /// Pages subscribe to receive notifications and trigger their own sync when appropriate.
    /// </summary>
    public sealed class SyncMonitorService : IDisposable
    {
        private static readonly Lazy<SyncMonitorService> _instance = new Lazy<SyncMonitorService>(() => new SyncMonitorService());
        public static SyncMonitorService Instance => _instance.Value;

        private readonly object _gate = new object();
        private System.Timers.Timer _timer;
        private ElapsedEventHandler _timerHandler;
        private Func<OfflineFirstService> _serviceProvider;
        private bool _lastLockActive;
        private bool _lastNetworkAvailable;
        private bool _initialized;
        private bool _disposed;
        private int _isTickRunning;
        private DateTime _lastSuggestUtc = DateTime.MinValue;
        private DateTime _lastForwardUtc = DateTime.MinValue;
        private DateTime _lastPeriodicPushUtc = DateTime.MinValue;
        private DateTime _lastRemoteReconCheckUtc = DateTime.MinValue;
        private readonly ConcurrentDictionary<string, (long Length, DateTime LastWriteUtcDate)> _remoteReconFingerprint = new ConcurrentDictionary<string, (long, DateTime)>(StringComparer.OrdinalIgnoreCase);

        public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(3);
        public TimeSpan SuggestCooldown { get; set; } = TimeSpan.FromSeconds(15);
        public TimeSpan ForwardCooldown { get; set; } = TimeSpan.FromMilliseconds(300);
        public TimeSpan PeriodicPushInterval { get; set; } = TimeSpan.FromMinutes(5);
        public TimeSpan RemoteReconCheckInterval { get; set; } = TimeSpan.FromMinutes(1);

        // Events
        public event Action<bool> LockStateChanged;              // arg: isActive
        public event Action LockReleased;                        // fired when wasActive -> false
        public event Action NetworkBecameAvailable;              // fired when false -> true
        public event Action<string> SyncSuggested;               // reason: "LockReleased" | "NetworkBecameAvailable"
        public event Action<bool> NetworkAvailabilityChanged;    // arg: isAvailable (true when online)
        public event Action<OfflineFirstService.SyncStateChangedEventArgs> SyncStateChanged; // forwarded from OfflineFirstService

        private SyncMonitorService() { }

        public void Initialize(Func<OfflineFirstService> serviceProvider)
        {
            if (serviceProvider == null) throw new ArgumentNullException(nameof(serviceProvider));
            lock (_gate)
            {
                _serviceProvider = serviceProvider;
                _initialized = true;
                // Prime last known states
                try
                {
                    var svc = _serviceProvider();
                    _lastNetworkAvailable = svc?.IsNetworkSyncAvailable == true;
                    // For lock, default false until first poll
                    _lastLockActive = false;

                    // Subscribe to sync state changes and forward them
                    if (svc != null)
                    {
                        try
                        {
                            svc.SyncStateChanged += (s, e) => ForwardSyncState(e);
                        }
                        catch { }
                    }
                }
                catch
                {
                    _lastNetworkAvailable = false;
                    _lastLockActive = false;
                }
            }
        }

        public void Start()
        {
            lock (_gate)
            {
                if (_disposed) throw new ObjectDisposedException(nameof(SyncMonitorService));
                if (!_initialized) throw new InvalidOperationException("SyncMonitorService not initialized");
                if (_timer != null) return;

                _timer = new System.Timers.Timer(PollInterval.TotalMilliseconds);
                _timer.AutoReset = true;
                _timerHandler = async (_, e) => await OnTimerElapsed(e);
                _timer.Elapsed += _timerHandler;
                _timer.Start();
            }
        }

        public void Stop()
        {
            lock (_gate)
            {
                if (_timer != null)
                {
                    if (_timerHandler != null)
                    {
                        _timer.Elapsed -= _timerHandler;
                        _timerHandler = null;
                    }
                    _timer.Stop();
                    _timer.Dispose();
                    _timer = null;
                }
            }
        }

        private async Task OnTimerElapsed(ElapsedEventArgs e)
        {
            if (Interlocked.CompareExchange(ref _isTickRunning, 1, 0) == 1) return;
            try
            {
                var svc = _serviceProvider?.Invoke();
                if (svc == null || string.IsNullOrEmpty(svc.CurrentCountryId)) return;

                // Check lock state
                bool lockActive = false;
                try { lockActive = await svc.IsGlobalLockActiveAsync(); }
                catch (Exception ex)
                {
                    lockActive = false;
                    LogManager.Warning("[SYNC-MONITOR] Unable to query global lock state", ex);
                }

                // Publish state change events
                if (lockActive != _lastLockActive)
                {
                    _lastLockActive = lockActive;
                    SafeInvoke(() => LockStateChanged?.Invoke(lockActive));
                    if (!lockActive)
                    {
                        SafeInvoke(() => LockReleased?.Invoke());
                        SuggestIfCooldownAllows("LockReleased");
                    }
                }

                // Check network availability
                bool networkAvailable = false;
                try { networkAvailable = svc.IsNetworkSyncAvailable; }
                catch (Exception ex)
                {
                    networkAvailable = false;
                    LogManager.Warning("[SYNC-MONITOR] Unable to query network availability", ex);
                }
                // Publish changes and also periodically reaffirm the current state
                if (networkAvailable != _lastNetworkAvailable)
                {
                    _lastNetworkAvailable = networkAvailable;
                    SafeInvoke(() => NetworkAvailabilityChanged?.Invoke(networkAvailable));
                    if (networkAvailable)
                    {
                        SafeInvoke(() => NetworkBecameAvailable?.Invoke());
                        if (!_lastLockActive)
                        {
                            SuggestIfCooldownAllows("NetworkBecameAvailable");
                        }
                    }
                }
                else
                {
                    // State unchanged: still echo it so UI can self-heal if it drifted due to non-network errors
                    SafeInvoke(() => NetworkAvailabilityChanged?.Invoke(networkAvailable));
                }

                // Periodic best-effort push when online and not locked
                try
                {
                    if (networkAvailable && !lockActive)
                    {
                        var nowUtc = DateTime.UtcNow;
                        if (nowUtc - _lastPeriodicPushUtc >= PeriodicPushInterval)
                        {
                            _lastPeriodicPushUtc = nowUtc;
                            var cid = svc?.CurrentCountryId;
                            if (!string.IsNullOrWhiteSpace(cid))
                            {
                                _ = svc.PushReconciliationIfPendingAsync(cid);
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    LogManager.Error("[SYNC-MONITOR] Periodic push failed", ex);
                }

                // Lightweight remote reconciliation DB change check (timestamp + length) every minute
                try
                {
                    if (networkAvailable && !lockActive)
                    {
                        var nowUtc = DateTime.UtcNow;
                        if (nowUtc - _lastRemoteReconCheckUtc >= RemoteReconCheckInterval)
                        {
                            _lastRemoteReconCheckUtc = nowUtc;
                            var cid = svc?.CurrentCountryId;
                            if (!string.IsNullOrWhiteSpace(cid))
                            {
                                string remoteDir = null;
                                string prefix = null;
                                try { remoteDir = svc.GetParameter("CountryDatabaseDirectory"); }
                                catch (Exception ex)
                                {
                                    remoteDir = null;
                                    LogManager.Warning("[SYNC-MONITOR] Failed to get CountryDatabaseDirectory", ex);
                                }
                                try { prefix = svc.GetParameter("CountryDatabasePrefix"); }
                                catch (Exception ex)
                                {
                                    prefix = null;
                                    LogManager.Warning("[SYNC-MONITOR] Failed to get CountryDatabasePrefix", ex);
                                }
                                if (string.IsNullOrWhiteSpace(prefix)) prefix = "DB_";
                                if (!string.IsNullOrWhiteSpace(remoteDir))
                                {
                                    var remotePath = Path.Combine(remoteDir, $"{prefix}{cid}.accdb");
                                    if (File.Exists(remotePath))
                                    {
                                        var fi = new FileInfo(remotePath);
                                        var current = (Length: fi.Length, LastWriteUtcDate: fi.LastWriteTimeUtc.Date);
                                        var previous = _remoteReconFingerprint.GetOrAdd(cid, (0L, DateTime.MinValue));
                                        if (current.Length != previous.Length || current.LastWriteUtcDate != previous.LastWriteUtcDate)
                                        {
                                            _remoteReconFingerprint[cid] = current;
                                            // Schedule a background sync (coalesced and debounced inside OfflineFirstService)
                                            _ = svc.ScheduleSyncIfNeededAsync(cid, TimeSpan.FromSeconds(5), onlyIfPending: false);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    LogManager.Error("[SYNC-MONITOR] Remote reconciliation check failed", ex);
                }

            }
            catch (Exception ex)
            {
                LogManager.Error("[SYNC-MONITOR] Timer tick failed", ex);
            }
            finally
            {
                Interlocked.Exchange(ref _isTickRunning, 0);
            }
        }

        private void SuggestIfCooldownAllows(string reason)
        {
            var now = DateTime.UtcNow;
            if (now - _lastSuggestUtc < SuggestCooldown) return;
            _lastSuggestUtc = now;
            SafeInvoke(() => SyncSuggested?.Invoke(reason));
        }

        private void ForwardSyncState(OfflineFirstService.SyncStateChangedEventArgs e)
        {
            try
            {
                var now = DateTime.UtcNow;
                if (now - _lastForwardUtc < ForwardCooldown) return;
                _lastForwardUtc = now;
                SafeInvoke(() => SyncStateChanged?.Invoke(e));
            }
            catch { }
        }

        private static void SafeInvoke(Action action)
        {
            try { action?.Invoke(); } catch { }
        }

        public void Dispose()
        {
            if (_disposed) return;
            Stop();
            _disposed = true;
        }
    }
}
