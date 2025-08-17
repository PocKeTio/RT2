using System;
using System.Threading.Tasks;
using System.Timers;

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
        private Timer _timer;
        private Func<OfflineFirstService> _serviceProvider;
        private bool _lastLockActive;
        private bool _lastNetworkAvailable;
        private bool _initialized;
        private bool _disposed;
        private bool _isTickRunning;
        private DateTime _lastSuggestUtc = DateTime.MinValue;

        public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(3);
        public TimeSpan SuggestCooldown { get; set; } = TimeSpan.FromSeconds(15);

        // Events
        public event Action<bool> LockStateChanged;              // arg: isActive
        public event Action LockReleased;                        // fired when wasActive -> false
        public event Action NetworkBecameAvailable;              // fired when false -> true
        public event Action<string> SyncSuggested;               // reason: "LockReleased" | "NetworkBecameAvailable"

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

                _timer = new Timer(PollInterval.TotalMilliseconds);
                _timer.AutoReset = true;
                _timer.Elapsed += OnTimerElapsed;
                _timer.Start();
            }
        }

        public void Stop()
        {
            lock (_gate)
            {
                if (_timer != null)
                {
                    _timer.Elapsed -= OnTimerElapsed;
                    _timer.Stop();
                    _timer.Dispose();
                    _timer = null;
                }
            }
        }

        private async void OnTimerElapsed(object sender, ElapsedEventArgs e)
        {
            if (_isTickRunning) return;
            _isTickRunning = true;
            try
            {
                var svc = _serviceProvider?.Invoke();
                if (svc == null || string.IsNullOrEmpty(svc.CurrentCountryId)) return;

                // Check lock state
                bool lockActive = false;
                try { lockActive = await svc.IsGlobalLockActiveAsync(); } catch { lockActive = false; }

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
                try { networkAvailable = svc.IsNetworkSyncAvailable; } catch { networkAvailable = false; }
                if (networkAvailable && !_lastNetworkAvailable)
                {
                    _lastNetworkAvailable = true;
                    SafeInvoke(() => NetworkBecameAvailable?.Invoke());
                    if (!_lastLockActive)
                    {
                        SuggestIfCooldownAllows("NetworkBecameAvailable");
                    }
                }
                else if (!networkAvailable)
                {
                    _lastNetworkAvailable = false;
                }
            }
            catch { }
            finally
            {
                _isTickRunning = false;
            }
        }

        private void SuggestIfCooldownAllows(string reason)
        {
            var now = DateTime.UtcNow;
            if (now - _lastSuggestUtc < SuggestCooldown) return;
            _lastSuggestUtc = now;
            SafeInvoke(() => SyncSuggested?.Invoke(reason));
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
