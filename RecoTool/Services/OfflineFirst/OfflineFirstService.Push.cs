using System;
using System.Collections.Concurrent;
using System.Threading;

namespace RecoTool.Services
{
    // Partial: background push control and per-country push gate
    public partial class OfflineFirstService
    {
        // Prevent overlapping background pushes and storm of triggers
        private static readonly ConcurrentDictionary<string, SemaphoreSlim> _pushSemaphores = new ConcurrentDictionary<string, SemaphoreSlim>(StringComparer.OrdinalIgnoreCase);
        private static readonly ConcurrentDictionary<string, DateTime> _lastPushTimesUtc = new ConcurrentDictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);
        private static readonly TimeSpan _pushCooldown = TimeSpan.FromSeconds(5);
        // Diagnostic: track current stage per country push
        private static readonly ConcurrentDictionary<string, string> _pushStages = new ConcurrentDictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        // Diagnostic: per-run identifier to avoid stale watchdog logs
        private static readonly ConcurrentDictionary<string, Guid> _pushRunIds = new ConcurrentDictionary<string, Guid>(StringComparer.OrdinalIgnoreCase);

        // Coalesce concurrent PushPendingChangesToNetworkAsync calls: if one is already running, return the same Task<int>
        private static readonly ConcurrentDictionary<string, System.Threading.Tasks.Task<int>> _activePushes = new ConcurrentDictionary<string, System.Threading.Tasks.Task<int>>(StringComparer.OrdinalIgnoreCase);
    }
}
