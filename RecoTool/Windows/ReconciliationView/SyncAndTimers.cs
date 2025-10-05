using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Threading;
using RecoTool.Services;
using RecoTool.Services.DTOs;

namespace RecoTool.Windows
{
    // Partial: Sync events, debounce timers, and VM change handling
    public partial class ReconciliationView
    {
        private void InitializeFilterDebounce()
        {
            _filterDebounceTimer = new DispatcherTimer
            {
                Interval = TimeSpan.FromMilliseconds(300)
            };
            _filterDebounceTimer.Tick += FilterDebounceTimer_Tick;
        }

        // Ensure the push debounce timer exists
        private void EnsurePushDebounceTimer()
        {
            if (_pushDebounceTimer != null) return;
            _pushDebounceTimer = new DispatcherTimer
            {
                Interval = TimeSpan.FromMilliseconds(400)
            };
            _pushDebounceTimer.Tick += PushDebounceTimer_Tick;
        }

        // Timer handlers (named so we can unsubscribe on Unloaded)
        private void FilterDebounceTimer_Tick(object sender, EventArgs e)
        {
            try
            {
                _filterDebounceTimer?.Stop();
                try { ApplyFilters(); } catch { }
            }
            catch { }
        }

        private void PushDebounceTimer_Tick(object sender, EventArgs e)
        {
            try
            {
                // Disable automatic background push: do not enqueue any network sync here
                if (_pushDebounceTimer != null)
                {
                    _pushDebounceTimer.Stop();
                }
                return;
            }
            catch { }
        }

        // Public entry to schedule a debounced background push
        public void ScheduleBulkPushDebounced()
        {
            try
            {
                // No-op to prevent synchronization being triggered by view interactions
                return;
            }
            catch { }
        }

        private void SubscribeToSyncEvents()
        {
            try
            {
                if (_syncEventsHooked) return;
                var svc = SyncMonitorService.Instance;
                if (svc != null)
                {
                    svc.SyncStateChanged += OnSyncStateChanged;
                    _syncEventsHooked = true;
                }
            }
            catch { }
        }

        private void ReconciliationView_Unloaded(object sender, RoutedEventArgs e)
        {
            try
            {
                if (_syncEventsHooked)
                {
                    var svc = SyncMonitorService.Instance;
                    if (svc != null)
                    {
                        svc.SyncStateChanged -= OnSyncStateChanged;
                    }
                    _syncEventsHooked = false;
                }
                // Unsubscribe from rule-applied event
                try { _reconciliationService.RuleApplied -= ReconciliationService_RuleApplied; } catch { }
                if (_highlightClearTimer != null)
                {
                    _highlightClearTimer.Stop();
                    _highlightClearTimer.Tick -= HighlightClearTimer_Tick;
                    _highlightClearTimer = null;
                }
                // Stop and release debounce timers
                if (_filterDebounceTimer != null)
                {
                    _filterDebounceTimer.Stop();
                    _filterDebounceTimer.Tick -= FilterDebounceTimer_Tick;
                    _filterDebounceTimer = null;
                }
                if (_pushDebounceTimer != null)
                {
                    _pushDebounceTimer.Stop();
                    _pushDebounceTimer.Tick -= PushDebounceTimer_Tick;
                    _pushDebounceTimer = null;
                }
                if (_multiUserWarningRefreshTimer != null)
                {
                    _multiUserWarningRefreshTimer.Stop();
                    _multiUserWarningRefreshTimer = null;
                }
                // Unhook grid scroll events
                if (_resultsScrollViewer != null)
                {
                    try { _resultsScrollViewer.ScrollChanged -= ResultsScrollViewer_ScrollChanged; } catch { }
                    _resultsScrollViewer = null;
                }
                _scrollHooked = false;
                // Detach VM change notifications
                try { VM.PropertyChanged -= VM_PropertyChanged; } catch { }
            }
            catch { }
        }

        private void OnSyncStateChanged(OfflineFirstService.SyncStateChangedEventArgs e)
        {
            // Fire-and-forget async handling
            _ = HandleSyncStateChangedAsync(e);
        }

        // When any VM Filter* property changes, debounce ApplyFilters
        private void VM_PropertyChanged(object sender, PropertyChangedEventArgs e)
        {
            try
            {
                if (e == null) { ScheduleApplyFiltersDebounced(); return; }
                if (string.IsNullOrEmpty(e.PropertyName) || e.PropertyName.StartsWith("Filter", StringComparison.Ordinal))
                {
                    ScheduleApplyFiltersDebounced();
                }
            }
            catch { }
        }

        private async Task HandleSyncStateChangedAsync(OfflineFirstService.SyncStateChangedEventArgs e)
        {
            try
            {
                if (e == null) return;
                if (e.State != OfflineFirstService.SyncStateKind.UpToDate) return;
                // Only react for current view's country
                if (string.IsNullOrWhiteSpace(_currentCountryId)) return;
                if (!string.Equals(e.CountryId, _currentCountryId, StringComparison.OrdinalIgnoreCase)) return;
                if (!_hasLoadedOnce) return; // defer handling until first explicit load done

                // Ensure we are on the UI thread before touching UI-bound state
                if (!Dispatcher.CheckAccess())
                {
                    await Dispatcher.InvokeAsync(() => _ = HandleSyncStateChangedAsync(e));
                    return;
                }

                if (_isSyncRefreshInProgress) return; // coalesce on UI thread
                _isSyncRefreshInProgress = true;

                // Snapshot old data keys for diff
                var oldSnapshot = (_allViewData ?? new List<ReconciliationViewData>()).ToList();
                var oldMap = new Dictionary<string, ReconciliationViewData>(StringComparer.OrdinalIgnoreCase);
                foreach (var r in oldSnapshot)
                {
                    try { oldMap[r?.GetUniqueKey() ?? string.Empty] = r; } catch { }
                }

                // Refresh data (async)
                await RefreshAsync();

                // Compute differences on freshly loaded _allViewData
                var newSnapshot = (_allViewData ?? new List<ReconciliationViewData>()).ToList();
                var newlyAdded = new List<ReconciliationViewData>();
                var updated = new List<ReconciliationViewData>();

                foreach (var n in newSnapshot)
                {
                    string key = null;
                    try { key = n?.GetUniqueKey(); } catch { key = null; }
                    if (string.IsNullOrWhiteSpace(key)) continue;

                    if (!oldMap.TryGetValue(key, out var o) || o == null)
                    {
                        newlyAdded.Add(n);
                        continue;
                    }

                    if (HasMeaningfulUpdate(o, n))
                    {
                        updated.Add(n);
                    }
                }

                // Apply highlight flags on UI objects
                foreach (var r in newlyAdded)
                {
                    try { r.IsNewlyAdded = true; r.IsHighlighted = true; } catch { }
                }
                foreach (var r in updated)
                {
                    try { r.IsUpdated = true; r.IsHighlighted = true; } catch { }
                }

                if (newlyAdded.Count > 0 || updated.Count > 0)
                {
                    StartHighlightClearTimer();
                }
            }
            catch { }
            finally
            {
                _isSyncRefreshInProgress = false;
            }
        }

        private static bool HasMeaningfulUpdate(ReconciliationViewData oldRow, ReconciliationViewData newRow)
        {
            try
            {
                if (oldRow == null || newRow == null) return false;
                // Compare fields that can change due to reconciliation edits or merge
                if (!NullableEquals(oldRow.Action, newRow.Action)) return true;
                if (!NullableEquals(oldRow.KPI, newRow.KPI)) return true;
                if (!NullableEquals(oldRow.IncidentType, newRow.IncidentType)) return true;
                if (!StringEquals(oldRow.Assignee, newRow.Assignee)) return true;
                if (!StringEquals(oldRow.Comments, newRow.Comments)) return true;
                if (oldRow.ToRemind != newRow.ToRemind) return true;
                if (!NullableEquals(oldRow.ToRemindDate, newRow.ToRemindDate)) return true;
                if (oldRow.ACK != newRow.ACK) return true;
                if (!StringEquals(oldRow.Reco_ModifiedBy, newRow.Reco_ModifiedBy)) return true;
                // Add other domain fields as needed
                return false;
            }
            catch { return false; }
        }

        private static bool StringEquals(string a, string b) => string.Equals(a ?? string.Empty, b ?? string.Empty, StringComparison.Ordinal);
        private static bool NullableEquals<T>(T? a, T? b) where T : struct => Nullable.Equals(a, b);

        private void StartHighlightClearTimer()
        {
            try
            {
                if (_highlightClearTimer == null)
                {
                    _highlightClearTimer = new DispatcherTimer();
                    _highlightClearTimer.Interval = TimeSpan.FromMilliseconds(HighlightDurationMs);
                    _highlightClearTimer.Tick += HighlightClearTimer_Tick;
                }
                else
                {
                    _highlightClearTimer.Stop();
                }
                _highlightClearTimer.Interval = TimeSpan.FromMilliseconds(HighlightDurationMs);
                _highlightClearTimer.Start();
            }
            catch { }
        }

        private void HighlightClearTimer_Tick(object sender, EventArgs e)
        {
            try
            {
                _highlightClearTimer?.Stop();
                // Clear flags on all rows
                foreach (var r in _allViewData ?? Enumerable.Empty<ReconciliationViewData>())
                {
                    try
                    {
                        if (r.IsNewlyAdded) r.IsNewlyAdded = false;
                        if (r.IsUpdated) r.IsUpdated = false;
                        if (r.IsHighlighted) r.IsHighlighted = false;
                    }
                    catch { }
                }
            }
            catch { }
        }

        private void ScheduleApplyFiltersDebounced()
        {
            try
            {
                _filterDebounceTimer.Stop();
                _filterDebounceTimer.Start();
            }
            catch { }
        }

        public void QueueBulkPush()
        {
            try
            {
                SyncMonitorService.Instance?.QueueBulkPush(_currentCountryId);
            }
            catch { }
        }
    }
}
