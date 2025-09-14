using System;
using System.Threading;

namespace RecoTool.Services
{
    // Partial: sync state events and notifications
    public partial class OfflineFirstService
    {
        public enum SyncStateKind
        {
            UpToDate,
            SyncInProgress,
            OfflinePending,
            Error
        }

        public sealed class SyncStateChangedEventArgs : EventArgs
        {
            public string CountryId { get; set; }
            public SyncStateKind State { get; set; }
            public int PendingCount { get; set; }
            public Exception LastError { get; set; }
            public DateTime TimestampUtc { get; set; } = DateTime.UtcNow;
        }

        public event EventHandler<SyncStateChangedEventArgs> SyncStateChanged;

        private async System.Threading.Tasks.Task RaiseSyncStateAsync(string countryId, SyncStateKind state, int? pendingOverride = null, Exception error = null)
        {
            try
            {
                int pending = 0;
                if (pendingOverride.HasValue)
                {
                    pending = pendingOverride.Value;
                }
                else if (!string.IsNullOrWhiteSpace(countryId))
                {
                    try { pending = await GetUnsyncedChangeCountAsync(countryId); } catch { pending = 0; }
                }

                var args = new SyncStateChangedEventArgs
                {
                    CountryId = countryId,
                    State = state,
                    PendingCount = pending,
                    LastError = error,
                    TimestampUtc = DateTime.UtcNow
                };
                SyncStateChanged?.Invoke(this, args);
            }
            catch { /* never throw from event raise */ }
        }
    }
}
