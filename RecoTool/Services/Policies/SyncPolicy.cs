using System;

namespace RecoTool.Services.Policies
{
    /// <summary>
    /// Default application sync policy: no background pushes, sync only on explicit or safe events.
    /// </summary>
    public sealed class SyncPolicy : ISyncPolicy
    {
        public bool AllowBackgroundPushes { get; private set; } = false;
        public bool ShouldSyncOnCountryChange { get; private set; } = true;
        public bool ShouldSyncOnPageUnload { get; private set; } = false;
        public bool ShouldSyncOnAppClose { get; private set; } = true;

        public ISyncPolicy WithBackgroundPushes(bool allow)
        {
            return new SyncPolicy
            {
                AllowBackgroundPushes = allow,
                ShouldSyncOnCountryChange = this.ShouldSyncOnCountryChange,
                ShouldSyncOnPageUnload = this.ShouldSyncOnPageUnload,
                ShouldSyncOnAppClose = this.ShouldSyncOnAppClose,
            };
        }
    }
}
