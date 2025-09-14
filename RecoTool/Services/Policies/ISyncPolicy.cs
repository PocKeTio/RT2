using System;

namespace RecoTool.Services.Policies
{
    /// <summary>
    /// Centralized policy to control when synchronization and background pushes are allowed.
    /// This decouples UI events from OfflineFirstService to avoid scattered sync triggers.
    /// </summary>
    public interface ISyncPolicy
    {
        /// <summary>
        /// If true, background pushes (debounced/periodic) are allowed by the app.
        /// </summary>
        bool AllowBackgroundPushes { get; }

        /// <summary>
        /// If true, a synchronization can be performed after a country change selection completes.
        /// </summary>
        bool ShouldSyncOnCountryChange { get; }

        /// <summary>
        /// If true, a synchronization should be performed when a page is unloaded.
        /// Default: false (navigation should not cause sync).
        /// </summary>
        bool ShouldSyncOnPageUnload { get; }

        /// <summary>
        /// If true, a synchronization should be performed when closing the application window.
        /// </summary>
        bool ShouldSyncOnAppClose { get; }

        /// <summary>
        /// Returns a potentially updated policy instance to enable or disable background pushes temporarily.
        /// </summary>
        ISyncPolicy WithBackgroundPushes(bool allow);
    }
}
