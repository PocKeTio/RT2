namespace RecoTool.Services
{
    #region Configuration Classes

    /// <summary>
    /// Paramètres de synchronisation
    /// </summary>
    public class SyncSettings
    {
        public bool AutoSyncEnabled { get; set; }
        public int SyncIntervalMinutes { get; set; }
        public int MaxConcurrentUsers { get; set; }
        public int LockTimeoutMinutes { get; set; }
        public ConflictResolutionStrategy ConflictResolutionStrategy { get; set; }
    }

    #endregion
}
