namespace RecoTool.Configuration
{
    /// <summary>
    /// Feature flags for enabling/disabling application features
    /// </summary>
    public static class FeatureFlags
    {
        /// <summary>
        /// Enable/disable multi-user features (TodoList session tracking, heartbeat, editing indicators)
        /// Set to false to disable all multi-user overhead (heartbeat timer, session tracking, etc.)
        /// </summary>
        public const bool ENABLE_MULTI_USER = false;
    }
}
