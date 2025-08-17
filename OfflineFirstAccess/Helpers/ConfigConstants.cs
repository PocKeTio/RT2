using System;

namespace OfflineFirstAccess.Helpers
{
    /// <summary>
    /// Constantes pour la configuration de l'application
    /// </summary>
    public static class ConfigConstants
    {
        // Constantes pour le cache d'entités
        public static class EntityCache
        {
            public const string ExpirationMinutes = "EntityCacheExpirationMinutes";
            public const string MaxSize = "EntityCacheMaxSize";
            public const string CleanupIntervalMinutes = "EntityCacheCleanupIntervalMinutes";
            
            // Valeurs par défaut
            public const int DefaultExpirationMinutes = 15;
            public const int DefaultMaxSize = 1000;
            public const int DefaultCleanupIntervalMinutes = 5;
        }
        
        // Constantes pour le gestionnaire de batch adaptatif
        public static class BatchSize
        {
            public const string MinSize = "SyncMinBatchSize";
            public const string MaxSize = "SyncMaxBatchSize";
            public const string InitialSize = "SyncInitialBatchSize";
            public const string Increment = "SyncBatchSizeIncrement";
            public const string Decrement = "SyncBatchSizeDecrement";
            public const string SuccessThreshold = "SyncSuccessThreshold";
            public const string FailureThreshold = "SyncFailureThreshold";
            
            // Valeurs par défaut
            public const int DefaultMinSize = 10;
            public const int DefaultMaxSize = 200;
            public const int DefaultInitialSize = 50;
            public const int DefaultIncrement = 20;
            public const int DefaultDecrement = 30;
            public const int DefaultSuccessThreshold = 3;
            public const int DefaultFailureThreshold = 1;
        }
        
        // Constantes pour la synchronisation
        public static class Sync
        {
            public const string LockTimeoutSeconds = "SyncLockTimeoutSeconds";
            public const string LogRetentionDays = "ChangeLogRetentionDays";
            public const string PushBatchSize = "PushBatchSize";
            public const string PullBatchSize = "PullBatchSize";
            public const string TableBatchSize = "TableBatchSize";
            public const string ConflictBatchSize = "ConflictBatchSize";
            
            // Valeurs par défaut
            public const int DefaultLockTimeoutSeconds = 30;
            public const int DefaultLogRetentionDays = 30;
            public const int DefaultBatchSize = 50;
        }
    }
}
