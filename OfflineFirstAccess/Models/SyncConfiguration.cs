using System.Collections.Generic;

namespace OfflineFirstAccess.Models
{
    /// <summary>
    /// Data object to define the synchronization behavior.
    /// </summary>
    public class SyncConfiguration
    {
        public string LocalDatabasePath { get; set; }
        public string RemoteDatabasePath { get; set; }
        public string LockDatabasePath { get; set; }
        public string ChangeLogConnectionString { get; set; }
        public List<string> TablesToSync { get; set; } = new List<string>();

        // Standard metadata columns
        public string LastModifiedColumn { get; set; } = "LastModified";
        public string IsDeletedColumn { get; set; } = "IsDeleted";
        public string PrimaryKeyColumn { get; set; } = "ID";
    }
}
