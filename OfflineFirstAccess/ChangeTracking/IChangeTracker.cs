using System.Collections.Generic;
using System.Threading.Tasks;
using OfflineFirstAccess.Models;

namespace OfflineFirstAccess.ChangeTracking
{
    /// <summary>
    /// Defines operations for tracking data changes.
    /// </summary>
    public interface IChangeTracker
    {
        /// <summary>
        /// Records a change made to a record.
        /// </summary>
        Task RecordChangeAsync(string tableName, string rowGuid, string operationType);

        /// <summary>
        /// Retrieves all changes that have not yet been synchronized.
        /// </summary>
        Task<IEnumerable<ChangeLogEntry>> GetUnsyncedChangesAsync();

        /// <summary>
        /// Marks a set of changes as synchronized.
        /// </summary>
        Task MarkChangesAsSyncedAsync(IEnumerable<long> changeIds);
    }
}
