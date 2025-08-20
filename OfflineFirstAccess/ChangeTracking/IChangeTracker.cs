using System;
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
        Task RecordChangeAsync(string tableName, string recordId, string operationType);

        /// <summary>
        /// Records multiple changes efficiently in a single batch.
        /// </summary>
        Task RecordChangesAsync(IEnumerable<(string TableName, string RecordId, string OperationType)> changes);

        /// <summary>
        /// Begins a session that reuses a single connection/transaction to record many changes efficiently.
        /// Call AddAsync(...) multiple times and then CommitAsync(). Dispose to rollback if not committed.
        /// </summary>
        Task<IChangeLogSession> BeginSessionAsync();

        /// <summary>
        /// Retrieves all changes that have not yet been synchronized.
        /// </summary>
        Task<IEnumerable<ChangeLogEntry>> GetUnsyncedChangesAsync();

        /// <summary>
        /// Marks a set of changes as synchronized.
        /// </summary>
        Task MarkChangesAsSyncedAsync(IEnumerable<long> changeIds);
    }

    /// <summary>
    /// Session for recording many change log entries using one connection/transaction.
    /// </summary>
    public interface IChangeLogSession : IDisposable
    {
        /// <summary>
        /// Adds a single change to the session.
        /// </summary>
        Task AddAsync(string tableName, string recordId, string operationType);

        /// <summary>
        /// Commits all pending changes.
        /// </summary>
        Task CommitAsync();
    }
}
