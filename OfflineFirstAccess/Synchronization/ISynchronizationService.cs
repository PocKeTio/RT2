using System;
using System.Threading.Tasks;
using OfflineFirstAccess.Models;

namespace OfflineFirstAccess.Synchronization
{
    /// <summary>
    /// The public interface for the synchronization service.
    /// </summary>
    public interface ISynchronizationService
    {
        /// <summary>
        /// Initializes the service with a specific configuration.
        /// </summary>
        /// <param name="config">The synchronization configuration.</param>
        Task InitializeAsync(SyncConfiguration config);

        /// <summary>
        /// Runs a full synchronization process.
        /// </summary>
        /// <param name="onProgress">An optional callback to report progress.</param>
        /// <returns>A SyncResult object summarizing the operation.</returns>
        Task<SyncResult> SynchronizeAsync(Action<int, string> onProgress = null);
    }
}
