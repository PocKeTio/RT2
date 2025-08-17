using System.Collections.Generic;
using System.Threading.Tasks;
using OfflineFirstAccess.Models;
using GenericRecord = System.Collections.Generic.Dictionary<string, object>;

namespace OfflineFirstAccess.Conflicts
{
    /// <summary>
    /// Defines a strategy for detecting and resolving data conflicts.
    /// </summary>
    public interface IConflictResolver
    {
        /// <summary>
        /// Separates incoming changes into conflicts and non-conflicts.
        /// </summary>
        Task<(List<Conflict> conflicts, List<GenericRecord> nonConflicts)> DetectConflicts(
            IEnumerable<GenericRecord> remoteChanges, IEnumerable<ChangeLogEntry> localUnsyncedChanges);

        /// <summary>
        /// Applies a strategy to resolve identified conflicts.
        /// </summary>
        Task<IEnumerable<GenericRecord>> Resolve(List<Conflict> conflicts);
    }
}
