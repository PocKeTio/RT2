using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using OfflineFirstAccess.Models;

namespace OfflineFirstAccess.Conflicts
{
    public class ManualConflictResolver : IConflictResolver
    {
        private readonly SyncConfiguration _config;

        public ManualConflictResolver(SyncConfiguration config)
        {
            _config = config;
        }

        public Task<(List<Conflict> conflicts, List<Dictionary<string, object>> nonConflicts)> DetectConflicts(
            IEnumerable<Dictionary<string, object>> remoteChanges,
            IEnumerable<ChangeLogEntry> localChanges)
        {
            var conflicts = new List<Conflict>();
            var nonConflicts = new List<Dictionary<string, object>>();

            var localChangesDict = localChanges
                .Where(c => !string.IsNullOrEmpty(c.RecordId))
                .GroupBy(c => c.RecordId)
                .ToDictionary(g => g.Key, g => g.First());

            foreach (var remoteChange in remoteChanges)
            {
                if (!remoteChange.TryGetValue(_config.PrimaryKeyColumn, out var idObj) || idObj == null)
                {
                    // No ID -> cannot determine conflict, consider as non-conflict to avoid data loss
                    nonConflicts.Add(remoteChange);
                    continue;
                }

                var remoteId = idObj.ToString();
                if (!string.IsNullOrEmpty(remoteId) && localChangesDict.TryGetValue(remoteId, out var localChange))
                {
                    // Build a simple Conflict object
                    conflicts.Add(new Conflict
                    {
                        RecordId = remoteId,
                        LocalVersion = null,   // Optional: could be fetched if needed
                        RemoteVersion = remoteChange,
                        ConflictType = localChange.OperationType
                    });
                }
                else
                {
                    nonConflicts.Add(remoteChange);
                }
            }

            return Task.FromResult((conflicts, nonConflicts));
        }

        public Task<IEnumerable<Dictionary<string, object>>> Resolve(List<Conflict> conflicts)
        {
            // Manual strategy: do not auto-resolve. Let the app decide later.
            return Task.FromResult(Enumerable.Empty<Dictionary<string, object>>());
        }
    }
}
