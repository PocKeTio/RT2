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
                .Where(c => !string.IsNullOrEmpty(c.RowGuid))
                .GroupBy(c => c.RowGuid)
                .ToDictionary(g => g.Key, g => g.First());

            foreach (var remoteChange in remoteChanges)
            {
                if (!remoteChange.TryGetValue(_config.PrimaryKeyGuidColumn, out var guidObj) || guidObj == null)
                {
                    // No GUID -> cannot determine conflict, consider as non-conflict to avoid data loss
                    nonConflicts.Add(remoteChange);
                    continue;
                }

                var remoteGuid = guidObj.ToString();
                if (!string.IsNullOrEmpty(remoteGuid) && localChangesDict.TryGetValue(remoteGuid, out var localChange))
                {
                    // Build a simple Conflict object
                    conflicts.Add(new Conflict
                    {
                        RowGuid = remoteGuid,
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
