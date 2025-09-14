using System;
using System.Linq;
using System.Threading.Tasks;

namespace RecoTool.Services
{
    // Partial: local ChangeLog feature and helpers
    public partial class OfflineFirstService
    {
        // When true, ChangeLog is stored in the LOCAL database instead of the Control/Lock DB on the network.
        // This guarantees durable pending-changes metadata when offline.
        private readonly bool _useLocalChangeLog = true;

        private string GetChangeLogConnectionString(string countryId)
        {
            // Always use a dedicated LOCAL ChangeLog database per country for durability
            var path = GetLocalChangeLogDbPath(countryId);
            return AceConn(path);
        }

        /// <summary>
        /// Returns the full path to the dedicated local ChangeLog database for a country.
        /// Example: DataDirectory\ChangeLog_{countryId}.accdb
        /// </summary>
        private string GetLocalChangeLogDbPath(string countryId)
        {
            if (string.IsNullOrWhiteSpace(countryId))
                throw new ArgumentException("countryId is required", nameof(countryId));
            string dataDirectory = GetParameter("DataDirectory");
            if (string.IsNullOrWhiteSpace(dataDirectory))
                throw new InvalidOperationException("Paramètre DataDirectory manquant (T_Param)");
            string fileName = $"ChangeLog_{countryId}.accdb";
            return System.IO.Path.Combine(dataDirectory, fileName);
        }

        // Returns true if the local ChangeLog contains unsynchronized entries
        private async Task<bool> HasUnsyncedLocalChangesAsync(string countryId)
        {
            if (!_useLocalChangeLog) return false;
            if (string.IsNullOrWhiteSpace(countryId)) return false;
            try
            {
                var tracker = new OfflineFirstAccess.ChangeTracking.ChangeTracker(GetChangeLogConnectionString(countryId));
                var list = await tracker.GetUnsyncedChangesAsync();
                return list != null && list.Any();
            }
            catch { return false; }
        }
    }
}
