using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace RecoTool.Services
{
    // Partial: configuration accessors and path builders
    public partial class OfflineFirstService
    {
        // Centralized config reader from referential T_Param
        private string GetCentralConfig(string key, string fallback = null)
        {
            if (string.IsNullOrWhiteSpace(key)) return fallback;
            var tparam = GetParameter(key);
            return string.IsNullOrWhiteSpace(tparam) ? fallback : tparam;
        }

        // Centralized ACE connection string builders
        private static string AceConn(string path)
            => $"Provider=Microsoft.ACE.OLEDB.16.0;Data Source={path};";

        private static string AceConnNetwork(string path)
            => $"Provider=Microsoft.ACE.OLEDB.16.0;Data Source={path};Jet OLEDB:Database Locking Mode=1;Mode=Share Deny None;";

        // Exclusive open mode (used for lock detection)
        private static string AceConnExclusive(string path)
            => $"Provider=Microsoft.ACE.OLEDB.16.0;Data Source={path};Mode=Share Exclusive;";

        private string GetNetworkAmbreDbPath(string countryId)
        {
            string remoteDir = GetCentralConfig("CountryDatabaseDirectory");
            if (string.IsNullOrWhiteSpace(remoteDir)) remoteDir = GetParameter("CountryDatabaseDirectory");
            string prefix = GetCentralConfig("AmbreDatabasePrefix");
            if (string.IsNullOrWhiteSpace(prefix)) prefix = GetCentralConfig("CountryDatabasePrefix");
            if (string.IsNullOrWhiteSpace(prefix)) prefix = GetParameter("CountryDatabasePrefix") ?? "DB_";
            return Path.Combine(remoteDir, $"{prefix}{countryId}.accdb");
        }

        private string GetNetworkReconciliationDbPath(string countryId)
        {
            string remoteDir = GetCentralConfig("CountryDatabaseDirectory");
            if (string.IsNullOrWhiteSpace(remoteDir)) remoteDir = GetParameter("CountryDatabaseDirectory");
            string prefix = GetCentralConfig("CountryDatabasePrefix");
            if (string.IsNullOrWhiteSpace(prefix)) prefix = GetParameter("CountryDatabasePrefix") ?? "DB_";
            return Path.Combine(remoteDir, $"{prefix}{countryId}.accdb");
        }

        private string GetLocalAmbreDbPath(string countryId)
        {
            string dataDirectory = GetParameter("DataDirectory");
            string prefix = GetCentralConfig("AmbreDatabasePrefix");
            if (string.IsNullOrWhiteSpace(prefix)) prefix = GetCentralConfig("CountryDatabasePrefix");
            if (string.IsNullOrWhiteSpace(prefix)) prefix = GetParameter("CountryDatabasePrefix") ?? "DB_";
            return Path.Combine(dataDirectory, $"{prefix}{countryId}.accdb");
        }

        // Public helpers to build connection strings for local AMBRE/DW databases
        public string GetAmbreConnectionString(string countryId = null)
        {
            var cid = string.IsNullOrWhiteSpace(countryId) ? _currentCountryId : countryId;
            if (string.IsNullOrWhiteSpace(cid)) return null;
            var path = GetLocalAmbreDbPath(cid);
            return string.IsNullOrWhiteSpace(path) ? null : AceConn(path);
        }

        public string GetDwConnectionString()
        {
            var path = GetLocalDwDbPath(_currentCountryId);
            return string.IsNullOrWhiteSpace(path) ? null : AceConn(path);
        }

        public string GetLocalDWDatabasePath(string countryId = null)
        {
            var cid = string.IsNullOrWhiteSpace(countryId) ? _currentCountryId : countryId;
            if (string.IsNullOrWhiteSpace(cid)) return null;
            try { return GetLocalDwDbPath(cid); } catch { return null; }
        }

        private string GetNetworkAmbreZipPath(string countryId)
        {
            string remoteDir = GetCentralConfig("CountryDatabaseDirectory");
            if (string.IsNullOrWhiteSpace(remoteDir)) remoteDir = GetParameter("CountryDatabaseDirectory");
            string prefix = GetCentralConfig("AmbreDatabasePrefix");
            if (string.IsNullOrWhiteSpace(prefix)) prefix = GetCentralConfig("CountryDatabasePrefix");
            if (string.IsNullOrWhiteSpace(prefix)) prefix = GetParameter("CountryDatabasePrefix") ?? "DB_";
            return Path.Combine(remoteDir, $"{prefix}{countryId}.zip");
        }

        private string GetLocalAmbreZipCachePath(string countryId)
        {
            string dataDirectory = GetParameter("DataDirectory");
            string prefix = GetCentralConfig("AmbreDatabasePrefix");
            if (string.IsNullOrWhiteSpace(prefix)) prefix = GetCentralConfig("CountryDatabasePrefix");
            if (string.IsNullOrWhiteSpace(prefix)) prefix = GetParameter("CountryDatabasePrefix") ?? "DB_";
            return Path.Combine(dataDirectory, $"{prefix}{countryId}.zip");
        }

        public Task<bool> IsLocalAmbreZipInSyncWithNetworkAsync(string countryId)
        {
            if (string.IsNullOrWhiteSpace(countryId)) throw new ArgumentException("countryId est requis", nameof(countryId));
            try
            {
                string networkZip = GetNetworkAmbreZipPath(countryId);
                if (string.IsNullOrWhiteSpace(networkZip) || !File.Exists(networkZip))
                {
                    return Task.FromResult(true);
                }

                string localZip = GetLocalAmbreZipCachePath(countryId);
                var netFi = new FileInfo(networkZip);
                var locFi = new FileInfo(localZip);

                if (!locFi.Exists) return Task.FromResult(false);

                bool same = FilesAreEqual(locFi, netFi);
                return Task.FromResult(same);
            }
            catch
            {
                return Task.FromResult(false);
            }
        }

        private string GetNetworkDwZipPath(string countryId)
        {
            string networkDbPath = GetNetworkDwDbPath(countryId);
            string remoteDir = Path.GetDirectoryName(networkDbPath);
            if (string.IsNullOrWhiteSpace(remoteDir) || !Directory.Exists(remoteDir)) return null;

            try
            {
                var candidates = Directory.EnumerateFiles(remoteDir, "*.zip", SearchOption.TopDirectoryOnly)
                    .Where(f => f.IndexOf("_" + countryId, StringComparison.OrdinalIgnoreCase) >= 0)
                    .Where(f =>
                    {
                        var n = Path.GetFileName(f);
                        return n.IndexOf("DW", StringComparison.OrdinalIgnoreCase) >= 0
                               || n.IndexOf("DWINGS", StringComparison.OrdinalIgnoreCase) >= 0;
                    });
                var best = candidates
                    .Select(f => new FileInfo(f))
                    .OrderByDescending(fi => fi.LastWriteTimeUtc)
                    .FirstOrDefault();
                return best?.FullName;
            }
            catch { return null; }
        }

        private string GetLocalDwZipCachePath(string countryId)
        {
            string dataDirectory = GetParameter("DataDirectory");
            string prefix = GetCentralConfig("DWDatabasePrefix");
            if (string.IsNullOrWhiteSpace(prefix)) prefix = GetParameter("DWDatabasePrefix");
            if (string.IsNullOrWhiteSpace(prefix)) prefix = GetCentralConfig("CountryDatabasePrefix");
            if (string.IsNullOrWhiteSpace(prefix)) prefix = GetParameter("CountryDatabasePrefix") ?? "DB_";
            return Path.Combine(dataDirectory, $"{prefix}{countryId}_DW.zip");
        }

        public Task<bool> IsLocalDwZipInSyncWithNetworkAsync(string countryId)
        {
            if (string.IsNullOrWhiteSpace(countryId)) throw new ArgumentException("countryId est requis", nameof(countryId));
            try
            {
                string networkZip = GetNetworkDwZipPath(countryId);
                if (string.IsNullOrWhiteSpace(networkZip) || !File.Exists(networkZip))
                {
                    return Task.FromResult(true);
                }
                string localZip = GetLocalDwZipCachePath(countryId);
                var netFi = new FileInfo(networkZip);
                var locFi = new FileInfo(localZip);
                if (!locFi.Exists) return Task.FromResult(false);
                bool same = FilesAreEqual(locFi, netFi);
                return Task.FromResult(same);
            }
            catch { return Task.FromResult(false); }
        }

        public string GetLocalAmbreDatabasePath(string countryId = null)
        {
            var cid = string.IsNullOrWhiteSpace(countryId) ? _currentCountryId : countryId;
            if (string.IsNullOrWhiteSpace(cid)) return null;
            try { return GetLocalAmbreDbPath(cid); } catch { return null; }
        }

        private string GetLocalReconciliationDbPath(string countryId)
        {
            string dataDirectory = GetParameter("DataDirectory");
            string prefix = GetCentralConfig("CountryDatabasePrefix");
            if (string.IsNullOrWhiteSpace(prefix)) prefix = GetParameter("CountryDatabasePrefix") ?? "DB_";
            return Path.Combine(dataDirectory, $"{prefix}{countryId}.accdb");
        }

        private string GetNetworkDwDbPath(string countryId)
        {
            string remoteDir = GetCentralConfig("CountryDatabaseDirectory");
            if (string.IsNullOrWhiteSpace(remoteDir)) remoteDir = GetParameter("CountryDatabaseDirectory");
            string prefix = GetCentralConfig("DWDatabasePrefix");
            if (string.IsNullOrWhiteSpace(prefix)) prefix = GetParameter("DWDatabasePrefix");
            if (string.IsNullOrWhiteSpace(prefix)) prefix = GetCentralConfig("CountryDatabasePrefix");
            if (string.IsNullOrWhiteSpace(prefix)) prefix = GetParameter("CountryDatabasePrefix") ?? "DB_";
            return Path.Combine(remoteDir, $"{prefix}{countryId}.accdb");
        }

        private string GetLocalDwDbPath(string countryId)
        {
            string dataDirectory = GetParameter("DataDirectory");
            string prefix = GetCentralConfig("DWDatabasePrefix");
            if (string.IsNullOrWhiteSpace(prefix)) prefix = GetParameter("DWDatabasePrefix");
            if (string.IsNullOrWhiteSpace(prefix)) prefix = GetCentralConfig("CountryDatabasePrefix");
            if (string.IsNullOrWhiteSpace(prefix)) prefix = GetParameter("CountryDatabasePrefix") ?? "DB_";
            return Path.Combine(dataDirectory, $"{prefix}{countryId}.accdb");
        }

        /// <summary>
        /// Returns the LastWriteTime (local time) of the DWINGS network database for the given country,
        /// or null when the file cannot be found.
        /// </summary>
        public DateTime? GetNetworkDwDatabaseLastWriteDate(string countryId)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(countryId)) return null;
                string path = GetNetworkDwDbPath(countryId);
                if (string.IsNullOrWhiteSpace(path) || !File.Exists(path)) return null;
                return File.GetLastWriteTime(path);
            }
            catch { return null; }
        }

        /// <summary>
        /// Convenience check: true when the DWINGS network database file was modified today (local date).
        /// Returns false when the file is missing.
        /// </summary>
        public bool IsNetworkDwDatabaseFromToday(string countryId)
        {
            try
            {
                var dt = GetNetworkDwDatabaseLastWriteDate(countryId);
                if (!dt.HasValue) return false;
                return dt.Value.Date == DateTime.Today;
            }
            catch { return false; }
        }

        private string GetNetworkCountryConnectionString(string countryId)
        {
            if (string.IsNullOrWhiteSpace(countryId)) throw new ArgumentException("countryId est requis", nameof(countryId));
            string remoteDir = GetParameter("CountryDatabaseDirectory");
            string countryPrefix = GetParameter("CountryDatabasePrefix") ?? "DB_";
            if (string.IsNullOrWhiteSpace(remoteDir))
                throw new InvalidOperationException("Paramètre CountryDatabaseDirectory manquant (T_Param)");
            string networkDbPath = Path.Combine(remoteDir, $"{countryPrefix}{countryId}.accdb");
            return AceConnNetwork(networkDbPath);
        }
    }
}
