using System;

namespace RecoTool.Services
{
    // Partial: lock helpers for remote control/lock database
    public partial class OfflineFirstService
    {
        private string GetRemoteLockConnectionString(string countryId)
        {
            // Prefer a Control DB per country if configured; fallback to legacy per-country lock file next to data DBs
            var perCountryControl = GetControlDbPath(countryId);
            if (!string.IsNullOrWhiteSpace(perCountryControl))
                return AceConn(perCountryControl);

            string remoteDir = GetParameter("CountryDatabaseDirectory");
            string countryDatabasePrefix = GetParameter("CountryDatabasePrefix") ?? "DB_";
            string lockPath = System.IO.Path.Combine(remoteDir, $"{countryDatabasePrefix}{countryId}_lock.accdb");
            return AceConn(lockPath);
        }

        /// <summary>
        /// Returns the absolute path to the remote lock/control database for the given country.
        /// Mirrors <see cref="GetRemoteLockConnectionString"/> logic but returns a file path instead of a connection string.
        /// </summary>
        private string GetRemoteLockDbPath(string countryId)
        {
            var perCountryControl = GetControlDbPath(countryId);
            if (!string.IsNullOrWhiteSpace(perCountryControl))
                return perCountryControl;

            string remoteDir = GetParameter("CountryDatabaseDirectory");
            string countryDatabasePrefix = GetParameter("CountryDatabasePrefix") ?? "DB_";
            return System.IO.Path.Combine(remoteDir, $"{countryDatabasePrefix}{countryId}_lock.accdb");
        }
    }
}
