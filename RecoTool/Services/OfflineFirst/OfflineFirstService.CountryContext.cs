using RecoTool.Models;
using System;
using System.Collections.Concurrent;
using System.IO;

namespace RecoTool.Services
{
    // Partial: country context, initialization flags, availability checks
    public partial class OfflineFirstService
    {
        private bool _isInitialized = false;
        private bool _disposed = false;

        // Gestion d'une seule country à la fois
        private Country _currentCountry;
        private string _currentCountryId;
        private readonly string _currentUser;
        private readonly ConcurrentDictionary<string, object> _countrySyncLocks = new ConcurrentDictionary<string, object>();

        public string ReferentialDatabasePath => _ReferentialDatabasePath;

        // Prefer using this when opening OleDbConnection to the referential database
        public string ReferentialConnectionString => AceConn(_ReferentialDatabasePath);

        /// <summary>
        /// Utilisateur actuel pour le verrouillage des enregistrements
        /// </summary>
        public string CurrentUser => _currentUser;

        /// <summary>
        /// Indique si le service est initialisé
        /// </summary>
        public bool IsInitialized => _isInitialized;

        /// <summary>
        /// Indique si la synchronisation réseau est disponible (basé sur la config et l'accès au fichier distant)
        /// </summary>
        public bool IsNetworkSyncAvailable
        {
            get
            {
                try
                {
                    var remotePath = _syncConfig?.RemoteDatabasePath;
                    return !string.IsNullOrWhiteSpace(remotePath) && File.Exists(remotePath);
                }
                catch { return false; }
            }
        }

        /// <summary>
        /// Pays actuellement sélectionné (lecture seule)
        /// </summary>
        public Country CurrentCountry => _currentCountry;

        /// <summary>
        /// Identifiant du pays actuellement sélectionné (lecture seule)
        /// </summary>
        public string CurrentCountryId => _currentCountryId;
    }
}
