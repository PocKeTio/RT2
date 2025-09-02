using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Linq;
using System.Threading;
using RecoTool.Models;

namespace RecoTool.Services
{
    /// <summary>
    /// Service centralisé pour la gestion des paramètres T_Param
    /// </summary>
    public class ParameterService
    {
        #region Private Fields
        
        private static readonly object _lockObject = new object();
        private static List<Param> _parameters = new List<Param>();
        private static bool _isInitialized = false;
        private readonly string _referentialConnectionString;
        
        #endregion
        
        #region Constructor
        
        /// <summary>
        /// Constructeur du service de paramètres
        /// </summary>
        /// <param name="referentialConnectionString">Chaîne de connexion à la base référentielle</param>
        public ParameterService(string referentialConnectionString)
        {
            _referentialConnectionString = referentialConnectionString ?? throw new ArgumentNullException(nameof(referentialConnectionString));
        }
        
        #endregion
        
        #region Public Methods
        
        /// <summary>
        /// Initialise le service en chargeant tous les paramètres T_Param
        /// </summary>
        public void Initialize()
        {
            if (_isInitialized)
                return;
                
            lock (_lockObject)
            {
                if (_isInitialized)
                    return;
                    
                LoadParametersFromDatabase();
                InitializeDefaultParameters();
                _isInitialized = true;
                
                System.Diagnostics.Debug.WriteLine($"ParameterService initialisé avec {_parameters.Count} paramètres");
            }
        }
        
        /// <summary>
        /// Récupère la valeur d'un paramètre
        /// </summary>
        /// <param name="key">Clé du paramètre</param>
        /// <returns>Valeur du paramètre ou null si non trouvé</returns>
        public string GetParameter(string key)
        {
            if (string.IsNullOrEmpty(key))
                return null;
                
            if (!_isInitialized)
                Initialize();
                
            lock (_lockObject)
            {
                var param = _parameters.FirstOrDefault(p => p.PAR_Key == key);
                return param?.PAR_Value;
            }
        }
        
        /// <summary>
        /// Définit la valeur d'un paramètre
        /// </summary>
        /// <param name="key">Clé du paramètre</param>
        /// <param name="value">Valeur du paramètre</param>
        public void SetParameter(string key, string value)
        {
            if (string.IsNullOrEmpty(key))
                return;
                
            if (!_isInitialized)
                Initialize();
                
            lock (_lockObject)
            {
                var existingParam = _parameters.FirstOrDefault(p => p.PAR_Key == key);
                if (existingParam != null)
                {
                    existingParam.PAR_Value = value;
                }
                else
                {
                    _parameters.Add(new Param { PAR_Key = key, PAR_Value = value });
                }
            }
        }
        
        /// <summary>
        /// Récupère tous les paramètres
        /// </summary>
        /// <returns>Liste de tous les paramètres</returns>
        public List<Param> GetAllParameters()
        {
            if (!_isInitialized)
                Initialize();
                
            lock (_lockObject)
            {
                return _parameters.ToList(); // Retourne une copie
            }
        }
        
        /// <summary>
        /// Recharge les paramètres depuis la base de données
        /// </summary>
        public void RefreshParameters()
        {
            lock (_lockObject)
            {
                _parameters.Clear();
                LoadParametersFromDatabase();
                InitializeDefaultParameters();
                
                System.Diagnostics.Debug.WriteLine($"Paramètres rechargés : {_parameters.Count} paramètres");
            }
        }
        
        /// <summary>
        /// Sauvegarde un paramètre dans la base de données
        /// </summary>
        /// <param name="key">Clé du paramètre</param>
        /// <param name="value">Valeur du paramètre</param>
        public void SaveParameterToDatabase(string key, string value)
        {
            if (string.IsNullOrEmpty(key))
                return;
                
            try
            {
                using (var connection = new OleDbConnection(_referentialConnectionString))
                {
                    connection.Open();
                    
                    // Vérifier si le paramètre existe
                    string checkQuery = "SELECT COUNT(*) FROM T_Param WHERE PAR_Key = ?";
                    using (var checkCommand = new OleDbCommand(checkQuery, connection))
                    {
                        checkCommand.Parameters.Add("@key", OleDbType.VarChar).Value = key;
                        int count = (int)checkCommand.ExecuteScalar();
                        
                        if (count > 0)
                        {
                            // Mise à jour
                            string updateQuery = "UPDATE T_Param SET PAR_Value = ? WHERE PAR_Key = ?";
                            using (var updateCommand = new OleDbCommand(updateQuery, connection))
                            {
                                updateCommand.Parameters.Add("@value", OleDbType.VarChar).Value = value ?? "";
                                updateCommand.Parameters.Add("@key", OleDbType.VarChar).Value = key;
                                updateCommand.ExecuteNonQuery();
                            }
                        }
                        else
                        {
                            // Insertion
                            string insertQuery = "INSERT INTO T_Param (PAR_Key, PAR_Value) VALUES (?, ?)";
                            using (var insertCommand = new OleDbCommand(insertQuery, connection))
                            {
                                insertCommand.Parameters.Add("@key", OleDbType.VarChar).Value = key;
                                insertCommand.Parameters.Add("@value", OleDbType.VarChar).Value = value ?? "";
                                insertCommand.ExecuteNonQuery();
                            }
                        }
                    }
                }
                
                // Mettre à jour le cache local
                SetParameter(key, value);
                
                System.Diagnostics.Debug.WriteLine($"Paramètre sauvegardé : {key} = {value}");
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Erreur lors de la sauvegarde du paramètre {key}: {ex.Message}");
                throw;
            }
        }
        
        #endregion
        
        #region Private Methods
        
        /// <summary>
        /// Charge les paramètres depuis la base de données
        /// </summary>
        private void LoadParametersFromDatabase()
        {
            try
            {
                using (var connection = new OleDbConnection(_referentialConnectionString))
                {
                    connection.Open();
                    string query = "SELECT PAR_Key, PAR_Value FROM T_Param";
                    
                    using (var command = new OleDbCommand(query, connection))
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var param = new Param
                            {
                                PAR_Key = reader["PAR_Key"].ToString(),
                                PAR_Value = reader["PAR_Value"].ToString()
                            };
                            _parameters.Add(param);
                        }
                    }
                }
                
                System.Diagnostics.Debug.WriteLine($"Chargement de {_parameters.Count} paramètres depuis T_Param");
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Erreur lors du chargement des paramètres: {ex.Message}");
                throw;
            }
        }
        
        /// <summary>
        /// Initialise les paramètres par défaut s'ils n'existent pas
        /// </summary>
        private void InitializeDefaultParameters()
        {
            // Répertoire de données par défaut
            if (string.IsNullOrEmpty(GetParameter("DataDirectory")))
            {
                string defaultDataDir = System.IO.Path.Combine(
                    Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData), 
                    "RecoTool", "Data");
                SetParameter("DataDirectory", defaultDataDir);
            }
            
            // Préfixe des bases country par défaut
            if (string.IsNullOrEmpty(GetParameter("CountryDatabasePrefix")))
            {
                SetParameter("CountryDatabasePrefix", "DB_");
            }
            
            // Intervalles et timeouts par défaut
            if (string.IsNullOrEmpty(GetParameter("ChangeCheckIntervalSeconds")))
            {
                SetParameter("ChangeCheckIntervalSeconds", "60");
            }
            
            if (string.IsNullOrEmpty(GetParameter("MaxConnectionRetries")))
            {
                SetParameter("MaxConnectionRetries", "3");
            }
            
            if (string.IsNullOrEmpty(GetParameter("ConnectionRetryDelayMs")))
            {
                SetParameter("ConnectionRetryDelayMs", "1000");
            }
            
            // Tables à synchroniser par défaut
            if (string.IsNullOrEmpty(GetParameter("SyncTables")))
            {
                SetParameter("SyncTables", "T_Reconciliation");
            }
        }
        
        #endregion
        
        #region Static Factory Method
        
        /// <summary>
        /// Crée une instance du ParameterService avec la chaîne de connexion depuis App.config
        /// </summary>
        /// <returns>Instance du ParameterService</returns>
        public static ParameterService CreateFromAppConfig()
        {
            string referentialDbPath = Properties.Settings.Default.ReferentialDB;
            if (string.IsNullOrEmpty(referentialDbPath))
            {
                throw new InvalidOperationException("La configuration 'ReferentialDB' n'est pas définie dans App.config");
            }
            
            string connectionString = $"Provider=Microsoft.ACE.OLEDB.16.0;Data Source={referentialDbPath};";
            return new ParameterService(connectionString);
        }
        
        #endregion
    }
}
