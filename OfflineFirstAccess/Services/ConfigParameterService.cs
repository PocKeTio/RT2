using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Globalization;
using System.Threading.Tasks;
using OfflineFirstAccess.Helpers;
using OfflineFirstAccess.Models;

namespace OfflineFirstAccess.Services
{
    /// <summary>
    /// Service de gestion des paramètres de configuration stockés en base de données
    /// </summary>
    public class T_ConfigParameterservice
    {
        private readonly string _connectionString;
        private readonly string _contextIdentifier;
        private readonly Dictionary<string, ConfigParameter> _parameterCache;
        
        /// <summary>
        /// Constructeur
        /// </summary>
        /// <param name="connectionString">Chaîne de connexion à la base de données</param>
        /// <param name="contextIdentifier">Identifiant de contexte</param>
        public T_ConfigParameterservice(string connectionString, string contextIdentifier)
        {
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            _contextIdentifier = contextIdentifier ?? throw new ArgumentNullException(nameof(contextIdentifier));
            _parameterCache = new Dictionary<string, ConfigParameter>();
        }
        
        /// <summary>
        /// Initialise le service et charge les paramètres en cache
        /// </summary>
        public async Task InitializeAsync()
        {
            await LoadParametersAsync();
        }
        
        /// <summary>
        /// Charge tous les paramètres de configuration depuis la base de données
        /// </summary>
        public async Task LoadParametersAsync()
        {
            _parameterCache.Clear();
            
            try
            {
                using (var connection = new OleDbConnection(_connectionString))
                {
                    await connection.OpenAsync();
                    
                    string sql = "SELECT * FROM T_ConfigParameters";
                    using (var command = new OleDbCommand(sql, connection))
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            var parameter = new ConfigParameter
                            {
                                Key = reader["CPA_Key"].ToString(),
                                Value = reader["CPA_Value"]?.ToString(),
                                Description = reader["CPA_Description"]?.ToString(),
                                Category = reader["CPA_Category"]?.ToString(),
                                LastModified = reader["CPA_LastModified"] != DBNull.Value
                                    ? Convert.ToDateTime(reader["CPA_LastModified"], CultureInfo.InvariantCulture).ToUniversalTime()
                                    : DateTime.UtcNow,
                                IsUserEditable = reader["CPA_IsUserEditable"] != DBNull.Value
                                    ? Convert.ToBoolean(reader["CPA_IsUserEditable"])
                                    : true
                            };
                            
                            _parameterCache[parameter.Key] = parameter;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogManager.Error($"Erreur lors du chargement des paramètres : {ex.Message}", ex);
            }
        }
        
        /// <summary>
        /// Obtient un paramètre par sa clé
        /// </summary>
        /// <param name="key">Clé du paramètre</param>
        /// <param name="defaultValue">Valeur par défaut si le paramètre n'existe pas</param>
        /// <returns>Valeur du paramètre ou valeur par défaut</returns>
        public string GetParameterValue(string key, string defaultValue = null)
        {
            if (_parameterCache.TryGetValue(key, out var parameter))
            {
                return parameter.Value;
            }
            
            return defaultValue;
        }
        
        /// <summary>
        /// Obtient un paramètre typé par sa clé
        /// </summary>
        /// <typeparam name="T">Type de la valeur</typeparam>
        /// <param name="key">Clé du paramètre</param>
        /// <param name="defaultValue">Valeur par défaut si le paramètre n'existe pas</param>
        /// <returns>Valeur du paramètre convertie au type spécifié ou valeur par défaut</returns>
        public T GetParameterValue<T>(string key, T defaultValue = default)
        {
            if (_parameterCache.TryGetValue(key, out var parameter))
            {
                try
                {
                    if (typeof(T) == typeof(int))
                    {
                        return (T)(object)Convert.ToInt32(parameter.Value);
                    }
                    else if (typeof(T) == typeof(long))
                    {
                        return (T)(object)Convert.ToInt64(parameter.Value);
                    }
                    else if (typeof(T) == typeof(double))
                    {
                        return (T)(object)Convert.ToDouble(parameter.Value);
                    }
                    else if (typeof(T) == typeof(bool))
                    {
                        return (T)(object)Convert.ToBoolean(parameter.Value);
                    }
                    else if (typeof(T) == typeof(DateTime))
                    {
                        return (T)(object)Convert.ToDateTime(parameter.Value, CultureInfo.InvariantCulture);
                    }
                    else
                    {
                        return (T)Convert.ChangeType(parameter.Value, typeof(T));
                    }
                }
                catch
                {
                    return defaultValue;
                }
            }
            
            return defaultValue;
        }
        
        /// <summary>
        /// Définit la valeur d'un paramètre
        /// </summary>
        /// <param name="key">Clé du paramètre</param>
        /// <param name="value">Nouvelle valeur</param>
        /// <param name="description">Description (optionnelle)</param>
        /// <param name="category">Catégorie (optionnelle)</param>
        /// <param name="isUserEditable">Indique si le paramètre est modifiable par l'utilisateur</param>
        /// <returns>True si l'opération a réussi</returns>
        public async Task<bool> SetParameterValueAsync(string key, string value, string description = null, string category = null, bool isUserEditable = true)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentException("La clé du paramètre ne peut pas être nulle ou vide", nameof(key));
            
            // Sauvegarde du paramètre existant pour le rollback en cas d'échec
            ConfigParameter oldParameter = null;
            if (_parameterCache.TryGetValue(key, out var existingParam))
            {
                // Faire une copie profonde pour pouvoir restaurer
                oldParameter = new ConfigParameter
                {
                    Key = existingParam.Key,
                    Value = existingParam.Value,
                    Description = existingParam.Description,
                    Category = existingParam.Category,
                    LastModified = existingParam.LastModified,
                    IsUserEditable = existingParam.IsUserEditable
                };
            }
            
            try
            {
                // Créer ou mettre à jour le paramètre en cache
                var parameter = existingParam ?? new ConfigParameter { Key = key };
                
                parameter.Value = value;
                parameter.LastModified = DateTime.UtcNow;
                
                if (description != null)
                    parameter.Description = description;
                
                if (category != null)
                    parameter.Category = category;
                
                parameter.IsUserEditable = isUserEditable;
                
                // Mettre à jour temporairement le cache - on le validera après la mise à jour en base
                _parameterCache[key] = parameter;
                
                // Enregistrer en base de données
                using (var connection = new OleDbConnection(_connectionString))
                {
                    await connection.OpenAsync();
                    
                    // Utiliser une transaction pour assurer l'atomicité
                    using (var transaction = connection.BeginTransaction())
                    {
                        try
                        {
                            // Vérifier si le paramètre existe déjà
                            bool exists = false;
                            using (var command = new OleDbCommand("SELECT COUNT(*) FROM T_ConfigParameters WHERE [Key] = ?", connection, transaction))
                            {
                                command.Parameters.AddWithValue("@Key", key);
                                exists = Convert.ToInt32(await command.ExecuteScalarAsync()) > 0;
                            }
                            
                            if (exists)
                            {
                                // Mise à jour
                                using (var command = new OleDbCommand(
                                    "UPDATE T_ConfigParameters SET [CPA_Value] = ?, [CPA_Description] = ?, [CPA_Category] = ?, " +
                                    "[CPA_LastModified] = ?, [CPA_IsUserEditable] = ? WHERE [CPA_Key] = ?", connection, transaction))
                                {
                                    command.Parameters.AddWithValue("@Value", value ?? (object)DBNull.Value);
                                    command.Parameters.AddWithValue("@Description", parameter.Description ?? (object)DBNull.Value);
                                    command.Parameters.AddWithValue("@Category", parameter.Category ?? (object)DBNull.Value);
                                    // Access expects DATETIME as OLE Automation Date (double)
                                    command.Parameters.AddWithValue("@LastModified", parameter.LastModified.ToOADate());
                                    command.Parameters.AddWithValue("@IsUserEditable", parameter.IsUserEditable);
                                    command.Parameters.AddWithValue("@Key", key);
                                    
                                    await command.ExecuteNonQueryAsync();
                                }
                            }
                            else
                            {
                                // Insertion
                                using (var command = new OleDbCommand(
                                    "INSERT INTO T_ConfigParameters ([CPA_Key], [CPA_Value], [CPA_Description], [CPA_Category], [CPA_LastModified], [CPA_IsUserEditable]) " +
                                    "VALUES (?, ?, ?, ?, ?, ?)", connection, transaction))
                                {
                                    command.Parameters.AddWithValue("@Key", key);
                                    command.Parameters.AddWithValue("@Value", value ?? (object)DBNull.Value);
                                    command.Parameters.AddWithValue("@Description", parameter.Description ?? (object)DBNull.Value);
                                    command.Parameters.AddWithValue("@Category", parameter.Category ?? (object)DBNull.Value);
                                    // Access expects DATETIME as OLE Automation Date (double)
                                    command.Parameters.AddWithValue("@LastModified", parameter.LastModified.ToOADate());
                                    command.Parameters.AddWithValue("@IsUserEditable", parameter.IsUserEditable);
                                    
                                    await command.ExecuteNonQueryAsync();
                                }
                            }
                            
                            // Valider la transaction
                            transaction.Commit();
                            return true;
                        }
                        catch (Exception ex)
                        {
                            // Annuler la transaction
                            LogManager.Error($"Erreur lors de l'enregistrement du paramètre en base : {ex.Message}", ex);
                            transaction.Rollback();
                            throw; // Propager l'exception pour déclencher le rollback du cache
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                // Rollback du cache en cas d'erreur
                if (oldParameter != null)
                {
                    // Restaurer l'ancienne valeur dans le cache
                    _parameterCache[key] = oldParameter;
                    LogManager.Info($"Rollback du cache pour le paramètre {key} après échec de la mise à jour en base");
                }
                else
                {
                    // Supprimer l'entrée du cache si elle n'existait pas avant
                    _parameterCache.Remove(key);
                    LogManager.Info($"Suppression du paramètre {key} du cache après échec de l'insertion en base");
                }
                
                LogManager.Error($"Erreur lors de la mise à jour du paramètre : {ex.Message}", ex);
                return false;
            }
        }
        
        /// <summary>
        /// Supprime un paramètre
        /// </summary>
        /// <param name="key">Clé du paramètre à supprimer</param>
        /// <returns>True si l'opération a réussi</returns>
        public async Task<bool> DeleteParameterAsync(string key)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentException("La clé du paramètre ne peut pas être vide", nameof(key));
            
            // Sauvegarde du paramètre existant pour le rollback en cas d'échec
            ConfigParameter oldParameter = null;
            bool existsInCache = _parameterCache.TryGetValue(key, out var existingParam);
            
            if (existsInCache)
            {
                // Faire une copie profonde pour pouvoir restaurer
                oldParameter = new ConfigParameter
                {
                    Key = existingParam.Key,
                    Value = existingParam.Value,
                    Description = existingParam.Description,
                    Category = existingParam.Category,
                    LastModified = existingParam.LastModified,
                    IsUserEditable = existingParam.IsUserEditable
                };
            }
            else
            {
                // Le paramètre n'est pas dans le cache, rien à supprimer
                return false;
            }
            
            try
            {
                // Supprimer du cache (temporairement)
                _parameterCache.Remove(key);
                
                using (var connection = new OleDbConnection(_connectionString))
                {
                    await connection.OpenAsync();
                    
                    // Utiliser une transaction pour assurer l'atomicité
                    using (var transaction = connection.BeginTransaction())
                    {
                        try
                        {
                            string sql = "DELETE FROM T_ConfigParameters WHERE [CPA_Key] = @Key";
                            using (var command = new OleDbCommand(sql, connection, transaction))
                            {
                                command.Parameters.AddWithValue("@Key", key);
                                
                                int rowsAffected = await command.ExecuteNonQueryAsync();
                                if (rowsAffected > 0)
                                {
                                    // Valider la transaction si au moins une ligne a été affectée
                                    transaction.Commit();
                                    return true;
                                }
                                else
                                {
                                    // Annuler la transaction si aucune ligne n'a été affectée
                                    LogManager.Warning($"Le paramètre {key} n'a pas été trouvé en base lors de la tentative de suppression");
                                    transaction.Rollback();
                                    throw new Exception($"Le paramètre {key} n'existe pas en base de données");
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            // Annuler la transaction
                            LogManager.Error($"Erreur lors de la suppression du paramètre en base : {ex.Message}", ex);
                            transaction.Rollback();
                            throw; // Propager l'exception pour déclencher le rollback du cache
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                // Rollback du cache en cas d'erreur
                if (oldParameter != null)
                {
                    // Restaurer l'ancienne valeur dans le cache
                    _parameterCache[key] = oldParameter;
                    LogManager.Info($"Rollback du cache pour le paramètre {key} après échec de la suppression en base");
                }
                
                LogManager.Error($"Erreur lors de la suppression du paramètre : {ex.Message}", ex);
                return false;
            }
        }
        
        /// <summary>
        /// Obtient tous les paramètres
        /// </summary>
        /// <returns>Liste des paramètres</returns>
        public List<ConfigParameter> GetAllParameters()
        {
            var result = new List<ConfigParameter>();
            
            foreach (var parameter in _parameterCache.Values)
            {
                result.Add(parameter);
            }
            
            return result;
        }
        
        /// <summary>
        /// Obtient tous les paramètres d'une catégorie
        /// </summary>
        /// <param name="category">Catégorie</param>
        /// <returns>Liste des paramètres de la catégorie</returns>
        public List<ConfigParameter> GetParametersByCategory(string category)
        {
            if (string.IsNullOrEmpty(category))
                throw new ArgumentException("La catégorie ne peut pas être vide", nameof(category));
                
            var result = new List<ConfigParameter>();
            
            foreach (var parameter in _parameterCache.Values)
            {
                if (parameter.Category == category)
                {
                    result.Add(parameter);
                }
            }
            
            return result;
        }
        
        /// <summary>
        /// Initialise les paramètres par défaut
        /// </summary>
        /// <returns>True si l'opération a réussi</returns>
        public async Task<bool> InitializeDefaultParametersAsync()
        {
            try
            {
                // Paramètres de chemins
                await SetParameterValueAsync("LocalDatabaseFormat", "Local_{0}.accdb", 
                    "Format du nom de fichier pour les bases de données locales", "Paths", false);
                    
                await SetParameterValueAsync("RemoteDataDatabaseFormat", "\\\\serveur\\share\\Data_{0}.accdb", 
                    "Format du chemin pour les bases de données distantes (données)", "Paths", false);
                    
                await SetParameterValueAsync("RemoteLockDatabaseFormat", "\\\\serveur\\share\\Lock_{0}.accdb", 
                    "Format du chemin pour les bases de données distantes (verrous)", "Paths", false);
                    
                await SetParameterValueAsync("RemoteSignalFileFormat", "\\\\serveur\\share\\Sessions_{0}.signal", 
                    "Format du chemin pour les fichiers de signal", "Paths", false);
                
                // Paramètres de connexion et de synchronisation
                await SetParameterValueAsync("ConnectionTimeoutSeconds", "30", 
                    "Délai d'expiration des connexions à la base de données en secondes", "Connection", true);
                    
                await SetParameterValueAsync("MaxConnectionRetries", "3", 
                    "Nombre maximum de tentatives de connexion", "Connection", true);
                    
                await SetParameterValueAsync("ConnectionRetryDelayMs", "500", 
                    "Délai entre les tentatives de connexion en millisecondes", "Connection", true);
                    
                await SetParameterValueAsync("ChangeCheckIntervalSeconds", "60", 
                    "Intervalle de vérification des changements en secondes", "Sync", true);
                    
                await SetParameterValueAsync("SyncLockTimeoutSeconds", "30", 
                    "Délai d'expiration des verrous de synchronisation en secondes", "Sync", true);
                    
                // Paramètres de session
                await SetParameterValueAsync("SessionTimeoutMinutes", "5", 
                    "Délai d'expiration des sessions en minutes", "Session", true);
                    
                await SetParameterValueAsync("HeartbeatIntervalMinutes", "1", 
                    "Intervalle d'envoi des signaux de heartbeat en minutes", "Session", true);
                    
                await SetParameterValueAsync("FallbackCheckIntervalSeconds", "30", 
                    "Intervalle de vérification de secours en secondes", "Session", true);
                    
                await SetParameterValueAsync("TimestampToleranceSeconds", "5", 
                    "Tolérance pour les comparaisons d'horodatages en secondes", "Sync", true);
                    
                await SetParameterValueAsync("SyncConflictStrategy", "LastModifiedWins", 
                    "Stratégie de résolution des conflits de synchronisation", "Sync", true);
                    
                // Note: HeartbeatIntervalMinutes est déjà défini plus haut, nous n'avons pas besoin de HeartbeatIntervalSeconds
                // pour éviter la confusion avec deux paramètres similaires
                
                // Ajouter des paramètres pour les batchs de synchronisation (recommandé par l'analyse)
                await SetParameterValueAsync("ConflictDetectionBatchSize", "5",
                    "Taille des lots pour la détection des conflits", "Sync", true);
                    
                await SetParameterValueAsync("SyncUpdateBatchSize", "50",
                    "Taille des lots pour les mises à jour pendant la synchronisation", "Sync", true);
                
                return true;
            }
            catch (Exception ex)
            {
                LogManager.Error($"Erreur lors de l'initialisation des paramètres par défaut : {ex.Message}", ex);
                return false;
            }
        }
    }
}
