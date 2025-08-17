using System;
using System.IO;
using System.Data.OleDb;
using System.Threading.Tasks;
using System.Text;
using OfflineFirstAccess.Models;

namespace OfflineFirstAccess.Helpers
{
    /// <summary>
    /// Classe utilitaire pour générer des bases de données template Access
    /// </summary>
    public static class DatabaseTemplateGenerator
    {
        /// <summary>
        /// Crée une base de données template avec la structure définie dans la configuration
        /// </summary>
        /// <param name="templatePath">Chemin complet où la base de données template sera créée</param>
        /// <param name="config">Configuration définissant la structure de la base de données</param>
        /// <returns>True si la création a réussi, False sinon</returns>
        public static async Task<bool> CreateDatabaseTemplateAsync(string templatePath, DatabaseConfiguration config)
        {
            try
            {
                // S'assurer que le répertoire existe
                string directory = Path.GetDirectoryName(templatePath);
                if (!Directory.Exists(directory))
                {
                    Directory.CreateDirectory(directory);
                }

                // Supprimer le fichier s'il existe déjà
                if (File.Exists(templatePath))
                {
                    File.Delete(templatePath);
                }

                // Créer une nouvelle base de données Access vide
                LogManager.Info($"Création d'une nouvelle base de données Access à {templatePath}");
                
                try
                {
                    // Stratégie 1: Utiliser une base de données template vide existante (méthode préférée)
                    string emptyTemplateFile = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "EmptyTemplate.accdb");
                    if (File.Exists(emptyTemplateFile))
                    {
                        // Copier le modèle vide vers la destination
                        LogManager.Info("Utilisation d'un modèle vide existant");
                        File.Copy(emptyTemplateFile, templatePath, true);
                        goto CreateTables;
                    }
                    
                    // Stratégie 2: Utiliser ADOX pour créer une nouvelle base de données (meilleure approche)
                    LogManager.Info("Tentative de création via ADOX");
                    
                    try
                    {
                        // ADOX est une bibliothèque COM qui permet de créer des objets ADO et des bases de données
                        // Nous utilisons la réflexion pour éviter une dépendance directe au moment de la compilation
                        Type catalogType = Type.GetTypeFromProgID("ADOX.Catalog");
                        if (catalogType != null)
                        {
                            LogManager.Info("ADOX est disponible, création de la base de données...");
                            
                            // Créer une instance du catalogue
                            dynamic catalog = Activator.CreateInstance(catalogType);
                            
                            // Définir la chaîne de connexion et créer la base de données
                            string connectionString2 = $"Provider=Microsoft.ACE.OLEDB.16.0;Data Source={templatePath};";
                            catalog.Create(connectionString2);
                            
                            LogManager.Info($"Base de données créée avec succès via ADOX à {templatePath}");
                            goto CreateTables;
                        }
                        else
                        {
                            LogManager.Warning("ADOX n'est pas disponible sur ce système");
                        }
                    }
                    catch (Exception adoxEx)
                    {
                        LogManager.Warning($"Erreur lors de la création via ADOX: {adoxEx.Message}");
                    }
                    
                    // Stratégie 3: Créer une base avec une approche native OleDb
                    LogManager.Info("Création d'une base de données vide via OleDb");
                    string dbConnectionString = $"Provider=Microsoft.ACE.OLEDB.16.0;Data Source={templatePath};Jet OLEDB:Engine Type=5;";
                    
                    // Créer un répertoire temporaire pour travailler si nécessaire
                    string tempDir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
                    Directory.CreateDirectory(tempDir);
                    
                    try
                    {
                        // Script SQL pour créer une table système minimale
                        string minimalTableSql = "CREATE TABLE MinimalTable (ID COUNTER PRIMARY KEY, Name TEXT(255))";
                        
                        // Créer un fichier vide
                        using (var fs = File.Create(templatePath))
                        {
                            fs.Close();
                        }
                        
                        // Essayer d'ouvrir une connexion et créer une table minimale
                        using (var connection = new OleDbConnection(dbConnectionString))
                        {
                            try
                            {
                                connection.Open();
                                using (var command = new OleDbCommand(minimalTableSql, connection))
                                {
                                    command.ExecuteNonQuery();
                                }
                                connection.Close();
                                LogManager.Info("Base de données Access créée avec succès via OleDb");
                            }
                            catch (Exception ex)
                            {
                                LogManager.Warning($"Échec de la création via OleDb: {ex.Message}");
                                // Continuer avec la stratégie 3
                            }
                        }
                    }
                    finally
                    {
                        // Nettoyer le répertoire temporaire
                        try { Directory.Delete(tempDir, true); } catch { }
                    }
                    
                    // Stratégie 3: Informer clairement de l'échec et des alternatives
                    LogManager.Error("Toutes les stratégies de création ont échoué",
                    new InvalidOperationException(
                        "Impossible de créer une base de données Access valide. \n" +
                        "Veuillez créer manuellement une base Access vide et la placer dans le répertoire de l'application " +
                        "sous le nom 'EmptyTemplate.accdb', ou installer Microsoft Access Database Engine."));
                }
                catch (Exception ex)
                {
                    LogManager.Error($"Erreur lors de la création de la base de données : {ex.Message}", ex);
                    return false;
                }

                CreateTables:
                // Ouvrir une connexion pour créer les tables
                string connectionString = $"Provider=Microsoft.ACE.OLEDB.16.0;Data Source={templatePath};Persist Security Info=False;";
                using (var connection = new OleDbConnection(connectionString))
                {
                    await connection.OpenAsync();
                    
                    // Créer les tables définies dans la configuration
                    foreach (var table in config.Tables)
                    {
                        try
                        {
                            LogManager.Info($"Création de la table {table.Name}...");
                            using (var command = new OleDbCommand(table.CreateTableSql, connection))
                            {
                                await command.ExecuteNonQueryAsync();
                            }
                            LogManager.Info($"Table {table.Name} créée avec succès");
                        }
                        catch (Exception ex)
                        {
                            LogManager.Error($"Erreur lors de la création de la table {table.Name} : {ex.Message}", new Exception(""));
                        }
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                LogManager.Error($"Erreur lors de la création de la base de données template : {ex.Message}", ex);
                return false;
            }
        }

        /// <summary>
        /// Crée une base de données template avec une structure standard
        /// </summary>
        /// <param name="templatePath">Chemin complet où la base de données template sera créée</param>
        /// <returns>True si la création a réussi, False sinon</returns>
        public static async Task<bool> CreateStandardTemplateAsync(string templatePath)
        {
            // Créer une configuration standard
            var config = DatabaseConfiguration.CreateDefault(Path.GetDirectoryName(templatePath));
            
            return await CreateDatabaseTemplateAsync(templatePath, config);
        }

        /// <summary>
        /// Crée une base de données template avec une structure personnalisée
        /// </summary>
        /// <param name="templatePath">Chemin complet où la base de données template sera créée</param>
        /// <param name="configureAction">Action permettant de configurer la structure de la base de données</param>
        /// <returns>True si la création a réussi, False sinon</returns>
        public static async Task<bool> CreateCustomTemplateAsync(string templatePath, Action<DatabaseConfiguration> configureAction)
        {
            // Créer une configuration de base
            var config = DatabaseConfiguration.CreateDefault(Path.GetDirectoryName(templatePath));
            
            // Appliquer la configuration personnalisée
            configureAction?.Invoke(config);
            
            return await CreateDatabaseTemplateAsync(templatePath, config);
        }

        /// <summary>
        /// Vérifie si la structure d'une base de données existante correspond à la configuration
        /// </summary>
        /// <param name="databasePath">Chemin de la base de données à vérifier</param>
        /// <param name="config">Configuration définissant la structure attendue</param>
        /// <returns>True si la structure correspond, False sinon</returns>
        public static async Task<bool> ValidateDatabaseStructureAsync(string databasePath, DatabaseConfiguration config)
        {
            if (!File.Exists(databasePath))
                return false;

            try
            {
                string connectionString = $"Provider=Microsoft.ACE.OLEDB.16.0;Data Source={databasePath};Persist Security Info=False;";
                using (var connection = new OleDbConnection(connectionString))
                {
                    await connection.OpenAsync();
                    
                    // Vérifier chaque table définie dans la configuration
                    foreach (var table in config.Tables)
                    {
                        // Vérifier si la table existe
                        bool tableExists = false;
                        using (var command = new OleDbCommand(
                            "SELECT COUNT(*) FROM MSysObjects WHERE Type=1 AND Name=?", connection))
                        {
                            command.Parameters.AddWithValue("@Name", table.Name);
                            int count = Convert.ToInt32(await command.ExecuteScalarAsync());
                            tableExists = count > 0;
                        }

                        if (!tableExists)
                            return false;

                        // Vérifier les colonnes de la table
                        foreach (var column in table.Columns)
                        {
                            bool columnExists = false;
                            using (var command = new OleDbCommand(
                                "SELECT COUNT(*) FROM MSysObjects o " +
                                "INNER JOIN MSysColumns c ON o.Id = c.TableId " +
                                "WHERE o.Name=? AND c.Name=?", connection))
                            {
                                command.Parameters.AddWithValue("@TableName", table.Name);
                                command.Parameters.AddWithValue("@ColumnName", column.Name);
                                int count = Convert.ToInt32(await command.ExecuteScalarAsync());
                                columnExists = count > 0;
                            }

                            if (!columnExists)
                                return false;
                        }
                    }
                }

                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Met à jour la structure d'une base de données existante pour correspondre à la configuration
        /// </summary>
        /// <param name="databasePath">Chemin de la base de données à mettre à jour</param>
        /// <param name="config">Configuration définissant la structure attendue</param>
        /// <returns>True si la mise à jour a réussi, False sinon</returns>
        public static async Task<bool> UpdateDatabaseStructureAsync(string databasePath, DatabaseConfiguration config)
        {
            if (!File.Exists(databasePath))
                return false;

            try
            {
                string connectionString = $"Provider=Microsoft.ACE.OLEDB.16.0;Data Source={databasePath};Persist Security Info=False;";
                using (var connection = new OleDbConnection(connectionString))
                {
                    await connection.OpenAsync();
                    
                    // Vérifier et mettre à jour chaque table définie dans la configuration
                    foreach (var table in config.Tables)
                    {
                        // Vérifier si la table existe
                        bool tableExists = false;
                        using (var command = new OleDbCommand(
                            "SELECT COUNT(*) FROM MSysObjects WHERE Type=1 AND Name=?", connection))
                        {
                            command.Parameters.AddWithValue("@Name", table.Name);
                            int count = Convert.ToInt32(await command.ExecuteScalarAsync());
                            tableExists = count > 0;
                        }

                        if (!tableExists)
                        {
                            // Créer la table si elle n'existe pas
                            using (var command = new OleDbCommand(table.CreateTableSql, connection))
                            {
                                await command.ExecuteNonQueryAsync();
                            }
                        }
                        else
                        {
                            // Vérifier et ajouter les colonnes manquantes
                            foreach (var column in table.Columns)
                            {
                                bool columnExists = false;
                                using (var command = new OleDbCommand(
                                    "SELECT COUNT(*) FROM MSysObjects o " +
                                    "INNER JOIN MSysColumns c ON o.Id = c.TableId " +
                                    "WHERE o.Name=? AND c.Name=?", connection))
                                {
                                    command.Parameters.AddWithValue("@TableName", table.Name);
                                    command.Parameters.AddWithValue("@ColumnName", column.Name);
                                    int count = Convert.ToInt32(await command.ExecuteScalarAsync());
                                    columnExists = count > 0;
                                }

                                if (!columnExists)
                                {
                                    // Ajouter la colonne manquante
                                    string nullableText = column.IsNullable ? "" : " NOT NULL";
                                    string alterSql = $"ALTER TABLE {table.Name} ADD COLUMN {column.Name} {column.SqlType}{nullableText}";
                                    
                                    using (var command = new OleDbCommand(alterSql, connection))
                                    {
                                        await command.ExecuteNonQueryAsync();
                                    }
                                }
                            }
                        }
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                LogManager.Error($"Erreur lors de la mise à jour de la structure de la base de données : {ex.Message}", ex);
                return false;
            }
        }
    }
}
