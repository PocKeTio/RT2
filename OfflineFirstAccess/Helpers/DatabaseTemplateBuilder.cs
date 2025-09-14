using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using OfflineFirstAccess.Models;

namespace OfflineFirstAccess.Helpers
{
    /// <summary>
    /// Classe facilitant la création de bases de données template avec une API fluide
    /// </summary>
    public class DatabaseTemplateBuilder
    {
        private readonly DatabaseConfiguration _config;
        private readonly string _templatePath;

        /// <summary>
        /// Crée un nouveau builder de template de base de données
        /// </summary>
        /// <param name="templatePath">Chemin où la base de données template sera créée</param>
        public DatabaseTemplateBuilder(string templatePath)
        {
            _templatePath = templatePath ?? throw new ArgumentNullException(nameof(templatePath));
            _config = new DatabaseConfiguration
            {
                LocalStoragePath = Path.GetDirectoryName(templatePath)
            };
            
            // Ajouter les tables système par défaut
            AddSystemTables();
        }

        /// <summary>
        /// Ajoute les tables système nécessaires au fonctionnement offline-first
        /// </summary>
        private void AddSystemTables()
        {
            // ChangeLog local pour durabilité hors-ligne
            AddTable("ChangeLog")
                .WithPrimaryKey("ChangeID", typeof(long), true)
                .WithColumn("TableName", typeof(string), true)
                .WithColumn("RecordID", typeof(string), true)
                .WithColumn("Operation", typeof(string), "TEXT(255)", true)
                // Stocké en DATETIME (UTC) pour un typage correct
                .WithColumn("Timestamp", typeof(DateTime), "DATETIME", true)
                .WithColumn("Synchronized", typeof(bool), true)
                .EndTable();

            // Table des verrous (SyncLocks)
            AddTable("SyncLocks")
                .WithPrimaryKey("LockID", typeof(string), false)
                .WithColumn("Reason", typeof(string), false)
                .WithColumn("CreatedAt", typeof(DateTime), false)
                .WithColumn("ExpiresAt", typeof(DateTime), true)
                .WithColumn("MachineName", typeof(string), "TEXT(100)", true)
                .WithColumn("ProcessId", typeof(long), true)
                .WithColumn("SyncStatus", typeof(string), "TEXT(50)", true)
                .WithLastModifiedColumn("CreatedAt")
                .EndTable();

            // Table des sessions (Sessions)
            AddTable("Sessions")
                .WithPrimaryKey("SessionID", typeof(string), false)
                .WithColumn("UserID", typeof(string), false)
                .WithColumn("MachineName", typeof(string), false)
                .WithColumn("StartTime", typeof(DateTime), false)
                .WithColumn("LastActivity", typeof(DateTime), false)
                .WithColumn("IsActive", typeof(bool), false)
                .WithLastModifiedColumn("LastActivity")
                .EndTable();
        }

        /// <summary>
        /// Commence la définition d'une nouvelle table
        /// </summary>
        /// <param name="tableName">Nom de la table</param>
        /// <returns>Un builder de table pour configurer la table</returns>
        public TableBuilder AddTable(string tableName)
        {
            return new TableBuilder(this, tableName);
        }

        /// <summary>
        /// Crée la base de données template avec la structure définie
        /// </summary>
        /// <returns>True si la création a réussi, False sinon</returns>
        public async Task<bool> CreateTemplateAsync()
        {
            return await DatabaseTemplateGenerator.CreateDatabaseTemplateAsync(_templatePath, _config);
        }

        /// <summary>
        /// Vérifie si une base de données existante correspond à la structure définie
        /// </summary>
        /// <param name="databasePath">Chemin de la base de données à vérifier</param>
        /// <returns>True si la structure correspond, False sinon</returns>
        public async Task<bool> ValidateDatabaseAsync(string databasePath)
        {
            return await DatabaseTemplateGenerator.ValidateDatabaseStructureAsync(databasePath, _config);
        }

        /// <summary>
        /// Met à jour la structure d'une base de données existante pour correspondre à la structure définie
        /// </summary>
        /// <param name="databasePath">Chemin de la base de données à mettre à jour</param>
        /// <returns>True si la mise à jour a réussi, False sinon</returns>
        public async Task<bool> UpdateDatabaseAsync(string databasePath)
        {
            return await DatabaseTemplateGenerator.UpdateDatabaseStructureAsync(databasePath, _config);
        }

        /// <summary>
        /// Obtient la configuration de base de données générée
        /// </summary>
        /// <returns>La configuration de base de données</returns>
        public DatabaseConfiguration GetConfiguration()
        {
            return _config;
        }

        /// <summary>
        /// Builder pour configurer une table
        /// </summary>
        public class TableBuilder
        {
            private readonly DatabaseTemplateBuilder _parentBuilder;
            private readonly TableConfiguration _tableConfig;

            internal TableBuilder(DatabaseTemplateBuilder parentBuilder, string tableName)
            {
                _parentBuilder = parentBuilder;
                _tableConfig = new TableConfiguration
                {
                    Name = tableName,
                    Columns = new List<ColumnDefinition>()
                };
            }

            /// <summary>
            /// Définit la colonne de clé primaire de la table
            /// </summary>
            /// <param name="columnName">Nom de la colonne</param>
            /// <param name="dataType">Type de données</param>
            /// <param name="autoIncrement">Indique si la colonne est auto-incrémentée</param>
            /// <returns>Le builder de table pour chaîner les appels</returns>
            public TableBuilder WithPrimaryKey(string columnName, Type dataType, bool autoIncrement = false)
            {
                _tableConfig.PrimaryKeyColumn = columnName;
                _tableConfig.PrimaryKeyType = dataType;

                _tableConfig.Columns.Add(new ColumnDefinition(
                    columnName,
                    dataType,
                    GetSqlType(dataType),
                    false,
                    true,
                    autoIncrement
                ));

                return this;
            }

            /// <summary>
            /// Ajoute une colonne à la table
            /// </summary>
            /// <param name="columnName">Nom de la colonne</param>
            /// <param name="dataType">Type de données</param>
            /// <param name="isNullable">Indique si la colonne peut être nulle</param>
            /// <returns>Le builder de table pour chaîner les appels</returns>
            public TableBuilder WithColumn(string columnName, Type dataType, bool isNullable = true)
            {
                _tableConfig.Columns.Add(new ColumnDefinition(
                    columnName,
                    dataType,
                    GetSqlType(dataType),
                    isNullable,
                    false,
                    false
                ));

                return this;
            }

            /// <summary>
            /// Ajoute une colonne à la table avec un type SQL personnalisé
            /// </summary>
            /// <param name="columnName">Nom de la colonne</param>
            /// <param name="dataType">Type de données .NET</param>
            /// <param name="sqlType">Type SQL à utiliser (ex: TEXT(50))</param>
            /// <param name="isNullable">Indique si la colonne peut être nulle</param>
            /// <returns>Le builder de table pour chaîner les appels</returns>
            public TableBuilder WithColumn(string columnName, Type dataType, string sqlType, bool isNullable = true)
            {
                _tableConfig.Columns.Add(new ColumnDefinition(
                    columnName,
                    dataType,
                    sqlType,
                    isNullable,
                    false,
                    false
                ));

                return this;
            }

            /// <summary>
            /// Définit la colonne de date de dernière modification
            /// </summary>
            /// <param name="columnName">Nom de la colonne (par défaut: LastModified)</param>
            /// <returns>Le builder de table pour chaîner les appels</returns>
            public TableBuilder WithLastModifiedColumn(string columnName = "LastModified")
            {
                _tableConfig.LastModifiedColumn = columnName;

                // S'assurer que la colonne existe
                if (!_tableConfig.Columns.Exists(c => c.Name == columnName))
                {
                    _tableConfig.Columns.Add(new ColumnDefinition(
                        columnName,
                        typeof(DateTime),
                        "DATETIME",
                        false,
                        false,
                        false
                    ));
                }

                return this;
            }

            /// <summary>
            /// Termine la définition de la table et retourne au builder de template
            /// </summary>
            /// <returns>Le builder de template pour continuer la configuration</returns>
            public DatabaseTemplateBuilder EndTable()
            {
                // Générer le SQL de création de table
                GenerateCreateTableSql();

                // Ajouter la table à la configuration
                int existingIndex = _parentBuilder._config.Tables.FindIndex(t => t.Name == _tableConfig.Name);
                if (existingIndex >= 0)
                {
                    _parentBuilder._config.Tables[existingIndex] = _tableConfig;
                }
                else
                {
                    _parentBuilder._config.Tables.Add(_tableConfig);
                }

                return _parentBuilder;
            }

            /// <summary>
            /// Génère le SQL de création de table
            /// </summary>
            private void GenerateCreateTableSql()
            {
                var columnDefs = new List<string>();

                foreach (var column in _tableConfig.Columns)
                {
                    var columnDef = $"{column.Name} {column.SqlType}";

                    if (column.IsPrimaryKey && !column.IsAutoIncrement)
                        columnDef += " PRIMARY KEY";

                    if (column.IsAutoIncrement)
                        columnDef = column.Name + " COUNTER PRIMARY KEY";

                    if (!column.IsNullable)
                        columnDef += " NOT NULL";

                    columnDefs.Add(columnDef);
                }

                _tableConfig.CreateTableSql = $"CREATE TABLE {_tableConfig.Name} (\n    " +
                                             string.Join(",\n    ", columnDefs) +
                                             "\n)";
            }

            /// <summary>
            /// Convertit un type .NET en type SQL Access
            /// </summary>
            /// <param name="type">Type .NET</param>
            /// <returns>Type SQL Access correspondant</returns>
            private string GetSqlType(Type type)
            {
                if (type == typeof(int) || type == typeof(long))
                    return "LONG";
                if (type == typeof(string))
                    return "TEXT(255)";
                if (type == typeof(DateTime))
                    return "DATETIME";
                if (type == typeof(bool))
                    return "BIT";
                if (type == typeof(decimal) || type == typeof(double) || type == typeof(float))
                    return "DOUBLE";
                if (type == typeof(byte[]))
                    return "BINARY";
                if (type == typeof(Guid))
                    return "TEXT(36)";

                return "TEXT(255)";
            }
        }
    }
}
