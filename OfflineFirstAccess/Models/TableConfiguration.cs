using System;
using System.Collections.Generic;

namespace OfflineFirstAccess.Models
{
    /// <summary>
    /// Configuration d'une table dans le système offline-first
    /// </summary>
    public class TableConfiguration
    {
        /// <summary>
        /// Nom de la table
        /// </summary>
        public string Name { get; set; }
        
        /// <summary>
        /// Nom de la colonne utilisée comme clé primaire
        /// </summary>
        public string PrimaryKeyColumn { get; set; }
        
        /// <summary>
        /// Type de la clé primaire
        /// </summary>
        public Type PrimaryKeyType { get; set; }
        
        /// <summary>
        /// Nom de la colonne qui stocke la date de dernière modification
        /// </summary>
        public string LastModifiedColumn { get; set; } = "LastModified";
        
        /// <summary>
        /// Nom de la colonne qui stocke le numéro de version de l'entité
        /// </summary>
        public string VersionColumn { get; set; } = "Version";
        
        /// <summary>
        /// Liste des colonnes de la table
        /// </summary>
        public List<ColumnDefinition> Columns { get; set; } = new List<ColumnDefinition>();
        
        /// <summary>
        /// Script SQL pour créer la table
        /// </summary>
        public string CreateTableSql { get; set; }
        
        /// <summary>
        /// Requête SQL pour sélectionner toutes les entités
        /// </summary>
        public string SelectAllSql => $"SELECT * FROM {Name}";
        
        /// <summary>
        /// Requête SQL pour sélectionner une entité par sa clé primaire
        /// </summary>
        public string SelectByIdSql => $"SELECT * FROM {Name} WHERE {PrimaryKeyColumn} = ?";
        
        /// <summary>
        /// Requête SQL pour sélectionner les entités modifiées depuis une certaine date
        /// </summary>
        public string SelectChangedSinceSql => $"SELECT * FROM {Name} WHERE {LastModifiedColumn} > ?";
        
        /// <summary>
        /// Génère la requête SQL d'insertion
        /// </summary>
        public string GenerateInsertSql()
        {
            var columnNames = new List<string>();
            var paramPlaceholders = new List<string>();
            
            foreach (var column in Columns)
            {
                columnNames.Add("[" + column.Name + "]");
                paramPlaceholders.Add("?");
            }
            
            return $"INSERT INTO {Name} ({string.Join(", ", columnNames)}) VALUES ({string.Join(", ", paramPlaceholders)})";
        }
        
        /// <summary>
        /// Génère la requête SQL de mise à jour
        /// </summary>
        public string GenerateUpdateSql()
        {
            var setStatements = new List<string>();
            
            foreach (var column in Columns)
            {
                if (column.Name != PrimaryKeyColumn) // Ne pas mettre à jour la clé primaire
                {
                    setStatements.Add($"{column.Name} = ?");
                }
            }
            
            return $"UPDATE {Name} SET {string.Join(", ", setStatements)} WHERE {PrimaryKeyColumn} = ?";
        }
        
        /// <summary>
        /// Requête SQL pour supprimer une entité
        /// </summary>
        public string DeleteSql => $"DELETE FROM {Name} WHERE {PrimaryKeyColumn} = ?";

        public string DeleteDateColumn { get; internal set; } = "DeleteDate";
    }
    
    /// <summary>
    /// Définition d'une colonne dans une table
    /// </summary>
    public class ColumnDefinition
    {
        /// <summary>
        /// Nom de la colonne
        /// </summary>
        public string Name { get; set; }
        
        /// <summary>
        /// Type de données de la colonne
        /// </summary>
        public Type DataType { get; set; }
        
        /// <summary>
        /// Type de données SQL (ex: INTEGER, TEXT, etc.)
        /// </summary>
        public string SqlType { get; set; }
        
        /// <summary>
        /// Indique si la colonne peut être nulle
        /// </summary>
        public bool IsNullable { get; set; } = true;
        
        /// <summary>
        /// Indique si la colonne est une clé primaire
        /// </summary>
        public bool IsPrimaryKey { get; set; } = false;
        
        /// <summary>
        /// Indique si la colonne est auto-incrémentée
        /// </summary>
        public bool IsAutoIncrement { get; set; } = false;
        
        /// <summary>
        /// Constructeur
        /// </summary>
        public ColumnDefinition(string name, Type dataType, string sqlType, bool isNullable = true, bool isPrimaryKey = false, bool isAutoIncrement = false)
        {
            Name = name;
            DataType = dataType;
            SqlType = sqlType;
            IsNullable = isNullable;
            IsPrimaryKey = isPrimaryKey;
            IsAutoIncrement = isAutoIncrement;
        }
    }
}
