using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using OfflineFirstAccess.Models;

namespace OfflineFirstAccess.Helpers
{
    /// <summary>
    /// Convertisseur entre les entités et les enregistrements de base de données
    /// </summary>
    public static class EntityConverter
    {
        /// <summary>
        /// Convertit un DataReader en entité
        /// </summary>
        public static Entity ConvertToEntity(IDataReader reader, TableConfiguration tableConfig)
        {
            var entity = new Entity
            {
                TableName = tableConfig.Name,
                PrimaryKeyColumn = tableConfig.PrimaryKeyColumn,
                LastModifiedColumn = tableConfig.LastModifiedColumn,
                VersionColumn = tableConfig.VersionColumn
            };

            // Lire toutes les colonnes du reader
            for (int i = 0; i < reader.FieldCount; i++)
            {
                string columnName = reader.GetName(i);
                object value = reader[i];

                // Convertir DBNull en null
                if (value == DBNull.Value)
                {
                    value = null;
                }

                entity.Properties[columnName] = value;
            }

            return entity;
        }

        /// <summary>
        /// Ajoute les paramètres d'une entité à une commande OleDb
        /// </summary>
        public static void AddParametersFromEntity(OleDbCommand command, Entity entity, TableConfiguration tableConfig, bool includePrimaryKey = true)
        {
            foreach (var column in tableConfig.Columns)
            {
                // Exclure la clé primaire si demandé
                if (!includePrimaryKey && column.Name == tableConfig.PrimaryKeyColumn)
                    continue;

                object value = DBNull.Value;

                if (entity.Properties.ContainsKey(column.Name))
                {
                    value = entity.Properties[column.Name] ?? DBNull.Value;

                    // Gérer les conversions de types spécifiques
                    if (value != DBNull.Value)
                    {
                        // S'assurer que les dates sont dans un format acceptable
                        if (value is DateTime dateValue)
                        {
                            value = dateValue.ToOADate();
                        }
                        else if (value is bool boolValue)
                        {
                            value = boolValue ? 1 : 0;
                        }
                    }
                }
                else
                {
                    if (value is bool boolValue)
                    {
                        value = 0;
                    }
                    else if (value is DateTime dateValue)
                    {
                        value = DateTime.MinValue.ToOADate();
                    }

                }

                command.Parameters.AddWithValue($"@{column.Name}", value);
            }
        }

        /// <summary>
        /// Obtient les valeurs des colonnes d'une entité dans l'ordre défini par la configuration
        /// </summary>
        public static object[] GetColumnValuesFromEntity(Entity entity, TableConfiguration tableConfig)
        {
            var values = new object[tableConfig.Columns.Count];

            for (int i = 0; i < tableConfig.Columns.Count; i++)
            {
                var column = tableConfig.Columns[i];

                if (entity.Properties.ContainsKey(column.Name))
                {
                    values[i] = entity.Properties[column.Name];
                }
                else
                {
                    values[i] = DBNull.Value;
                }
            }

            return values;
        }

        /// <summary>
        /// Crée une copie profonde d'une entité
        /// </summary>
        public static Entity CloneEntity(Entity source)
        {
            var clone = new Entity
            {
                TableName = source.TableName,
                PrimaryKeyColumn = source.PrimaryKeyColumn,
                LastModifiedColumn = source.LastModifiedColumn,
                VersionColumn = source.VersionColumn
            };

            foreach (var kvp in source.Properties)
            {
                // Cloner les valeurs pour éviter les références partagées
                if (kvp.Value is ICloneable cloneable)
                {
                    clone.Properties[kvp.Key] = cloneable.Clone();
                }
                else
                {
                    clone.Properties[kvp.Key] = kvp.Value;
                }
            }

            return clone;
        }

        /// <summary>
        /// Compare deux entités pour détecter les différences
        /// </summary>
        public static Dictionary<string, (object oldValue, object newValue)> CompareEntities(Entity entity1, Entity entity2)
        {
            var differences = new Dictionary<string, (object, object)>();

            // Vérifier toutes les propriétés de entity1
            foreach (var prop in entity1.Properties)
            {
                if (!entity2.Properties.ContainsKey(prop.Key))
                {
                    differences[prop.Key] = (prop.Value, null);
                }
                else if (!AreValuesEqual(prop.Value, entity2.Properties[prop.Key]))
                {
                    differences[prop.Key] = (prop.Value, entity2.Properties[prop.Key]);
                }
            }

            // Vérifier les propriétés qui existent uniquement dans entity2
            foreach (var prop in entity2.Properties)
            {
                if (!entity1.Properties.ContainsKey(prop.Key))
                {
                    differences[prop.Key] = (null, prop.Value);
                }
            }

            return differences;
        }

        /// <summary>
        /// Compare deux valeurs pour l'égalité
        /// </summary>
        private static bool AreValuesEqual(object value1, object value2)
        {
            // Si les deux sont null, ils sont égaux
            if (value1 == null && value2 == null)
                return true;

            // Si un seul est null, ils ne sont pas égaux
            if (value1 == null || value2 == null)
                return false;

            // Comparaison spéciale pour les dates (ignorer les millisecondes)
            if (value1 is DateTime date1 && value2 is DateTime date2)
            {
                return Math.Abs((date1 - date2).TotalSeconds) < 1;
            }

            // Comparaison spéciale pour les nombres décimaux
            if (IsNumericType(value1) && IsNumericType(value2))
            {
                double d1 = Convert.ToDouble(value1);
                double d2 = Convert.ToDouble(value2);
                return Math.Abs(d1 - d2) < 0.0001;
            }

            // Comparaison standard
            return value1.Equals(value2);
        }

        /// <summary>
        /// Vérifie si un type est numérique
        /// </summary>
        private static bool IsNumericType(object o)
        {
            return o is byte || o is sbyte ||
                   o is short || o is ushort ||
                   o is int || o is uint ||
                   o is long || o is ulong ||
                   o is float || o is double ||
                   o is decimal;
        }
    }
}