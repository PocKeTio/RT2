using System;
using System.Collections.Generic;
using System.Dynamic;

namespace OfflineFirstAccess.Models
{
    /// <summary>
    /// Classe générique pour représenter n'importe quelle entité dans le système
    /// </summary>
    public class Entity
    {
        /// <summary>
        /// Nom de la table à laquelle cette entité appartient
        /// </summary>
        public string TableName { get; set; }
        
        /// <summary>
        /// Colonne utilisée comme identifiant primaire
        /// </summary>
        public string PrimaryKeyColumn { get; set; }

        /// <summary>
        /// Colonne utilisée pour le timestamp de modification
        /// </summary>
        public string LastModifiedColumn { get; set; } = "LastModified";

        /// <summary>
        /// Colonne utilisée pour le numéro de version
        /// </summary>
        public string VersionColumn { get; set; } = "Version";
        
        /// <summary>
        /// Dictionnaire des propriétés de l'entité
        /// Les clés correspondent aux noms de colonnes
        /// </summary>
        public Dictionary<string, object> Properties { get; set; } = new Dictionary<string, object>();
        
        /// <summary>
        /// Obtient ou définit une propriété de l'entité
        /// </summary>
        /// <param name="name">Nom de la propriété (colonne)</param>
        public object this[string name]
        {
            get => Properties.ContainsKey(name) ? Properties[name] : null;
            set => Properties[name] = value;
        }
        
        /// <summary>
        /// Obtient la valeur de l'identifiant primaire
        /// </summary>
        public object GetPrimaryKey()
        {
            return Properties.ContainsKey(PrimaryKeyColumn) ? Properties[PrimaryKeyColumn] : null;
        }
        
        /// <summary>
        /// Obtient la date de dernière modification
        /// </summary>
        public DateTime GetLastModified()
        {
            if (Properties.ContainsKey(LastModifiedColumn) && Properties[LastModifiedColumn] is DateTime date)
            {
                return date;
            }
            return DateTime.MinValue;
        }
        
        /// <summary>
        /// Obtient le numéro de version de l'entité
        /// </summary>
        /// <returns>Le numéro de version ou 0 si non défini</returns>
        public long GetVersion()
        {
            if (Properties.ContainsKey(VersionColumn))
            {
                if (Properties[VersionColumn] is long longVersion)
                    return longVersion;
                    
                if (Properties[VersionColumn] is int intVersion)
                    return intVersion;
                    
                if (Properties[VersionColumn] != null)
                {
                    // Tenter de convertir d'autres types en long
                    try
                    {
                        return Convert.ToInt64(Properties[VersionColumn]);
                    }
                    catch
                    {
                        // Ignorer les erreurs de conversion
                    }
                }
            }
            return 0;
        }
        
        /// <summary>
        /// Incrémente et définit le numéro de version de l'entité
        /// </summary>
        /// <returns>Le nouveau numéro de version</returns>
        public long IncrementVersion()
        {
            long currentVersion = GetVersion();
            long newVersion = currentVersion + 1;
            Properties[VersionColumn] = newVersion;
            return newVersion;
        }
        
        /// <summary>
        /// Met à jour la date de modification et le numéro de version
        /// </summary>
        public void UpdateVersionAndTimestamp()
        {
            // Mettre à jour la date de dernière modification
            Properties[LastModifiedColumn] = DateTime.Now;
            
            // Incrémenter le numéro de version
            IncrementVersion();
        }
        
        /// <summary>
        /// Définit la valeur du timestamp de dernière modification
        /// </summary>
        public void SetLastModified(DateTime value)
        {
            Properties[LastModifiedColumn] = value;
        }
    }
}
