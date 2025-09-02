using System;

namespace OfflineFirstAccess.Models
{
    /// <summary>
    /// Représente un paramètre de configuration stocké en base de données
    /// </summary>
    public class ConfigParameter
    {
        /// <summary>
        /// Clé unique du paramètre
        /// </summary>
        public string Key { get; set; }
        
        /// <summary>
        /// Valeur du paramètre
        /// </summary>
        public string Value { get; set; }
        
        /// <summary>
        /// Description du paramètre
        /// </summary>
        public string Description { get; set; }
        
        /// <summary>
        /// Catégorie du paramètre
        /// </summary>
        public string Category { get; set; }
        
        /// <summary>
        /// Date de dernière modification
        /// </summary>
        public DateTime LastModified { get; set; }
        
        /// <summary>
        /// Indique si le paramètre est modifiable par l'utilisateur
        /// </summary>
        public bool IsUserEditable { get; set; }
        
        /// <summary>
        /// Constructeur par défaut
        /// </summary>
        public ConfigParameter()
        {
            LastModified = DateTime.UtcNow;
            IsUserEditable = true;
        }
        
        /// <summary>
        /// Constructeur avec paramètres essentiels
        /// </summary>
        public ConfigParameter(string key, string value, string description = "", string category = "General")
        {
            Key = key;
            Value = value;
            Description = description;
            Category = category;
            LastModified = DateTime.UtcNow;
            IsUserEditable = true;
        }
    }
}
