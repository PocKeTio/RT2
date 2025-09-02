using System;

namespace RecoTool.Models
{
    /// <summary>
    /// Classe de base pour toutes les entités avec gestion de la synchronisation
    /// </summary>
    public abstract class BaseEntity
    {
        public DateTime? CreationDate { get; set; }
        public DateTime? DeleteDate { get; set; }
        public string ModifiedBy { get; set; }
        public DateTime? LastModified { get; set; }
        public int Version { get; set; }

        protected BaseEntity()
        {
            CreationDate = DateTime.UtcNow;
            Version = 1;
        }

        /// <summary>
        /// Indique si l'entité est supprimée (logiquement)
        /// </summary>
        public bool IsDeleted => DeleteDate.HasValue;

        /// <summary>
        /// Marque l'entité comme supprimée
        /// </summary>
        public virtual void MarkAsDeleted()
        {
            DeleteDate = DateTime.UtcNow;
        }

        /// <summary>
        /// Met à jour les informations de modification
        /// </summary>
        /// <param name="modifiedBy">Utilisateur qui modifie</param>
        public virtual void UpdateModification(string modifiedBy)
        {
            ModifiedBy = modifiedBy;
            LastModified = DateTime.UtcNow;
            Version++;
        }
    }
}
