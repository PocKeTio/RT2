using System;
using System.Collections.Generic;

namespace OfflineFirstAccess.Models
{
    /// <summary>
    /// Résultat d'une opération de synchronisation avec informations de diagnostic
    /// </summary>
    public class SyncResult
    {
        /// <summary>
        /// Constructeur par défaut
        /// </summary>
        public SyncResult()
        {
            StartTime = DateTime.Now;
        }

        /// <summary>
        /// Indique si la synchronisation a réussi
        /// </summary>
        public bool Success { get; set; }

        /// <summary>
        /// Message d'information ou d'erreur
        /// </summary>
        public string Message { get; set; }

        /// <summary>
        /// Détails complémentaires en cas d'erreur
        /// </summary>
        public string ErrorDetails { get; set; }

        /// <summary>
        /// Exception capturée lors de la synchronisation
        /// </summary>
        public Exception Exception { get; set; }

        /// <summary>
        /// Nombre de conflits résolus
        /// </summary>
        public int ConflictsResolved { get; set; }

        /// <summary>
        /// Nombre d'entités envoyées vers le serveur
        /// </summary>
        public int PushedChanges { get; set; }

        /// <summary>
        /// Nombre d'entités récupérées du serveur
        /// </summary>
        public int PulledChanges { get; set; }

        /// <summary>
        /// Liste des conflits non résolus à la fin de la synchronisation.
        /// </summary>
        public List<Conflict> UnresolvedConflicts { get; set; } = new List<Conflict>();

        /// <summary>
        /// Date et heure de la synchronisation
        /// </summary>
        public DateTime SyncTime { get; set; } = DateTime.Now;

        /// <summary>
        /// Durée totale de la synchronisation en millisecondes
        /// </summary>
        public long SyncTimeMs { get; set; }

        /// <summary>
        /// Heure de début de la synchronisation
        /// </summary>
        public DateTime StartTime { get; private set; }

        /// <summary>
        /// Heure de fin de la synchronisation
        /// </summary>
        public DateTime EndTime
        {
            get
            {
                if (SyncTimeMs > 0)
                    return StartTime.AddMilliseconds(SyncTimeMs);
                return DateTime.Now;
            }
        }

        /// <summary>
        /// Nombre total d'entités traitées (envoyées + reçues)
        /// </summary>
        public int TotalEntitiesProcessed => PushedChanges + PulledChanges;

        /// <summary>
        /// Taux de traitement moyen des entités (entités par seconde)
        /// </summary>
        public double EntitiesPerSecond
        {
            get
            {
                if (SyncTimeMs <= 0) return 0;
                return TotalEntitiesProcessed / (SyncTimeMs / 1000.0);
            }
        }
    }

    /// <summary>
    /// Représente un conflit de synchronisation
    /// </summary>
    public class SyncConflict
    {
        /// <summary>
        /// Nom de la table concernée
        /// </summary>
        public string TableName { get; set; }
        
        /// <summary>
        /// ID de l'enregistrement en conflit
        /// </summary>
        public string RecordId { get; set; }
        
        /// <summary>
        /// Type de conflit
        /// </summary>
        public string ConflictType { get; set; }
        
        /// <summary>
        /// Description du conflit
        /// </summary>
        public string Description { get; set; }
        
        /// <summary>
        /// Indique si le conflit a été résolu
        /// </summary>
        public bool IsResolved { get; set; }
        
        /// <summary>
        /// Stratégie utilisée pour résoudre le conflit
        /// </summary>
        public string ResolutionStrategy { get; set; }
    }
}
