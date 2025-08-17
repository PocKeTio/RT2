using System.Collections.Generic;

namespace RecoTool.Models
{
    /// <summary>
    /// Classe pour stocker les changements calculés lors d'un import Ambre
    /// </summary>
    public class ImportChanges
    {
        /// <summary>
        /// Enregistrements à ajouter
        /// </summary>
        public List<DataAmbre> ToAdd { get; set; } = new List<DataAmbre>();

        /// <summary>
        /// Enregistrements à mettre à jour
        /// </summary>
        public List<DataAmbre> ToUpdate { get; set; } = new List<DataAmbre>();

        /// <summary>
        /// Enregistrements à archiver (suppression logique)
        /// </summary>
        public List<DataAmbre> ToArchive { get; set; } = new List<DataAmbre>();

        /// <summary>
        /// Nombre total de changements
        /// </summary>
        public int TotalChanges => ToAdd.Count + ToUpdate.Count + ToArchive.Count;
    }
}
