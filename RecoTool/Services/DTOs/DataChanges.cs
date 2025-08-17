using System.Collections.Generic;
using RecoTool.Models;

namespace RecoTool.Services
{
    /// <summary>
    /// Structure pour gérer les changements de données
    /// </summary>
    internal class DataChanges
    {
        public List<DataAmbre> NewRecords { get; set; } = new List<DataAmbre>();
        public List<DataAmbre> UpdatedRecords { get; set; } = new List<DataAmbre>();
        public List<DataAmbre> DeletedRecords { get; set; } = new List<DataAmbre>();
    }
}
