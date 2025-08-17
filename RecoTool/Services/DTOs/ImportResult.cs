using System;
using System.Collections.Generic;
using System.Linq;

namespace RecoTool.Services
{
    /// <summary>
    /// Résultat d'un import Ambre
    /// </summary>
    public class ImportResult
    {
        public string CountryId { get; set; }
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }
        public bool IsSuccess { get; set; }
        public int ProcessedRecords { get; set; }
        public int NewRecords { get; set; }
        public int UpdatedRecords { get; set; }
        public int DeletedRecords { get; set; }
        public List<string> Errors { get; set; } = new List<string>();
        public List<string> ValidationErrors { get; set; } = new List<string>();
        
        public TimeSpan Duration => EndTime - StartTime;
        public bool HasErrors => Errors.Any() || ValidationErrors.Any();
    }
}
