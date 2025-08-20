using GenericRecord = System.Collections.Generic.Dictionary<string, object>;

namespace OfflineFirstAccess.Models
{
    public class Conflict
    {
        public string TableName { get; set; }
        public string RecordId { get; set; }
        public GenericRecord LocalVersion { get; set; }
        public GenericRecord RemoteVersion { get; set; }
        public string ConflictType { get; set; } // e.g., "Update-Update", "Update-Delete"
    }
}
