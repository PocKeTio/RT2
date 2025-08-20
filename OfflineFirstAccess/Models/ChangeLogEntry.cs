using System;

namespace OfflineFirstAccess.Models
{
    public class ChangeLogEntry
    {
        public long Id { get; set; }
        public string TableName { get; set; }
        public string RecordId { get; set; }
        public string OperationType { get; set; } // e.g., "INSERT", "UPDATE", "DELETE"
        public DateTime TimestampUTC { get; set; }
    }
}
