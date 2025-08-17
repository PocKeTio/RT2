using System;

namespace RecoTool.Models
{
    /// <summary>
    /// Maps to T_Ref_User_Fields_Preference
    /// Stores user-created (but globally visible) saved views for the reconciliation grid
    /// </summary>
    public class UserFieldsPreference
    {
        // Access Autonumber
        public int UPF_id { get; set; }
        public string UPF_Name { get; set; }
        public string UPF_user { get; set; }
        /// <summary>
        /// Optional SQL filter/order used to build the dataset (WHERE/ORDER BY part or full query snapshot)
        /// </summary>
        public string UPF_SQL { get; set; }
        /// <summary>
        /// JSON payload of column settings: visibility, displayIndex, widths, sort descriptions, etc.
        /// </summary>
        public string UPF_ColumnWidths { get; set; }
        
        // Convenience
        public DateTime? CreationDate { get; set; }
        public DateTime? LastModified { get; set; }
    }
}
