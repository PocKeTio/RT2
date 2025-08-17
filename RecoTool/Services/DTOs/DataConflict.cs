using System;

namespace RecoTool.Services
{
    #region Helper Classes

    /// <summary>
    /// Représente un conflit de données
    /// </summary>
    public class DataConflict
    {
        public string TableName { get; set; }
        public string RecordId { get; set; }
        public string FieldName { get; set; }
        public object LocalValue { get; set; }
        public object ServerValue { get; set; }
        public DateTime LocalModifiedAt { get; set; }
        public DateTime ServerModifiedAt { get; set; }
        public string LocalModifiedBy { get; set; }
        public string ServerModifiedBy { get; set; }
    }

    #endregion
}
