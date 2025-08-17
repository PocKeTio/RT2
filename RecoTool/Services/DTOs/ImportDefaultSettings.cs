namespace RecoTool.Services
{
    #region Configuration Classes

    /// <summary>
    /// Paramètres d'import par défaut
    /// </summary>
    public class ImportDefaultSettings
    {
        public int StartRow { get; set; }
        public bool DetectChanges { get; set; }
        public bool ApplyTransformations { get; set; }
        public bool ApplyRules { get; set; }
        public bool CreateBackup { get; set; }
        public string BackupDirectory { get; set; }
    }

    #endregion
}
