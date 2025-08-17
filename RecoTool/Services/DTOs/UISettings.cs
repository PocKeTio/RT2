namespace RecoTool.Services
{
    #region Configuration Classes

    /// <summary>
    /// Paramètres d'interface utilisateur
    /// </summary>
    public class UISettings
    {
        public int RefreshIntervalSeconds { get; set; }
        public int MaxRecordsPerPage { get; set; }
        public bool ShowProgressBar { get; set; }
        public string Theme { get; set; }
        public string Language { get; set; }
    }

    #endregion
}
