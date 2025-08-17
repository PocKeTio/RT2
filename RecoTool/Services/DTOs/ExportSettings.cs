namespace RecoTool.Services
{
    #region Configuration Classes

    /// <summary>
    /// Paramètres d'export
    /// </summary>
    public class ExportSettings
    {
        public string DefaultExportDirectory { get; set; }
        public ExportFormat DefaultFileFormat { get; set; }
        public bool IncludeHeaders { get; set; }
        public string DateFormat { get; set; }
        public string DecimalSeparator { get; set; }
    }

    #endregion
}
