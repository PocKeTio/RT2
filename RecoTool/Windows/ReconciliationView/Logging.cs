using System.Windows;
using RecoTool.Infrastructure.Logging;

namespace RecoTool.Windows
{
    // Partial: Logging and error helpers for ReconciliationView
    public partial class ReconciliationView
    {
        private void ShowError(string message)
        {
            MessageBox.Show(message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }

        private void LogAction(string action, string details)
        {
            LogHelper.WriteAction(action, details);
        }

        // Append performance diagnostics to %APPDATA%/RecoTool/perf.log
        private void LogPerf(string area, string details)
        {
            LogHelper.WritePerf(area, details);
        }
    }
}
