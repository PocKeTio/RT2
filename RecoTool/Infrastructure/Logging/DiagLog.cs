using System;
using System.Diagnostics;
using RecoTool.Infrastructure.Logging;

namespace RecoTool.Infrastructure.Logging
{
    public static class DiagLog
    {
        public static void Info(string area, string message)
        {
            try { Debug.WriteLine($"[{area}] {message}"); } catch { }
            try { LogHelper.WritePerf(area ?? "diag", message ?? string.Empty); } catch { }
        }

        public static void Error(string area, string message, Exception ex = null)
        {
            try { Debug.WriteLine($"[{area}] ERROR: {message} - {ex?.Message}"); } catch { }
            try { LogHelper.WritePerf(area ?? "diag", $"ERROR: {message} - {ex?.Message}"); } catch { }
        }
    }
}
