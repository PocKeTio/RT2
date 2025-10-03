using System;
using System.Globalization;
using System.IO;
using System.Text;

namespace RecoTool.Infrastructure.Logging
{
    public static class LogHelper
    {
        private static string EnsureAppDir()
        {
            var dir = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "RecoTool");
            if (!Directory.Exists(dir)) Directory.CreateDirectory(dir);
            return dir;
        }

        public static void WriteAction(string action, string details)
        {
            try
            {
                var dir = EnsureAppDir();
                var path = Path.Combine(dir, "actions.log");
                var user = Environment.UserName;
                var line = $"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)}\t{user}\t{action}\t{details}";
                File.AppendAllLines(path, new[] { line }, Encoding.UTF8);
            }
            catch { /* swallow logging errors */ }
        }

        public static void WritePerf(string area, string details)
        {
            try
            {
                var dir = EnsureAppDir();
                var path = Path.Combine(dir, "perf.log");
                var line = $"{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)}\t{area}\t{details}";
                File.AppendAllLines(path, new[] { line }, Encoding.UTF8);
            }
            catch { /* swallow logging errors */ }
        }

        public static void WriteRuleApplied(string origin, string countryId, string recoId, string ruleId, string outputs, string message)
        {
            try
            {
                var dir = EnsureAppDir();
                var file = $"rules-{DateTime.Now.ToString("yyyyMMdd", CultureInfo.InvariantCulture)}.log";
                var path = Path.Combine(dir, file);
                var user = Environment.UserName;
                var ts = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
                // Columns: ts, user, origin(import/edit/run-now), country, recoId, ruleId, outputs, message
                var safeOutputs = outputs?.Replace('\t', ' ');
                var safeMsg = message?.Replace('\t', ' ');
                var line = string.Join("\t", new[] { ts, user, origin ?? string.Empty, countryId ?? string.Empty, recoId ?? string.Empty, ruleId ?? string.Empty, safeOutputs ?? string.Empty, safeMsg ?? string.Empty });
                File.AppendAllLines(path, new[] { line }, Encoding.UTF8);
            }
            catch { /* best-effort */ }
        }
    }
}
