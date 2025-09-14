using System;
using System.IO;

namespace RecoTool.Services
{
    // Partial: diagnostics and logging helpers for OfflineFirst
    public partial class OfflineFirstService
    {
        /// <summary>
        /// Diagnostics détaillés sur l'état de synchronisation ZIP AMBRE (réseau vs cache local).
        /// </summary>
        public string GetAmbreZipDiagnostics(string countryId)
        {
            if (string.IsNullOrWhiteSpace(countryId)) return "countryId manquant";
            try
            {
                var net = GetNetworkAmbreZipPath(countryId);
                var loc = GetLocalAmbreZipCachePath(countryId);
                var sb = new System.Text.StringBuilder();
                sb.AppendLine($"Network ZIP: {(string.IsNullOrWhiteSpace(net) ? "(introuvable)" : net)}");
                if (!string.IsNullOrWhiteSpace(net) && File.Exists(net))
                {
                    var fi = new FileInfo(net);
                    sb.AppendLine($"  Size: {fi.Length:N0} bytes");
                    sb.AppendLine($"  LastWriteUtc: {fi.LastWriteTimeUtc:O}");
                }
                sb.AppendLine($"Local ZIP: {(string.IsNullOrWhiteSpace(loc) ? "(introuvable)" : loc)}");
                if (!string.IsNullOrWhiteSpace(loc) && File.Exists(loc))
                {
                    var fi = new FileInfo(loc);
                    sb.AppendLine($"  Size: {fi.Length:N0} bytes");
                    sb.AppendLine($"  LastWriteUtc: {fi.LastWriteTimeUtc:O}");
                }
                return sb.ToString();
            }
            catch (Exception ex)
            {
                return $"Diagnostics AMBRE indisponibles: {ex.Message}";
            }
        }

        /// <summary>
        /// Diagnostics détaillés sur l'état de synchronisation ZIP DW (réseau vs cache local).
        /// </summary>
        public string GetDwZipDiagnostics(string countryId)
        {
            if (string.IsNullOrWhiteSpace(countryId)) return "countryId manquant";
            try
            {
                var net = GetNetworkDwZipPath(countryId);
                var loc = GetLocalDwZipCachePath(countryId);
                var sb = new System.Text.StringBuilder();
                sb.AppendLine($"Network ZIP: {(string.IsNullOrWhiteSpace(net) ? "(introuvable)" : net)}");
                if (!string.IsNullOrWhiteSpace(net) && File.Exists(net))
                {
                    var fi = new FileInfo(net);
                    sb.AppendLine($"  Size: {fi.Length:N0} bytes");
                    sb.AppendLine($"  LastWriteUtc: {fi.LastWriteTimeUtc:O}");
                }
                sb.AppendLine($"Local ZIP: {(string.IsNullOrWhiteSpace(loc) ? "(introuvable)" : loc)}");
                if (!string.IsNullOrWhiteSpace(loc) && File.Exists(loc))
                {
                    var fi = new FileInfo(loc);
                    sb.AppendLine($"  Size: {fi.Length:N0} bytes");
                    sb.AppendLine($"  LastWriteUtc: {fi.LastWriteTimeUtc:O}");
                }
                return sb.ToString();
            }
            catch (Exception ex)
            {
                return $"Diagnostics DW indisponibles: {ex.Message}";
            }
        }
    }
}
