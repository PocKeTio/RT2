using System;
using System.IO;
using System.Threading.Tasks;

namespace RecoTool.Services
{
    // Partial: snapshot copy/extraction for AMBRE and DWINGS
    public partial class OfflineFirstService
    {
        /// <summary>
        /// Variante avec reporting de progression.
        /// </summary>
        public async Task EnsureLocalSnapshotsUpToDateAsync(string countryId, Action<int, string> onProgress)
        {
            if (string.IsNullOrWhiteSpace(countryId)) throw new ArgumentException("countryId requis", nameof(countryId));
            // Safety: only operate on the currently selected country to avoid cross-country copies
            if (!string.Equals(countryId, _currentCountryId, StringComparison.OrdinalIgnoreCase))
            {
                return;
            }

            onProgress?.Invoke(0, "Vérification des instantanés locaux...");

            // Helper local pour comparer et copier si besoin
            async Task CopyIfDifferentAsync(string networkPath, string localPath)
            {
                try
                {
                    if (string.IsNullOrWhiteSpace(networkPath) || string.IsNullOrWhiteSpace(localPath)) return;
                    if (!File.Exists(networkPath)) return; // rien à copier

                    var netFi = new FileInfo(networkPath);
                    var locFi = new FileInfo(localPath);
                    bool needCopy = !locFi.Exists || !FilesAreEqual(locFi, netFi);
                    if (needCopy)
                    {
                        Directory.CreateDirectory(Path.GetDirectoryName(localPath) ?? string.Empty);
                        // Copie atomique au mieux: copier vers temp puis replace
                        string tmp = localPath + ".tmp_copy";
                        await CopyFileAsync(networkPath, tmp, overwrite: true).ConfigureAwait(false);
                        // Remplace en conservant ACL; File.Replace nécessite un backup, sinon fallback move
                        try { await FileReplaceWithRetriesAsync(tmp, localPath, localPath + ".bak", maxAttempts: 5, initialDelayMs: 200).ConfigureAwait(false); }
                        catch
                        {
                            try { if (File.Exists(localPath)) File.Delete(localPath); } catch { }
                            File.Move(tmp, localPath);
                        }
                        // Cleanup backup best-effort
                        try { var bak = localPath + ".bak"; if (File.Exists(bak)) File.Delete(bak); } catch { }
                    }
                }
                catch { /* best-effort */ }
                await Task.CompletedTask.ConfigureAwait(false);
            }

            // AMBRE (préférez ZIP si présent)
            try
            {
                onProgress?.Invoke(10, "AMBRE: vérification ZIP/copie...");
                var netAmbreZip = GetNetworkAmbreZipPath(countryId);
                var locAmbreDb = GetLocalAmbreDbPath(countryId);
                if (!string.IsNullOrWhiteSpace(netAmbreZip) && File.Exists(netAmbreZip))
                {
                    var locZip = GetLocalAmbreZipCachePath(countryId);
                    try
                    {
                        var copied = await CopyZipIfDifferentAsync(netAmbreZip, locZip);
                        if (copied)
                        {
                            onProgress?.Invoke(25, "AMBRE: extraction en cours...");
                            await ExtractAmbreZipToLocalAsync(countryId, locZip, locAmbreDb);
                            onProgress?.Invoke(35, "AMBRE: prêt");
                        }
                        else if (!File.Exists(locAmbreDb))
                        {
                            // Première extraction
                            onProgress?.Invoke(25, "AMBRE: première extraction...");
                            await ExtractAmbreZipToLocalAsync(countryId, locZip, locAmbreDb);
                            onProgress?.Invoke(35, "AMBRE: prêt");
                        }
                    }
                    catch { }
                }
                else
                {
                    // Fallback: copie brute .accdb si aucun ZIP AMBRE côté réseau
                    var netAmbre = GetNetworkAmbreDbPath(countryId);
                    await CopyIfDifferentAsync(netAmbre, locAmbreDb);
                    onProgress?.Invoke(40, "AMBRE: prêt");
                }
            }
            catch { }

            // DWINGS (préférez ZIP si présent)
            try
            {
                onProgress?.Invoke(55, "DW: vérification ZIP/copie...");
                var netDwZip = GetNetworkDwZipPath(countryId);
                var locDwDb = GetLocalDwDbPath(countryId);
                if (!string.IsNullOrWhiteSpace(netDwZip) && File.Exists(netDwZip))
                {
                    var locZip = GetLocalDwZipCachePath(countryId);
                    try
                    {
                        var copied = await CopyZipIfDifferentAsync(netDwZip, locZip);
                        if (copied)
                        {
                            onProgress?.Invoke(70, "DW: extraction en cours...");
                            await ExtractDwZipToLocalAsync(countryId, locZip, locDwDb);
                            onProgress?.Invoke(85, "DW: prêt");
                        }
                        else if (!File.Exists(locDwDb))
                        {
                            // Première extraction si DB absente
                            onProgress?.Invoke(70, "DW: première extraction...");
                            await ExtractDwZipToLocalAsync(countryId, locZip, locDwDb);
                            onProgress?.Invoke(85, "DW: prêt");
                        }
                    }
                    catch { }
                }
                else
                {
                    var netDw = GetNetworkDwDbPath(countryId);
                    var locDw = GetLocalDwDbPath(countryId);
                    await CopyIfDifferentAsync(netDw, locDw);
                    onProgress?.Invoke(90, "DW: prêt");
                }
            }
            catch { }

            onProgress?.Invoke(100, "Instantanés locaux à jour");
        }
    }
}
