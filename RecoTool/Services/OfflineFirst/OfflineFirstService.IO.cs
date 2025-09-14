using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace RecoTool.Services
{
    // Partial: IO helpers (atomic replace, async copy)
    public partial class OfflineFirstService
    {
        /// <summary>
        /// Perform File.Replace with limited retries for transient sharing violations.
        /// Cleans up backup file on success.
        /// </summary>
        private async Task FileReplaceWithRetriesAsync(string sourceFileName, string destinationFileName, string destinationBackupFileName, int maxAttempts, int initialDelayMs)
        {
            if (maxAttempts < 1) maxAttempts = 1;
            int attempt = 0;
            int delay = Math.Max(50, initialDelayMs);
            for (;;)
            {
                try
                {
                    File.Replace(sourceFileName, destinationFileName, destinationBackupFileName);
                    // Cleanup policy
                    try
                    {
                        if (!string.IsNullOrWhiteSpace(destinationBackupFileName) && File.Exists(destinationBackupFileName))
                        {
                            File.Delete(destinationBackupFileName);
                        }
                    }
                    catch { }
                    return;
                }
                catch (IOException ioex)
                {
                    attempt++;
                    // Heuristic: treat as share/lock if message hints a sharing violation or file in use
                    var msg = ioex.Message?.ToLowerInvariant() ?? string.Empty;
                    bool isSharing = msg.Contains("being used") || msg.Contains("process cannot access the file") || msg.Contains("sharing violation") || msg.Contains("used by another process");
                    if (!isSharing || attempt >= maxAttempts)
                        throw;
                    try
                    {
                        System.Diagnostics.Debug.WriteLine($"[Replace][Retry] Attempt {attempt}/{maxAttempts} failed with sharing violation. Retrying in {delay}ms...");
                        await Task.Delay(delay).ConfigureAwait(false);
                    }
                    catch { }
                    delay = Math.Min(delay * 2, 5000);
                }
            }
        }

        private static async Task CopyFileAsync(string sourceFileName, string destinationFileName, bool overwrite, CancellationToken cancellationToken = default)
        {
            Directory.CreateDirectory(Path.GetDirectoryName(destinationFileName) ?? string.Empty);
            var mode = overwrite ? FileMode.Create : FileMode.CreateNew;
            using (var source = new FileStream(sourceFileName, FileMode.Open, FileAccess.Read, FileShare.Read, 81920, useAsync: true))
            using (var dest = new FileStream(destinationFileName, mode, FileAccess.Write, FileShare.None, 81920, useAsync: true))
            {
                await source.CopyToAsync(dest, 81920, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
