using System;
using System.Data;
using System.Data.OleDb;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace RecoTool.Services
{
    // Partial: maintenance utilities (compact/repair, cleanup)
    public partial class OfflineFirstService
    {
        /// <summary>
        /// Tente un Compact & Repair de la base Access en utilisant Access.Application (COM) en late-binding.
        /// Retourne le chemin du fichier compacté si succès, sinon null. Best-effort: ne jette pas en cas d'absence d'Access.
        /// </summary>
        /// <param name="sourcePath">Chemin de la base source (.accdb)</param>
        /// <returns>Chemin du fichier compacté temporaire, ou null en cas d'échec</returns>
        private async Task<string> TryCompactAccessDatabaseAsync(string sourcePath)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(sourcePath) || !File.Exists(sourcePath)) return null;
                string dir = Path.GetDirectoryName(sourcePath);
                string nameNoExt = Path.GetFileNameWithoutExtension(sourcePath);
                string tempCompact = Path.Combine(dir ?? "", $"{nameNoExt}.compact_{Guid.NewGuid():N}.accdb");

                return await Task.Run(() =>
                {
                    try
                    {
                        // Late-bind Access.Application to avoid adding COM references
                        var accType = Type.GetTypeFromProgID("Access.Application");
                        if (accType == null) return null;
                        dynamic app = Activator.CreateInstance(accType);
                        try
                        {
                            // Some versions return bool, some void; rely on file existence afterwards
                            try { var _ = app.CompactRepair(sourcePath, tempCompact, true); }
                            catch { app.CompactRepair(sourcePath, tempCompact, true); }

                            return File.Exists(tempCompact) ? tempCompact : null;
                        }
                        finally
                        {
                            try { app.Quit(); } catch { }
                        }
                    }
                    catch
                    {
                        return null;
                    }
                });
            }
            catch
            {
                return null;
            }
        }

        /// <summary>
        /// Deletes all synchronized ChangeLog entries from the control/lock database and then attempts a Compact & Repair.
        /// Safe to call multiple times. Should be called while holding the global lock to avoid external access.
        /// IMPORTANT: May fail if other connections (e.g., TodoListSessionTracker heartbeat) are holding the DB open.
        /// </summary>
        public async Task CleanupChangeLogAndCompactAsync(string countryId)
        {
            if (string.IsNullOrWhiteSpace(countryId)) throw new ArgumentException("countryId est requis", nameof(countryId));

            // 1) Delete synchronized rows from ChangeLog (best-effort if table exists)
            try
            {
                var connStr = GetRemoteLockConnectionString(countryId);
                using (var connection = new OleDbConnection(connStr))
                {
                    await connection.OpenAsync();

                    // Verify table exists before deleting
                    var schema = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, new object[] { null, null, null, "TABLE" });
                    bool hasChangeLog = schema != null && schema.Rows.OfType<System.Data.DataRow>()
                        .Any(r => string.Equals(Convert.ToString(r["TABLE_NAME"]), "ChangeLog", StringComparison.OrdinalIgnoreCase));
                    if (hasChangeLog)
                    {
                        using (var cmd = new OleDbCommand("DELETE FROM ChangeLog WHERE Synchronized = TRUE", connection))
                        {
                            try { await cmd.ExecuteNonQueryAsync(); } catch { /* ignore delete errors */ }
                        }
                    }
                }
            }
            catch { /* best-effort cleanup */ }

            // 2) Close all TodoListSessionTracker connections to release locks
            //    (connections will automatically recreate on next heartbeat tick)
            try
            {
                TodoListSessionTracker.CloseAllConnections();
            }
            catch { /* best-effort cleanup */ }
            
            // Wait briefly for connections to fully release
            await Task.Delay(1000).ConfigureAwait(false);

            // 3) Compact & Repair the lock/control database to reclaim space
            try
            {
                var dbPath = GetRemoteLockDbPath(countryId);
                System.Diagnostics.Debug.WriteLine($"[CleanupChangeLogAndCompactAsync] Attempting to compact control DB: {dbPath}");
                var compacted = await TryCompactAccessDatabaseAsync(dbPath);
                
                if (string.IsNullOrWhiteSpace(compacted) || !File.Exists(compacted))
                {
                    System.Diagnostics.Debug.WriteLine("[CleanupChangeLogAndCompactAsync] Compaction failed or was skipped (likely DB is locked by heartbeat or other process)");
                    return; // Exit early if compaction failed
                }
                
                if (!string.IsNullOrWhiteSpace(compacted) && File.Exists(compacted))
                {
                    try
                    {
                        await FileReplaceWithRetriesAsync(compacted, dbPath, dbPath + ".bak", maxAttempts: 6, initialDelayMs: 300);
                    }
                    catch
                    {
                        try { if (File.Exists(dbPath)) File.Delete(dbPath); } catch { }
                        File.Move(compacted, dbPath);
                    }
                    // Cleanup backup if present
                    try { var bak = dbPath + ".bak"; if (File.Exists(bak)) File.Delete(bak); } catch { }
                }
            }
            catch { /* ignore compaction errors */ }
        }
    }
}
