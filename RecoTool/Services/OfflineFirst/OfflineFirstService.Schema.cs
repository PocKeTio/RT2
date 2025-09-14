using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using OfflineFirstAccess.Helpers;

namespace RecoTool.Services
{
    // Partial: schema creation/checks for control DB and verification of local DB schemas
    public partial class OfflineFirstService
    {
        /// <summary>
        /// Ensure core Control DB schema exists: _SyncConfig and T_ConfigParameters.
        /// Safe to call multiple times.
        /// </summary>
        public async Task EnsureControlSchemaAsync()
        {
            if (CurrentCountry?.CNT_Id == null)
                return;

            var connStr = GetControlConnectionString(CurrentCountry?.CNT_Id);
            using (var connection = new OleDbConnection(connStr))
            {
                await connection.OpenAsync();

                // Existing tables snapshot
                var tables = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, new object[] { null, null, null, "TABLE" });
                bool Has(string name) => tables != null && tables.Rows.OfType<System.Data.DataRow>()
                    .Any(r => string.Equals(Convert.ToString(r["TABLE_NAME"]), name, StringComparison.OrdinalIgnoreCase));

                // _SyncConfig
                if (!Has("_SyncConfig"))
                {
                    using (var cmd = new OleDbCommand("CREATE TABLE _SyncConfig (ConfigKey TEXT(255) PRIMARY KEY, ConfigValue MEMO)", connection))
                        await cmd.ExecuteNonQueryAsync();
                }

                // T_ConfigParameters removed: configuration is read from referential T_Param only

                // SyncLocks (global locking metadata)
                if (!Has("SyncLocks"))
                {
                    var sql = @"CREATE TABLE SyncLocks (
                        LockID TEXT(255) PRIMARY KEY,
                        Reason MEMO,
                        CreatedAt DATETIME,
                        ExpiresAt DATETIME,
                        MachineName TEXT(100),
                        ProcessId LONG,
                        SyncStatus TEXT(50)
                    )";
                    using (var cmd = new OleDbCommand(sql, connection))
                        await cmd.ExecuteNonQueryAsync();
                }

                // ImportRuns (import auditing)
                if (!Has("ImportRuns"))
                {
                    var sql = @"CREATE TABLE ImportRuns (
                        Id COUNTER PRIMARY KEY,
                        CountryId TEXT(50),
                        Source TEXT(255),
                        StartedAtUtc DATETIME,
                        CompletedAtUtc DATETIME,
                        Status TEXT(50),
                        Message MEMO,
                        Version TEXT(255)
                    )";
                    using (var cmd = new OleDbCommand(sql, connection))
                        await cmd.ExecuteNonQueryAsync();
                }

                // SystemVersion (app/control DB versioning)
                if (!Has("SystemVersion"))
                {
                    var sql = @"CREATE TABLE SystemVersion (
                        Id COUNTER PRIMARY KEY,
                        Component TEXT(100),
                        Version TEXT(50),
                        AppliedAtUtc DATETIME
                    )";
                    using (var cmd = new OleDbCommand(sql, connection))
                        await cmd.ExecuteNonQueryAsync();
                }
            }
        }

        private async Task EnsureSyncLocksTableExistsAsync(OleDbConnection connection)
        {
            var restrictions = new string[4];
            restrictions[2] = "SyncLocks";
            DataTable table = connection.GetSchema("Tables", restrictions);
            if (table.Rows.Count == 0)
            {
                using (var cmd = new OleDbCommand(
                    "CREATE TABLE SyncLocks (" +
                    "LockID TEXT(36) PRIMARY KEY, " +
                    "Reason TEXT, " +
                    "CreatedAt DATETIME, " +
                    "ExpiresAt DATETIME, " +
                    "MachineName TEXT, " +
                    "ProcessId INTEGER, " +
                    "SyncStatus TEXT(50))", connection))
                {
                    await cmd.ExecuteNonQueryAsync();
                }
            }

            // Ensure SyncStatus column exists in any case (upgrade path)
            var colRestrictions = new string[4];
            colRestrictions[2] = "SyncLocks";
            DataTable columns = connection.GetSchema("Columns", colRestrictions);
            bool hasSyncStatus = false;
            foreach (DataRow r in columns.Rows)
            {
                var colName = r["COLUMN_NAME"]?.ToString();
                if (string.Equals(colName, "SyncStatus", StringComparison.OrdinalIgnoreCase))
                {
                    hasSyncStatus = true;
                    break;
                }
            }
            if (!hasSyncStatus)
            {
                try
                {
                    using (var alter = new OleDbCommand("ALTER TABLE SyncLocks ADD COLUMN SyncStatus TEXT(50)", connection))
                    {
                        await alter.ExecuteNonQueryAsync();
                    }
                }
                catch { /* best-effort upgrade */ }
            }
        }

        /// <summary>
        /// Vérifie les schémas des bases locales (DWINGS, AMBRE, RECONCILIATION) pour un pays donné.
        /// Compare uniquement la présence des colonnes attendues et loggue les manquants dans la fenêtre Immediate.
        /// Aucune interaction UI; non bloquant autant que possible.
        /// </summary>
        /// <param name="countryId">Code pays (ex: "ES")</param>
        private async Task VerifyDatabaseSchemaAsync(string countryId)
        {
            if (string.IsNullOrWhiteSpace(countryId)) return;

            System.Diagnostics.Debug.WriteLine($"[SchemaVerification] Start for {countryId}");

            // Helpers locaux
            bool TableExists(OleDbConnection conn, string tableName)
            {
                try
                {
                    var schema = conn.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, new object[] { null, null, null, "TABLE" });
                    return schema != null && schema.Rows.OfType<System.Data.DataRow>()
                        .Any(r => string.Equals(Convert.ToString(r["TABLE_NAME"]), tableName, StringComparison.OrdinalIgnoreCase));
                }
                catch { return false; }
            }

            HashSet<string> GetActualColumns(OleDbConnection conn, string tableName)
            {
                var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                try
                {
                    var schema = conn.GetOleDbSchemaTable(OleDbSchemaGuid.Columns, new object[] { null, null, tableName, null });
                    if (schema != null)
                    {
                        foreach (System.Data.DataRow row in schema.Rows)
                        {
                            var colName = Convert.ToString(row["COLUMN_NAME"]);
                            if (!string.IsNullOrEmpty(colName)) set.Add(colName);
                        }
                    }
                }
                catch { }
                return set;
            }

            // Construit dynamiquement les schémas attendus via les configureActions existantes
            Dictionary<string, HashSet<string>> BuildExpected(DatabaseTemplateBuilder builder, params string[] onlyTheseTables)
            {
                var dict = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
                var cfg = builder.GetConfiguration();
                var filter = (onlyTheseTables != null && onlyTheseTables.Length > 0);
                foreach (var table in cfg.Tables)
                {
                    if (filter && !onlyTheseTables.Contains(table.Name, StringComparer.OrdinalIgnoreCase))
                        continue;
                    var cols = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    if (table.Columns != null)
                    {
                        foreach (var c in table.Columns)
                            if (!string.IsNullOrEmpty(c.Name)) cols.Add(c.Name);
                    }
                    dict[table.Name] = cols;
                }
                return dict;
            }

            Func<string> newTmp = () => Path.Combine(Path.GetTempPath(), $"schema_{Guid.NewGuid():N}.accdb");

            // DWINGS: la configuration retire certaines tables système; ne vérifier que les tables métier
            var dwBuilder = new DatabaseTemplateBuilder(newTmp());
            DatabaseRecreationService.ConfigureDwings(dwBuilder);
            var expectedDw = BuildExpected(dwBuilder, "T_DW_Guarantee", "T_DW_Data");

            // AMBRE: filtrer uniquement la table métier
            var ambreBuilder = new DatabaseTemplateBuilder(newTmp());
            DatabaseRecreationService.ConfigureAmbre(ambreBuilder);
            var expectedAmbre = BuildExpected(ambreBuilder, "T_Data_Ambre");

            // RECONCILIATION: filtrer uniquement la table métier
            var reconBuilder = new DatabaseTemplateBuilder(newTmp());
            DatabaseRecreationService.ConfigureReconciliation(reconBuilder);
            var expectedRecon = BuildExpected(reconBuilder, "T_Reconciliation");

            async Task VerifyOneAsync(string dbLabel, string dbPath, Dictionary<string, HashSet<string>> expected)
            {
                try
                {
                    if (string.IsNullOrWhiteSpace(dbPath) || !File.Exists(dbPath))
                    {
                        System.Diagnostics.Debug.WriteLine($"[SchemaVerification] {dbLabel} for {countryId}: local DB not found -> {dbPath}");
                        return;
                    }

                    using (var conn = new OleDbConnection(AceConn(dbPath)))
                    {
                        await conn.OpenAsync();

                        foreach (var kvp in expected)
                        {
                            var table = kvp.Key;
                            var expectedCols = kvp.Value;

                            if (!TableExists(conn, table))
                            {
                                System.Diagnostics.Debug.WriteLine($"[SchemaVerification] {dbLabel} for {countryId}: missing table '{table}'");
                                continue;
                            }

                            var actualCols = GetActualColumns(conn, table);
                            var missing = expectedCols.Except(actualCols, StringComparer.OrdinalIgnoreCase).ToList();
                            if (missing.Count > 0)
                            {
                                System.Diagnostics.Debug.WriteLine($"[SchemaVerification] {dbLabel} for {countryId}: table '{table}' missing {missing.Count} column(s): {string.Join(", ", missing)}");
                            }
                            else
                            {
                                System.Diagnostics.Debug.WriteLine($"[SchemaVerification] {dbLabel} for {countryId}: table '{table}' OK");
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine($"[SchemaVerification] {dbLabel} for {countryId}: error -> {ex.Message}");
                }
            }

            // DWINGS
            await VerifyOneAsync("DWINGS", GetLocalDwDbPath(countryId), expectedDw);
            // AMBRE
            await VerifyOneAsync("AMBRE", GetLocalAmbreDbPath(countryId), expectedAmbre);
            // RECONCILIATION
            await VerifyOneAsync("RECONCILIATION", GetLocalReconciliationDbPath(countryId), expectedRecon);

            System.Diagnostics.Debug.WriteLine($"[SchemaVerification] End for {countryId}");
        }
    }
}
