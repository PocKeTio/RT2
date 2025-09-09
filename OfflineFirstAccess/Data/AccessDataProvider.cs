using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.Linq;
using System.Threading.Tasks;
using OfflineFirstAccess.Models;
using System.Collections.Concurrent;
using System.Globalization;
using OfflineFirstAccess.Helpers;

using GenericRecord = System.Collections.Generic.Dictionary<string, object>;

namespace OfflineFirstAccess.Data
{
    public class AccessDataProvider : IDataProvider
    {
        private readonly string _connectionString;
        private readonly SyncConfiguration _config;
        private readonly ConcurrentDictionary<string, Dictionary<string, OleDbType>> _columnTypeCache = new ConcurrentDictionary<string, Dictionary<string, OleDbType>>(StringComparer.OrdinalIgnoreCase);

        private AccessDataProvider(string connectionString, SyncConfiguration config)
        {
            _connectionString = connectionString;
            _config = config;
        }

        // Detect common transient Access/ACE locking errors
        private static bool IsAccessLockException(OleDbException ex)
        {
            try
            {
                // Common ACE/Jet lock/share violations seen in the field
                var codes = new HashSet<int> { 3218, 3260, 3050, 3188, 3197, 3211, 3008 };
                foreach (OleDbError err in ex.Errors)
                {
                    if (codes.Contains(err.NativeError)) return true;
                }
            }
            catch { }
            return false;
        }

        // Minimal mapping to match schemas storing dates as numeric OADate (fallback only)
        private static object MapValueForDb(object value)
        {
            if (value == null || value is DBNull) return DBNull.Value;
            if (value is DateTime dt) return dt.ToOADate();
            return value;
        }

        // Helpers for schema-aware typing and conversion
        private static bool IsDateOleDbType(OleDbType t)
        {
            return t == OleDbType.DBTimeStamp || t == OleDbType.Date || t == OleDbType.DBDate || t == OleDbType.DBTime;
        }

        private static bool IsNumericOleDbType(OleDbType t)
        {
            switch (t)
            {
                case OleDbType.Double:
                case OleDbType.Single:
                case OleDbType.Decimal:
                case OleDbType.Numeric:
                case OleDbType.Currency:
                case OleDbType.Integer:
                case OleDbType.BigInt:
                case OleDbType.SmallInt:
                case OleDbType.TinyInt:
                case OleDbType.UnsignedInt:
                case OleDbType.UnsignedSmallInt:
                case OleDbType.UnsignedTinyInt:
                    return true;
                default:
                    return false;
            }
        }

        // Create parameter with optional expected type and convert DateTime<->OADate if necessary
        private static OleDbParameter CreateParameter(string name, object value, OleDbType? expectedType = null)
        {
            if (!expectedType.HasValue)
            {
                if (value == null || value is DBNull)
                {
                    return new OleDbParameter(name, OleDbType.Variant) { Value = DBNull.Value };
                }

                switch (value)
                {
                    case DateTime dt:
                        return new OleDbParameter(name, OleDbType.DBTimeStamp) { Value = dt };
                    case bool b:
                        return new OleDbParameter(name, OleDbType.Boolean) { Value = b };
                    case string s:
                        return new OleDbParameter(name, OleDbType.VarWChar) { Value = s };
                    case byte[] bytes:
                        return new OleDbParameter(name, OleDbType.Binary) { Value = bytes };
                }

                // Numerics and others: let OleDb infer, but avoid OADate conversions
                return new OleDbParameter(name, value);
            }

            var targetType = expectedType.Value;
            var p = new OleDbParameter(name, targetType);

            if (value == null || value is DBNull)
            {
                p.Value = DBNull.Value;
                return p;
            }

            if (IsDateOleDbType(targetType))
            {
                if (value is DateTime dt)
                {
                    p.Value = dt;
                    return p;
                }
                if (value is double d)
                {
                    p.Value = DateTime.FromOADate(d);
                    return p;
                }
                if (value is float f)
                {
                    p.Value = DateTime.FromOADate(f);
                    return p;
                }
                if (value is decimal dec)
                {
                    p.Value = DateTime.FromOADate((double)dec);
                    return p;
                }
                if (value is string s)
                {
                    if (double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out var sd))
                    {
                        p.Value = DateTime.FromOADate(sd);
                        return p;
                    }
                    if (DateTime.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out var parsedDt))
                    {
                        p.Value = parsedDt;
                        return p;
                    }
                }
                // Fallback
                p.Value = value;
                return p;
            }

            if (IsNumericOleDbType(targetType) && value is DateTime dt2)
            {
                // Store DateTime as OADate number in numeric columns
                p.OleDbType = OleDbType.Double;
                p.Value = dt2.ToOADate();
                return p;
            }

            // Convert common string inputs to the expected target type to avoid type mismatch
            if (value is string sVal)
            {
                try
                {
                    switch (targetType)
                    {
                        case OleDbType.Integer:
                        case OleDbType.UnsignedInt:
                            if (int.TryParse(sVal, NumberStyles.Integer, CultureInfo.InvariantCulture, out var iv)) { p.Value = iv; return p; }
                            break;
                        case OleDbType.SmallInt:
                        case OleDbType.UnsignedSmallInt:
                            if (short.TryParse(sVal, NumberStyles.Integer, CultureInfo.InvariantCulture, out var sv)) { p.Value = sv; return p; }
                            break;
                        case OleDbType.TinyInt:
                        case OleDbType.UnsignedTinyInt:
                            if (byte.TryParse(sVal, NumberStyles.Integer, CultureInfo.InvariantCulture, out var bv)) { p.Value = bv; return p; }
                            break;
                        case OleDbType.BigInt:
                            if (long.TryParse(sVal, NumberStyles.Integer, CultureInfo.InvariantCulture, out var lv)) { p.Value = lv; return p; }
                            break;
                        case OleDbType.Double:
                            if (double.TryParse(sVal, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out var dv)) { p.Value = dv; return p; }
                            break;
                        case OleDbType.Single:
                            if (float.TryParse(sVal, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out var flv)) { p.Value = flv; return p; }
                            break;
                        case OleDbType.Decimal:
                        case OleDbType.Numeric:
                        case OleDbType.Currency:
                            if (decimal.TryParse(sVal, NumberStyles.Number, CultureInfo.InvariantCulture, out var decv)) { p.Value = decv; return p; }
                            break;
                        case OleDbType.Boolean:
                            if (bool.TryParse(sVal, out var bvbool)) { p.Value = bvbool; return p; }
                            if (sVal == "1" || sVal.Equals("yes", StringComparison.OrdinalIgnoreCase) || sVal.Equals("y", StringComparison.OrdinalIgnoreCase)) { p.Value = true; return p; }
                            if (sVal == "0" || sVal.Equals("no", StringComparison.OrdinalIgnoreCase) || sVal.Equals("n", StringComparison.OrdinalIgnoreCase)) { p.Value = false; return p; }
                            break;
                        case OleDbType.Guid:
                            if (Guid.TryParse(sVal, out var g)) { p.Value = g; return p; }
                            break;
                    }
                }
                catch { /* fallthrough to default assignment */ }
            }

            if ((targetType == OleDbType.VarWChar || targetType == OleDbType.WChar || targetType == OleDbType.VarChar || targetType == OleDbType.LongVarWChar || targetType == OleDbType.LongVarChar) && !(value is string))
            {
                p.Value = Convert.ToString(value, CultureInfo.InvariantCulture);
                return p;
            }

            p.Value = value;
            return p;
        }

        private Dictionary<string, OleDbType> GetColumnTypes(OleDbConnection connection, string tableName)
        {
            if (_columnTypeCache.TryGetValue(tableName, out var cached))
                return cached;

            var dict = new Dictionary<string, OleDbType>(StringComparer.OrdinalIgnoreCase);
            try
            {
                var schema = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Columns, new object[] { null, null, tableName, null });
                if (schema != null)
                {
                    foreach (DataRow row in schema.Rows)
                    {
                        var colName = Convert.ToString(row["COLUMN_NAME"]);
                        if (string.IsNullOrEmpty(colName)) continue;
                        var typeCode = Convert.ToInt32(row["DATA_TYPE"]);
                        var t = (OleDbType)typeCode;
                        dict[colName] = t;
                    }
                }
            }
            catch { }
            _columnTypeCache[tableName] = dict;
            return dict;
        }

        private static string BuildParamsDebug(System.Data.OleDb.OleDbParameterCollection parameters)
        {
            try
            {
                var parts = new List<string>();
                foreach (OleDbParameter p in parameters)
                {
                    var val = p.Value;
                    string valType = val == null || val is DBNull ? "(null)" : val.GetType().FullName;
                    string display = val is byte[] ? $"byte[{((byte[])val).Length}]" : (val == null || val is DBNull ? "NULL" : val.ToString());
                    parts.Add($"{p.ParameterName} type={p.OleDbType} runtime={valType} value={display}");
                }
                return string.Join(" | ", parts);
            }
            catch { return "<param-inspect-failed>"; }
        }

        // On crée une "fabrique" publique et asynchrone
        public static async Task<AccessDataProvider> CreateAsync(string connectionString, SyncConfiguration config)
        {
            var provider = new AccessDataProvider(connectionString, config);
            await provider.EnsureConfigTableExistsAsync(); // Appel asynchrone correct
            return provider;
        }

        public async Task<IEnumerable<GenericRecord>> GetChangesAsync(string tableName, DateTime? since)
        {
            var records = new List<GenericRecord>();
            var query = $"SELECT * FROM [{tableName}] WHERE [{_config.LastModifiedColumn}] > @lastSync";

            using (var connection = new OleDbConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var command = new OleDbCommand(query, connection))
                {
                    var lastSyncValue = since.HasValue ? since.Value : DateTime.MinValue;
                    var colTypes = GetColumnTypes(connection, tableName);
                    OleDbType? expected = null;
                    if (colTypes != null && colTypes.TryGetValue(_config.LastModifiedColumn, out var t)) expected = t;
                    command.Parameters.Add(CreateParameter("@lastSync", lastSyncValue, expected));
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            var record = new GenericRecord();
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                record[reader.GetName(i)] = reader.GetValue(i);
                            }
                            records.Add(record);
                        }
                    }
                }
            }
            return records;
        }

        public async Task ApplyChangesAsync(string tableName, IEnumerable<GenericRecord> changesToApply)
        {
            if (changesToApply == null || !changesToApply.Any())
            {
                return;
            }

            using (var connection = new OleDbConnection(_connectionString))
            {
                await connection.OpenAsync();
                OleDbTransaction transaction = null;
                try
                {
                    transaction = connection.BeginTransaction();
                    var colTypes = GetColumnTypes(connection, tableName);
                    // Process records in primary key order to reduce page-lock hotspots
                    var orderedChanges = changesToApply
                        .OrderBy(r => r.ContainsKey(_config.PrimaryKeyColumn) ? Convert.ToString(r[_config.PrimaryKeyColumn], CultureInfo.InvariantCulture) : string.Empty, StringComparer.Ordinal)
                        .ToList();
                    // Detect fresh hydration: if target table is empty, use INSERT-first strategy to avoid UPDATE pass
                    bool insertFirst = false;
                    try
                    {
                        using (var cntCmd = new OleDbCommand($"SELECT COUNT(*) FROM [{tableName}]", connection, transaction))
                        {
                            var obj = await cntCmd.ExecuteScalarAsync();
                            int cnt = (obj != null && obj != DBNull.Value) ? Convert.ToInt32(obj, CultureInfo.InvariantCulture) : 0;
                            insertFirst = (cnt == 0);
                        }
                    }
                    catch { /* if count fails, keep default path */ }
                    int processed = 0;
                    const int chunkSize = 1000;
                    var failedIds = new List<string>();
                    foreach (var record in orderedChanges)
                    {
                        var isDeleted = record.ContainsKey(_config.IsDeletedColumn) && (bool)record[_config.IsDeletedColumn];
                        var idValue = record[_config.PrimaryKeyColumn];

                        if (isDeleted)
                        {
                            var deleteSql = $"DELETE FROM [{tableName}] WHERE [{_config.PrimaryKeyColumn}] = @id";
                            using (var command = new OleDbCommand(deleteSql, connection, transaction))
                            {
                                OleDbType? pkType = null;
                                if (colTypes != null && colTypes.TryGetValue(_config.PrimaryKeyColumn, out var pkt)) pkType = pkt;
                                command.Parameters.Add(CreateParameter("@id", idValue, pkType));
                                try { LogManager.Debug($"APPLY DELETE: Table={tableName} SQL={deleteSql} Params: {BuildParamsDebug(command.Parameters)}"); } catch { }
                                // Retry on transient lock errors
                                {
                                    if (colTypes != null && colTypes.TryGetValue(_config.PrimaryKeyColumn, out var pkt2)) pkType = pkt2;
                                    command.Parameters.Add(CreateParameter("@id", idValue, pkType));
                                    try { LogManager.Debug($"APPLY DELETE: Table={tableName} SQL={deleteSql} Params: {BuildParamsDebug(command.Parameters)}"); } catch { }
                                    // Retry on transient lock errors
                                    {
                                        int attempts = 0;
                                        var rng = new Random();
                                        while (true)
                                        {
                                            try
                                            {
                                                await command.ExecuteNonQueryAsync();
                                                break;
                                            }
                                            catch (OleDbException ex) when (IsAccessLockException(ex) && attempts < 10)
                                            {
                                                attempts++;
                                                int delay = Math.Min(1500, (int)(Math.Pow(2, attempts) * 25));
                                                delay += rng.Next(0, 50);
                                                await Task.Delay(delay);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        else
                        {
                            // Optimized Upsert logic
                            // If fresh hydration (empty target), try INSERT first to avoid UPDATE pass and reduce locks.
                            // Use deterministic ordering to avoid OleDb positional parameter mismatch
                            var hasSchema = colTypes != null && colTypes.Count > 0;
                            var setKeys = record.Keys
                                                .Where(k => !string.Equals(k, _config.PrimaryKeyColumn, StringComparison.OrdinalIgnoreCase)
                                                            && (!hasSchema || colTypes.ContainsKey(k)))
                                                .OrderBy(k => k, StringComparer.OrdinalIgnoreCase)
                                                .ToList();
                            var updateSetClause = string.Join(", ", setKeys.Select(k => $"[{k}] = @{k}"));
                            var updateSql = $"UPDATE [{tableName}] SET {updateSetClause} WHERE [{_config.PrimaryKeyColumn}] = @id";
                            int rowsAffected;

                            if (!insertFirst)
                            {
                                // UPDATE-first path (normal incremental sync)
                                if (setKeys.Count > 0)
                                {
                                    using (var updateCommand = new OleDbCommand(updateSql, connection, transaction))
                                    {
                                        foreach (var key in setKeys)
                                        {
                                            OleDbType? expected = null;
                                            if (colTypes != null && colTypes.TryGetValue(key, out var t)) expected = t;
                                            updateCommand.Parameters.Add(CreateParameter($"@{key}", record[key], expected));
                                        }
                                        OleDbType? pkType2 = null;
                                        if (colTypes != null && colTypes.TryGetValue(_config.PrimaryKeyColumn, out var pkt2)) pkType2 = pkt2;
                                        updateCommand.Parameters.Add(CreateParameter("@id", idValue, pkType2));
                                        try { LogManager.Debug($"APPLY UPDATE: Table={tableName} SQL={updateSql} SetKeys=[{string.Join(",", setKeys)}] Params: {BuildParamsDebug(updateCommand.Parameters)}"); } catch { }
                                        // Retry on transient lock errors
                                        {
                                            int attempts = 0;
                                            var rng = new Random();
                                            while (true)
                                            {
                                                try
                                                {
                                                    rowsAffected = await updateCommand.ExecuteNonQueryAsync();
                                                    try { LogManager.Debug($"APPLY UPDATE result: Table={tableName} ID={idValue} RowsAffected={rowsAffected}"); } catch { }
                                                    break;
                                                }
                                                catch (OleDbException ex) when (IsAccessLockException(ex) && attempts < 10)
                                                {
                                                    attempts++;
                                                    int delay = Math.Min(1500, (int)(Math.Pow(2, attempts) * 25));
                                                    delay += rng.Next(0, 50);
                                                    await Task.Delay(delay);
                                                }
                                                catch (OleDbException ex)
                                                {
                                                    var dbg = BuildParamsDebug(updateCommand.Parameters);
                                                    try { LogManager.Error($"APPLY UPDATE failed (will skip row) Table={tableName} ID={idValue} Error={ex.Message} Params={dbg}", null); } catch { }
                                                    failedIds.Add(Convert.ToString(idValue, CultureInfo.InvariantCulture));
                                                    rowsAffected = -1; // mark as failed so we don't attempt insert here
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                }
                                else
                                {
                                    // No known columns to update; force insert path
                                    rowsAffected = 0;
                                }
                            }
                            else
                            {
                                // INSERT-first path (fresh hydration)
                                rowsAffected = 0; // set to 0 so we still try insert below
                            }

                            if (rowsAffected == 0)
                            {
                                // If no rows were updated, the record doesn't exist. Insert it.
                                var allKeys = record.Keys
                                                    .Where(k => !hasSchema || colTypes.ContainsKey(k))
                                                    .OrderBy(k => k, StringComparer.OrdinalIgnoreCase)
                                                    .ToList();
                                if (!allKeys.Contains(_config.PrimaryKeyColumn))
                                {
                                    allKeys.Insert(0, _config.PrimaryKeyColumn);
                                }
                                var insertColumns = string.Join(", ", allKeys.Select(k => $"[{k}]"));
                                var insertValues = string.Join(", ", allKeys.Select(k => $"@{k}"));
                                var insertSql = $"INSERT INTO [{tableName}] ({insertColumns}) VALUES ({insertValues})";
                                using (var insertCommand = new OleDbCommand(insertSql, connection, transaction))
                                {
                                    foreach (var key in allKeys)
                                    {
                                        OleDbType? expected = null;
                                        if (colTypes != null && colTypes.TryGetValue(key, out var t)) expected = t;
                                        object valueForParam;
                                        if (string.Equals(key, _config.PrimaryKeyColumn, StringComparison.OrdinalIgnoreCase) && !record.ContainsKey(key))
                                        {
                                            valueForParam = idValue;
                                        }
                                        else
                                        {
                                            valueForParam = record[key];
                                        }
                                        insertCommand.Parameters.Add(CreateParameter($"@{key}", valueForParam, expected));
                                    }
                                    try { LogManager.Debug($"APPLY INSERT: Table={tableName} SQL={insertSql} Keys=[{string.Join(",", allKeys)}] Params: {BuildParamsDebug(insertCommand.Parameters)}"); } catch { }
                                    // Retry on transient lock errors; fallback to UPDATE if duplicate-key (3022) when in insert-first mode
                                    {
                                        int attempts = 0;
                                        var rng = new Random();
                                        while (true)
                                        {
                                            try
                                            {
                                                await insertCommand.ExecuteNonQueryAsync();
                                                break;
                                            }
                                            catch (OleDbException ex) when (IsAccessLockException(ex) && attempts < 10)
                                            {
                                                attempts++;
                                                int delay = Math.Min(1500, (int)(Math.Pow(2, attempts) * 25));
                                                delay += rng.Next(0, 50);
                                                await Task.Delay(delay);
                                            }
                                            catch (OleDbException ex)
                                            {
                                                // If fresh hydration and duplicate key, fallback to UPDATE for this row only
                                                bool isDuplicate = false;
                                                try { foreach (OleDbError e in ex.Errors) { if (e.NativeError == 3022) { isDuplicate = true; break; } } } catch { }
                                                if (insertFirst && isDuplicate && setKeys.Count > 0)
                                                {
                                                    var updateSqlLocal = updateSql;
                                                    using (var updateCommand = new OleDbCommand(updateSqlLocal, connection, transaction))
                                                    {
                                                        foreach (var key in setKeys)
                                                        {
                                                            OleDbType? expected2 = null;
                                                            if (colTypes != null && colTypes.TryGetValue(key, out var t2)) expected2 = t2;
                                                            updateCommand.Parameters.Add(CreateParameter($"@{key}", record[key], expected2));
                                                        }
                                                        OleDbType? pkType3 = null;
                                                        if (colTypes != null && colTypes.TryGetValue(_config.PrimaryKeyColumn, out var pkt3)) pkType3 = pkt3;
                                                        updateCommand.Parameters.Add(CreateParameter("@id", idValue, pkType3));
                                                        // Apply the same lock-aware retry
                                                        int attempts2 = 0;
                                                        var rng2 = new Random();
                                                        while (true)
                                                        {
                                                            try { await updateCommand.ExecuteNonQueryAsync(); break; }
                                                            catch (OleDbException ex2) when (IsAccessLockException(ex2) && attempts2 < 10)
                                                            {
                                                                attempts2++;
                                                                int delay2 = Math.Min(1500, (int)(Math.Pow(2, attempts2) * 25));
                                                                delay2 += rng2.Next(0, 50);
                                                                await Task.Delay(delay2);
                                                            }
                                                        }
                                                    }
                                                    break; // handled via update
                                                }
                                                else
                                                {
                                                    var dbg = BuildParamsDebug(insertCommand.Parameters);
                                                    try { LogManager.Error($"APPLY INSERT failed (will skip row) Table={tableName} ID={idValue} Error={ex.Message} Params={dbg}", null); } catch { }
                                                    failedIds.Add(Convert.ToString(idValue, CultureInfo.InvariantCulture));
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        // Chunked commit to release locks periodically
                        processed++;
                        if (processed % chunkSize == 0)
                        {
                            try { transaction?.Commit(); } catch { }
                            transaction?.Dispose();
                            transaction = connection.BeginTransaction();
                        }
                    }
                    try { transaction?.Commit(); } catch { }
                    // Log failures summary (do not throw, by design to avoid full rollback)
                    if (failedIds.Count > 0)
                    {
                        try { LogManager.Warn($"APPLY CHANGES completed with failures: {failedIds.Count} row(s) skipped. Example IDs: {string.Join(", ", failedIds.Take(5))}..."); } catch { }
                    }
                }
                catch
                {
                    try { transaction?.Rollback(); } catch { }
                    throw;
                }
            }
        }

        public async Task<IEnumerable<GenericRecord>> GetRecordsByIds(string tableName, IEnumerable<string> ids)
        {
            var records = new List<GenericRecord>();
            if (ids == null)
                return records;

            // Sanitize ID list: remove null/empty and duplicates
            var idList = ids.Where(g => !string.IsNullOrWhiteSpace(g)).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
            if (idList.Count == 0)
                return records;

            using (var connection = new OleDbConnection(_connectionString))
            {
                await connection.OpenAsync();

                // Validate table exists
                var tableSchema = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, new object[] { null, null, null, "TABLE" });
                bool tableExists = tableSchema != null && tableSchema.Rows.OfType<DataRow>()
                    .Any(r => string.Equals(r["TABLE_NAME"].ToString(), tableName, StringComparison.OrdinalIgnoreCase));
                if (!tableExists)
                {
                    throw new InvalidOperationException($"Table '{tableName}' introuvable dans la base. Disponible(s): " +
                        string.Join(", ", tableSchema?.Rows.OfType<DataRow>().Select(r => r["TABLE_NAME"].ToString()) ?? Array.Empty<string>()));
                }

                // Validate column exists
                var colSchema = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Columns, new object[] { null, null, tableName, null });
                bool columnExists = colSchema != null && colSchema.Rows.OfType<DataRow>()
                    .Any(r => string.Equals(r["COLUMN_NAME"].ToString(), _config.PrimaryKeyColumn, StringComparison.OrdinalIgnoreCase));
                if (!columnExists)
                {
                    throw new InvalidOperationException($"Colonne '{_config.PrimaryKeyColumn}' introuvable dans la table '{tableName}'. Colonnes: " +
                        string.Join(", ", colSchema?.Rows.OfType<DataRow>().Select(r => r["COLUMN_NAME"].ToString()) ?? Array.Empty<string>()));
                }

                // Build query and parameters
                var parameterPlaceholders = string.Join(",", idList.Select((_, i) => $"@p{i}"));
                var query = $"SELECT * FROM [{tableName}] WHERE [{_config.PrimaryKeyColumn}] IN ({parameterPlaceholders})";

                using (var command = new OleDbCommand(query, connection))
                {
                    // Use schema-aware typing for PK to avoid data type mismatches
                    var colTypes = GetColumnTypes(connection, tableName);
                    OleDbType? pkType = null;
                    if (colTypes != null && colTypes.TryGetValue(_config.PrimaryKeyColumn, out var t)) pkType = t;
                    for (int i = 0; i < idList.Count; i++)
                    {
                        command.Parameters.Add(CreateParameter($"@p{i}", idList[i], pkType));
                    }
                    try
                    {
                        using (var reader = await command.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                var record = new GenericRecord();
                                for (int j = 0; j < reader.FieldCount; j++)
                                {
                                    record[reader.GetName(j)] = reader.GetValue(j);
                                }
                                records.Add(record);
                            }
                        }
                    }
                    catch (OleDbException ex)
                    {
                        var dbg = BuildParamsDebug(command.Parameters);
                        throw new InvalidOperationException($"SELECT by IDs failed on table '{tableName}'. SQL: {query}. Params: {dbg}. Error: {ex.Message}", ex);
                    }
                }
            }
            return records;
        }
        public async Task<string> GetParameterAsync(string key)
        {
            using (var connection = new OleDbConnection(_connectionString))
            {
                await connection.OpenAsync();
                var command = new OleDbCommand("SELECT ConfigValue FROM _SyncConfig WHERE ConfigKey = @Key", connection);
                // _SyncConfig.ConfigKey is TEXT(255)
                command.Parameters.Add(CreateParameter("@Key", key, OleDbType.VarWChar));
                var result = await command.ExecuteScalarAsync();
                return result?.ToString();
            }
        }

        public async Task SetParameterAsync(string key, string value)
        {
            using (var connection = new OleDbConnection(_connectionString))
            {
                await connection.OpenAsync();
                
                var updateCommand = new OleDbCommand("UPDATE _SyncConfig SET ConfigValue = @Value WHERE ConfigKey = @Key", connection);
                // ConfigValue is MEMO (LongVarWChar), ConfigKey is TEXT(255)
                updateCommand.Parameters.Add(CreateParameter("@Value", value, OleDbType.LongVarWChar));
                updateCommand.Parameters.Add(CreateParameter("@Key", key, OleDbType.VarWChar));
                int rowsAffected = await updateCommand.ExecuteNonQueryAsync();

                if (rowsAffected == 0)
                {
                    var insertCommand = new OleDbCommand("INSERT INTO _SyncConfig (ConfigKey, ConfigValue) VALUES (@Key, @Value)", connection);
                    insertCommand.Parameters.Add(CreateParameter("@Key", key, OleDbType.VarWChar));
                    insertCommand.Parameters.Add(CreateParameter("@Value", value, OleDbType.LongVarWChar));
                    await insertCommand.ExecuteNonQueryAsync();
                }
            }
        }

        private async Task EnsureConfigTableExistsAsync()
        {
            try
            {
                using (var connection = new OleDbConnection(_connectionString))
                {
                    await connection.OpenAsync();
                    var schema = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, new object[] { null, null, null, "TABLE" });
                    bool tableExists = schema.Rows.OfType<DataRow>().Any(r => r["TABLE_NAME"].ToString().Equals("_SyncConfig", StringComparison.OrdinalIgnoreCase));

                    if (!tableExists)
                    {
                        var createCommand = new OleDbCommand("CREATE TABLE _SyncConfig (ConfigKey TEXT(255) PRIMARY KEY, ConfigValue MEMO)", connection);
                        await createCommand.ExecuteNonQueryAsync();
                    }
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Error ensuring _SyncConfig table exists: {ex.Message}");
                // In a real app, this should be logged to a proper logging framework.
            }
        }
    }
}