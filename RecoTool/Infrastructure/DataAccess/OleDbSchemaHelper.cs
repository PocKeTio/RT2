using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.Globalization;
using System.Threading.Tasks;

namespace RecoTool.Services.Helpers
{
    public static class OleDbSchemaHelper
    {
        /// <summary>
        /// Returns a map of column name -> OleDbType for the given table by reading the schema.
        /// Opens the connection if needed and closes it if it was opened here.
        /// </summary>
        public static async Task<Dictionary<string, OleDbType>> GetColumnTypesAsync(OleDbConnection connection, string tableName)
        {
            if (connection == null) throw new ArgumentNullException(nameof(connection));
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentException("tableName is required", nameof(tableName));

            bool openedHere = false;
            if (connection.State != ConnectionState.Open)
            {
                await connection.OpenAsync();
                openedHere = true;
            }

            try
            {
                var map = new Dictionary<string, OleDbType>(StringComparer.OrdinalIgnoreCase);
                using (var schema = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Columns, new object[] { null, null, tableName, null }))
                {
                    if (schema != null)
                    {
                        foreach (System.Data.DataRow row in schema.Rows)
                        {
                            var colName = Convert.ToString(row["COLUMN_NAME"]);
                            if (string.IsNullOrWhiteSpace(colName)) continue;
                            try
                            {
                                var typeCode = Convert.ToInt32(row["DATA_TYPE"]);
                                map[colName] = (OleDbType)typeCode;
                            }
                            catch
                            {
                                map[colName] = OleDbType.Variant;
                            }
                        }
                    }
                }
                return await Task.FromResult(map);
            }
            finally
            {
                if (openedHere)
                {
                    try { connection.Close(); } catch { }
                }
            }
        }

        /// <summary>
        /// Best-effort inference of OleDbType from a CLR value when schema metadata is unavailable.
        /// </summary>
        public static OleDbType InferOleDbTypeFromValue(object value)
        {
            if (value == null || value == DBNull.Value) return OleDbType.Variant;

            var t = value.GetType();
            if (t == typeof(Guid)) return OleDbType.Guid;
            if (t == typeof(byte[])) return OleDbType.Binary;
            if (t == typeof(TimeSpan)) return OleDbType.Double; // store duration as Double (e.g., seconds)

            switch (Type.GetTypeCode(t))
            {
                case TypeCode.Boolean: return OleDbType.Boolean;
                case TypeCode.Byte: return OleDbType.UnsignedTinyInt;
                case TypeCode.SByte: return OleDbType.TinyInt;
                case TypeCode.Int16: return OleDbType.SmallInt;
                case TypeCode.UInt16: return OleDbType.Integer; // closest available (Access has no unsigned 16)
                case TypeCode.Int32: return OleDbType.Integer;
                case TypeCode.UInt32: return OleDbType.BigInt;
                case TypeCode.Int64: return OleDbType.BigInt;
                case TypeCode.UInt64: return OleDbType.Decimal;
                case TypeCode.Single: return OleDbType.Single;
                case TypeCode.Double: return OleDbType.Double;
                case TypeCode.Decimal: return OleDbType.Decimal;
                case TypeCode.DateTime: return OleDbType.Date;
                case TypeCode.Char: return OleDbType.WChar;
                case TypeCode.String: return OleDbType.VarWChar;
                default: return OleDbType.Variant;
            }
        }

        /// <summary>
        /// Coerces application values into OleDb-compatible values based on the target OleDbType.
        /// Returns DBNull.Value for null/empty where appropriate.
        /// </summary>
        public static object CoerceValueForOleDb(object value, OleDbType targetType)
        {
            if (value == null || value == DBNull.Value) return DBNull.Value;

            try
            {
                switch (targetType)
                {
                    case OleDbType.Boolean:
                        if (value is bool b) return b;
                        if (value is string bs)
                        {
                            if (bool.TryParse(bs, out var bb)) return bb;
                            if (int.TryParse(bs, NumberStyles.Integer, CultureInfo.InvariantCulture, out var bi)) return bi != 0;
                            return DBNull.Value;
                        }
                        return Convert.ToBoolean(value, CultureInfo.InvariantCulture);

                    case OleDbType.TinyInt:
                        if (value is sbyte sb) return sb;
                        return Convert.ToSByte(value, CultureInfo.InvariantCulture);

                    case OleDbType.UnsignedTinyInt:
                        if (value is byte by) return by;
                        return Convert.ToByte(value, CultureInfo.InvariantCulture);

                    case OleDbType.SmallInt:
                        if (value is short s) return s;
                        return Convert.ToInt16(value, CultureInfo.InvariantCulture);

                    case OleDbType.Integer:
                        if (value is int i32) return i32;
                        return Convert.ToInt32(value, CultureInfo.InvariantCulture);

                    case OleDbType.BigInt:
                        if (value is long i64) return i64;
                        return Convert.ToInt64(value, CultureInfo.InvariantCulture);

                    case OleDbType.Single:
                        if (value is float f) return f;
                        return Convert.ToSingle(value, CultureInfo.InvariantCulture);

                    case OleDbType.Double:
                        if (value is double d) return d;
                        return Convert.ToDouble(value, CultureInfo.InvariantCulture);

                    case OleDbType.Decimal:
                    case OleDbType.Numeric:
                    case OleDbType.VarNumeric:
                    case OleDbType.Currency:
                        if (value is decimal dec) return dec;
                        if (value is string ds)
                        {
                            if (decimal.TryParse(ds, NumberStyles.Any, CultureInfo.InvariantCulture, out var dd)) return dd;
                            return DBNull.Value;
                        }
                        return Convert.ToDecimal(value, CultureInfo.InvariantCulture);

                    case OleDbType.Date:
                        if (value is DateTime dt) return dt;
                        if (value is DateTimeOffset dto) return dto.UtcDateTime;
                        if (value is string dts)
                        {
                            if (DateTime.TryParse(dts, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out var parsed)) return parsed;
                            return DBNull.Value;
                        }
                        return Convert.ToDateTime(value, CultureInfo.InvariantCulture);

                    case OleDbType.Guid:
                        if (value is Guid g) return g;
                        if (value is string gs) { return Guid.TryParse(gs, out var gg) ? gg : (object)DBNull.Value; }
                        return DBNull.Value;

                    case OleDbType.Binary:
                    case OleDbType.VarBinary:
                    case OleDbType.LongVarBinary:
                        if (value is byte[] bytes) return bytes;
                        return DBNull.Value;

                    case OleDbType.Char:
                    case OleDbType.VarChar:
                    case OleDbType.LongVarChar:
                    case OleDbType.WChar:
                    case OleDbType.VarWChar:
                    case OleDbType.LongVarWChar:
                        var sVal = value.ToString();
                        return sVal;

                    default:
                        return value;
                }
            }
            catch
            {
                return DBNull.Value;
            }
        }
    }
}
