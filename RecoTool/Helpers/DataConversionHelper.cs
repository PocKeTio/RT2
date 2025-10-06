using System;
using System.Data;
using System.Globalization;

namespace RecoTool.Helpers
{
    /// <summary>
    /// Centralized utilities for safe data conversion from DB objects
    /// Eliminates duplication across HomePage, ReconciliationView, etc.
    /// </summary>
    public static class DataConversionHelper
    {
        #region Safe Integer Conversion
        
        /// <summary>
        /// Safely converts an object or DataRow column to int, returning 0 if conversion fails
        /// </summary>
        public static int SafeGetInt(object value)
        {
            if (value == null || value == DBNull.Value)
                return 0;

            if (value is int intValue)
                return intValue;

            if (int.TryParse(value.ToString(), out var result))
                return result;

            if (double.TryParse(value.ToString(), out var doubleValue))
                return (int)Math.Round(doubleValue);

            return 0;
        }

        /// <summary>
        /// Safely gets an int value from a DataRow column
        /// </summary>
        public static int SafeGetInt(DataRow row, string columnName)
        {
            if (row == null || !row.Table.Columns.Contains(columnName))
                return 0;

            return SafeGetInt(row[columnName]);
        }

        #endregion

        #region Safe Nullable Integer Conversion

        /// <summary>
        /// Safely converts an object to nullable int, returning null if conversion fails
        /// </summary>
        public static int? SafeGetNullableInt(object value)
        {
            if (value == null || value == DBNull.Value)
                return null;

            if (value is int intValue)
                return intValue;

            if (int.TryParse(value.ToString(), out var result))
                return result;

            return null;
        }

        /// <summary>
        /// Safely gets a nullable int value from a DataRow column
        /// </summary>
        public static int? SafeGetNullableInt(DataRow row, string columnName)
        {
            if (row == null || !row.Table.Columns.Contains(columnName))
                return null;

            return SafeGetNullableInt(row[columnName]);
        }

        #endregion

        #region Safe Decimal Conversion

        /// <summary>
        /// Safely converts an object to decimal, returning 0m if conversion fails
        /// </summary>
        public static decimal SafeGetDecimal(object value)
        {
            if (value == null || value == DBNull.Value)
                return 0m;

            if (value is decimal decimalValue)
                return decimalValue;

            if (decimal.TryParse(value.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var result))
                return result;

            if (double.TryParse(value.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var doubleValue))
                return (decimal)doubleValue;

            return 0m;
        }

        /// <summary>
        /// Safely gets a decimal value from a DataRow column
        /// </summary>
        public static decimal SafeGetDecimal(DataRow row, string columnName)
        {
            if (row == null || !row.Table.Columns.Contains(columnName))
                return 0m;

            return SafeGetDecimal(row[columnName]);
        }

        #endregion

        #region Safe Boolean Conversion

        /// <summary>
        /// Safely converts an object to bool, treating null/DBNull as false
        /// Supports: true/false strings, 1/0 integers, actual booleans
        /// </summary>
        public static bool SafeGetBool(object value)
        {
            if (value == null || value == DBNull.Value)
                return false;

            if (value is bool boolValue)
                return boolValue;

            var strValue = value.ToString().ToLowerInvariant();
            if (bool.TryParse(strValue, out var result))
                return result;

            // Handle integer representations: 1 = true, 0 = false
            if (int.TryParse(strValue, out var intValue))
                return intValue != 0;

            return strValue == "1" || strValue == "yes" || strValue == "y";
        }

        /// <summary>
        /// Safely gets a bool value from a DataRow column
        /// </summary>
        public static bool SafeGetBool(DataRow row, string columnName)
        {
            if (row == null || !row.Table.Columns.Contains(columnName))
                return false;

            return SafeGetBool(row[columnName]);
        }

        #endregion

        #region Safe Nullable Boolean Conversion

        /// <summary>
        /// Safely converts an object to nullable bool, returning null if value is null/DBNull
        /// </summary>
        public static bool? SafeGetNullableBool(object value)
        {
            if (value == null || value == DBNull.Value)
                return null;

            if (value is bool boolValue)
                return boolValue;

            var strValue = value.ToString();
            if (bool.TryParse(strValue, out var result))
                return result;

            if (int.TryParse(strValue, out var intValue))
                return intValue != 0;

            return null;
        }

        /// <summary>
        /// Safely gets a nullable bool value from a DataRow column
        /// </summary>
        public static bool? SafeGetNullableBool(DataRow row, string columnName)
        {
            if (row == null || !row.Table.Columns.Contains(columnName))
                return null;

            return SafeGetNullableBool(row[columnName]);
        }

        #endregion

        #region Safe DateTime Conversion

        /// <summary>
        /// Safely converts an object to nullable DateTime
        /// </summary>
        public static DateTime? SafeGetDateTime(object value)
        {
            if (value == null || value == DBNull.Value)
                return null;

            if (value is DateTime dateValue)
                return dateValue;

            if (DateTime.TryParse(value.ToString(), out var result))
                return result;

            return null;
        }

        /// <summary>
        /// Safely gets a nullable DateTime value from a DataRow column
        /// </summary>
        public static DateTime? SafeGetDateTime(DataRow row, string columnName)
        {
            if (row == null || !row.Table.Columns.Contains(columnName))
                return null;

            return SafeGetDateTime(row[columnName]);
        }

        #endregion

        #region Safe String Conversion

        /// <summary>
        /// Safely converts an object to string, returning null for null/DBNull
        /// </summary>
        public static string SafeGetString(object value)
        {
            if (value == null || value == DBNull.Value)
                return null;

            return value.ToString();
        }

        /// <summary>
        /// Safely gets a string value from a DataRow column
        /// </summary>
        public static string SafeGetString(DataRow row, string columnName)
        {
            if (row == null || !row.Table.Columns.Contains(columnName))
                return null;

            return SafeGetString(row[columnName]);
        }

        #endregion

        #region Safe Double Conversion

        /// <summary>
        /// Safely converts an object to double, returning 0.0 if conversion fails
        /// </summary>
        public static double SafeGetDouble(object value)
        {
            if (value == null || value == DBNull.Value)
                return 0.0;

            if (value is double doubleValue)
                return doubleValue;

            if (double.TryParse(value.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var result))
                return result;

            return 0.0;
        }

        /// <summary>
        /// Safely gets a double value from a DataRow column
        /// </summary>
        public static double SafeGetDouble(DataRow row, string columnName)
        {
            if (row == null || !row.Table.Columns.Contains(columnName))
                return 0.0;

            return SafeGetDouble(row[columnName]);
        }

        #endregion
    }
}
