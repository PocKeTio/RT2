using System;
using System.Globalization;
using System.Linq;
using RecoTool.Services.Helpers;

namespace RecoTool.Helpers
{
    /// <summary>
    /// Centralized helper for culture and number format handling.
    /// Ensures consistent decimal/thousand separator handling across Excel, Access, and UI.
    /// Works in conjunction with ValidationHelper for robust parsing.
    /// </summary>
    public static class CultureHelper
    {
        /// <summary>
        /// Gets the current user's culture settings from Windows
        /// </summary>
        public static CultureInfo GetUserCulture()
        {
            try
            {
                return CultureInfo.CurrentCulture;
            }
            catch
            {
                return CultureInfo.InvariantCulture;
            }
        }

        /// <summary>
        /// Gets culture info for the user with diagnostic information
        /// </summary>
        public static string GetCultureDiagnostics()
        {
            try
            {
                var culture = CultureInfo.CurrentCulture;
                var nfi = culture.NumberFormat;
                
                return $"Culture: {culture.Name}\n" +
                       $"Decimal Separator: '{nfi.NumberDecimalSeparator}'\n" +
                       $"Thousand Separator: '{nfi.NumberGroupSeparator}'\n" +
                       $"Currency Symbol: '{nfi.CurrencySymbol}'\n" +
                       $"Date Format: {culture.DateTimeFormat.ShortDatePattern}";
            }
            catch
            {
                return "Culture information unavailable";
            }
        }

        /// <summary>
        /// Formats a decimal for Access database SQL queries (always uses period as decimal separator)
        /// </summary>
        public static string FormatDecimalForAccessSQL(decimal value)
        {
            // Access ALWAYS expects period (.) as decimal separator in SQL queries
            return value.ToString("0.################", CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Formats a decimal for display to user (uses their culture)
        /// </summary>
        public static string FormatDecimalForDisplay(decimal value, int decimals = 2)
        {
            return value.ToString($"N{decimals}", CultureInfo.CurrentCulture);
        }

        /// <summary>
        /// Parses a decimal from user input (tries current culture first, then invariant)
        /// </summary>
        public static decimal? ParseDecimalFromUserInput(string input)
        {
            if (string.IsNullOrWhiteSpace(input))
                return null;

            // Try current culture first (user's format)
            if (decimal.TryParse(input, NumberStyles.Any, CultureInfo.CurrentCulture, out var result))
                return result;

            // Fallback to invariant culture
            if (decimal.TryParse(input, NumberStyles.Any, CultureInfo.InvariantCulture, out result))
                return result;

            return null;
        }

        /// <summary>
        /// Parses a decimal from Excel/file data (uses specified culture or auto-detect)
        /// Leverages ValidationHelper.SafeParseDecimal for robust parsing
        /// </summary>
        public static decimal? ParseDecimalFromFile(string input, CultureInfo culture = null)
        {
            if (string.IsNullOrWhiteSpace(input))
                return null;

            // Use the robust ValidationHelper which handles all edge cases
            var result = ValidationHelper.SafeParseDecimal(input, culture ?? CultureInfo.CurrentCulture);
            
            // Return null if result is 0 and input wasn't actually "0"
            if (result == 0 && !input.Trim().Equals("0") && !input.Trim().StartsWith("0.") && !input.Trim().StartsWith("0,"))
            {
                // Could be a parsing failure, but ValidationHelper returns 0 as fallback
                // Check if input looks like it should be a number
                if (input.Any(c => char.IsDigit(c)))
                    return result; // It has digits, so 0 is probably correct
                return null; // No digits, probably invalid
            }
            
            return result;
        }

        /// <summary>
        /// Parses a decimal from Access database (always uses invariant culture)
        /// </summary>
        public static decimal? ParseDecimalFromAccess(object value)
        {
            if (value == null || value == DBNull.Value)
                return null;

            if (value is decimal dec)
                return dec;

            if (value is double dbl)
                return (decimal)dbl;

            if (value is float flt)
                return (decimal)flt;

            if (value is int i)
                return i;

            if (value is long l)
                return l;

            // Try parsing string with invariant culture
            var str = value.ToString();
            if (decimal.TryParse(str, NumberStyles.Any, CultureInfo.InvariantCulture, out var result))
                return result;

            return null;
        }

        /// <summary>
        /// Validates if a string can be parsed as a decimal
        /// </summary>
        public static bool IsValidDecimal(string input)
        {
            return ParseDecimalFromUserInput(input).HasValue;
        }

        /// <summary>
        /// Gets a safe culture for Excel operations based on country settings
        /// </summary>
        public static CultureInfo GetExcelCulture(Country country)
        {
            if (country == null)
                return CultureInfo.CurrentCulture;

            try
            {
                // Try to get culture from country code
                if (!string.IsNullOrWhiteSpace(country.CNT_Code))
                {
                    // Map country codes to culture names
                    var cultureMap = new System.Collections.Generic.Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                    {
                        { "FR", "fr-FR" },
                        { "BE", "fr-BE" },
                        { "LU", "fr-LU" },
                        { "CH", "fr-CH" },
                        { "GB", "en-GB" },
                        { "US", "en-US" },
                        { "DE", "de-DE" },
                        { "IT", "it-IT" },
                        { "ES", "es-ES" },
                        { "NL", "nl-NL" }
                    };

                    if (cultureMap.TryGetValue(country.CNT_Code, out var cultureName))
                    {
                        return new CultureInfo(cultureName);
                    }
                }
            }
            catch
            {
                // Fall through to current culture
            }

            return CultureInfo.CurrentCulture;
        }
    }
}
