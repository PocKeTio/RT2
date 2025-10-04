using System;
using System.Globalization;
using System.Text.RegularExpressions;
using RecoTool.Services.DTOs;

namespace RecoTool.Services.Helpers
{
    public static class DwingsDateHelper
    {
        /// <summary>
        /// DEPRECATED: Date normalization is now handled by lazy-loaded properties in ReconciliationViewData
        /// This method is kept for backward compatibility but does nothing
        /// </summary>
        public static void NormalizeDwingsDateStrings(ReconciliationViewData dto)
        {
            // NO-OP: Date properties are now lazy-loaded and already formatted as yyyy-MM-dd
            // All I_* and G_* date properties automatically format dates when accessed
        }

        public static bool TryParseDwingsDate(string input, out DateTime dt)
        {
            dt = default;
            if (string.IsNullOrWhiteSpace(input)) return false;

            var s = input.Trim();
            // Replace multiple spaces and normalize dashes
            s = Regex.Replace(s, "[\u2013\u2014\\-]", "-");
            s = Regex.Replace(s, "\\s+", " ").Trim();

            // Try exact formats with English month abbreviations
            var formats = new[]
            {
                "dd-MMM-yy",
                "dd-MMM-yyyy",
                "d-MMM-yy",
                "d-MMM-yyyy",
                "dd/MM/yy",
                "dd/MM/yyyy",
                "d/M/yy",
                "d/M/yyyy",
                "yyyy-MM-dd",
                "dd.MM.yyyy",
                "d.MM.yyyy",
            };

            if (DateTime.TryParseExact(s, formats, CultureInfo.InvariantCulture, DateTimeStyles.None, out dt))
                return true;

            // Fallback generic parses
            if (DateTime.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.None, out dt)) return true;
            if (DateTime.TryParse(s, CultureInfo.GetCultureInfo("fr-FR"), DateTimeStyles.None, out dt)) return true;
            if (DateTime.TryParse(s, CultureInfo.GetCultureInfo("it-IT"), DateTimeStyles.None, out dt)) return true;

            // Try uppercasing month abbreviations to help parsing
            var up = s.ToUpperInvariant();
            if (!ReferenceEquals(up, s) && DateTime.TryParseExact(up, formats, CultureInfo.InvariantCulture, DateTimeStyles.None, out dt))
                return true;

            return false;
        }
    }
}
