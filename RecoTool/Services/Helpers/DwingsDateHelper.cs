using System;
using System.Globalization;
using System.Text.RegularExpressions;
using RecoTool.Services.DTOs;

namespace RecoTool.Services.Helpers
{
    public static class DwingsDateHelper
    {
        public static void NormalizeDwingsDateStrings(ReconciliationViewData dto)
        {
            if (dto == null) return;

            string Norm(string s)
            {
                if (string.IsNullOrWhiteSpace(s)) return s;
                if (TryParseDwingsDate(s, out var dt)) return dt.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                if (DateTime.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.None, out dt)) return dt.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                if (DateTime.TryParse(s, CultureInfo.GetCultureInfo("fr-FR"), DateTimeStyles.None, out dt)) return dt.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                if (DateTime.TryParse(s, CultureInfo.GetCultureInfo("it-IT"), DateTimeStyles.None, out dt)) return dt.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                return s;
            }

            // DWINGS Invoice date-like strings
            dto.I_START_DATE = Norm(dto.I_START_DATE);
            dto.I_END_DATE = Norm(dto.I_END_DATE);
            dto.I_REQUESTED_EXECUTION_DATE = Norm(dto.I_REQUESTED_EXECUTION_DATE);

            // DWINGS Guarantee date-like strings
            dto.G_EVENT_EFFECTIVEDATE = Norm(dto.G_EVENT_EFFECTIVEDATE);
            dto.G_ISSUEDATE = Norm(dto.G_ISSUEDATE);
            dto.G_EXPIRYDATE = Norm(dto.G_EXPIRYDATE);
            dto.G_CANCELLATIONDATE = Norm(dto.G_CANCELLATIONDATE);
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
