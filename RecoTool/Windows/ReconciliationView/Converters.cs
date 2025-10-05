using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Windows.Data;
using System.Windows.Media;
using RecoTool.Models;

// Static helper class outside namespace for potential reuse
internal static class BrushCache
{
    public static readonly Brush Transparent = Brushes.Transparent;
    public static readonly Brush LightYellow = Brushes.LightYellow;
    public static readonly Brush LightBlue = Brushes.LightBlue;
    public static readonly Brush LightGreen = Brushes.LightGreen;
    public static readonly Brush LightRed = Brushes.LightCoral;
    public static readonly Brush Red = new SolidColorBrush(Color.FromRgb(244, 67, 54));      // #F44336
    public static readonly Brush Yellow = new SolidColorBrush(Color.FromRgb(255, 193, 7));   // #FFC107
    public static readonly Brush Green = new SolidColorBrush(Color.FromRgb(76, 175, 80));    // #4CAF50
}

namespace RecoTool.Windows
{
    #region Data Models

    /// <summary>
    /// Represents a user field option for ComboBox binding
    /// </summary>
    public class UserFieldOption
    {
        public int? USR_ID { get; set; }
        public string USR_FieldName { get; set; }
    }

    #endregion

    #region Action and Color Converters

    /// <summary>
    /// Converts action IDs to color brushes based on user field configuration.
    /// Only shows color for PENDING actions (not DONE).
    /// </summary>
    public class ActionColorConverter : IMultiValueConverter
    {
        // Cache: actionId -> brush, rebuilt when AllUserFields reference changes
        private IReadOnlyList<UserField> _lastAllRef;
        private Dictionary<int, Brush> _cacheByActionId = new Dictionary<int, Brush>();

        public object Convert(object[] values, Type targetType, object parameter, CultureInfo culture)
        {
            try
            {
                if (values == null || values.Length < 2) return BrushCache.Transparent;

                int? actionId = ExtractActionId(values[0]);
                var all = values[1] as IReadOnlyList<UserField>;

                if (actionId == null || all == null) return BrushCache.Transparent;

                // CRITICAL: Only show color if action is PENDING (not DONE)
                // values[2] is ActionStatus (true = DONE, false/null = PENDING)
                if (values.Length >= 3 && values[2] is bool actionStatus && actionStatus)
                {
                    return BrushCache.Transparent; // Action is DONE, no color
                }

                // Rebuild cache only when the source list instance changes (cheap reference check)
                if (!ReferenceEquals(all, _lastAllRef))
                {
                    RebuildCache(all);
                }

                return _cacheByActionId.TryGetValue(actionId.Value, out var cached)
                    ? cached ?? BrushCache.Transparent
                    : BrushCache.Transparent;
            }
            catch { return BrushCache.Transparent; }
        }

        private static int? ExtractActionId(object value)
        {
            if (value is int i) return i;
            if (value != null && int.TryParse(value.ToString(), out var parsed)) return parsed;
            return null;
        }

        private void RebuildCache(IReadOnlyList<UserField> all)
        {
            _cacheByActionId.Clear();
            foreach (var uf in all)
            {
                if (!_cacheByActionId.ContainsKey(uf.USR_ID))
                {
                    _cacheByActionId[uf.USR_ID] = ColorStringToBrush(uf?.USR_Color);
                }
            }
            _lastAllRef = all;
        }

        private static Brush ColorStringToBrush(string colorRaw)
        {
            var color = colorRaw?.Trim()?.ToUpperInvariant();
            if (string.IsNullOrEmpty(color)) return BrushCache.Transparent;

            switch (color)
            {
                case "RED": return BrushCache.LightRed;
                case "GREEN": return BrushCache.LightGreen;
                case "YELLOW": return BrushCache.LightYellow;
                case "BLUE": return BrushCache.LightBlue;
                default:
                    return TryParseCustomColor(color);
            }
        }

        private static Brush TryParseCustomColor(string color)
        {
            try
            {
                var conv = new BrushConverter();
                return conv.ConvertFromString(color) as Brush ?? BrushCache.Transparent;
            }
            catch { return BrushCache.Transparent; }
        }

        public object[] ConvertBack(object value, Type[] targetTypes, object parameter, CultureInfo culture)
        {
            throw new NotSupportedException();
        }
    }

    #endregion

    #region Reconciliation Status Converters

    /// <summary>
    /// Converter for N/U column color based on reconciliation status.
    /// RED: Not matched to DWINGS | YELLOW: Not grouped | GREEN: Fully reconciled
    /// </summary>
    public class ReconciliationStatusColorConverter : IMultiValueConverter
    {
        // values[0] = DWINGS_InvoiceID (string)
        // values[1] = InternalInvoiceReference (string)
        // values[2] = IsMatchedAcrossAccounts (bool)
        public object Convert(object[] values, Type targetType, object parameter, CultureInfo culture)
        {
            try
            {
                if (values == null || values.Length < 3) return BrushCache.Transparent;

                var dwingsInvoiceId = values[0] as string;
                var internalInvoiceRef = values[1] as string;
                var isMatched = values[2] is bool matched && matched;

                // RED: Not linked to DWINGS at all
                if (string.IsNullOrWhiteSpace(dwingsInvoiceId) && string.IsNullOrWhiteSpace(internalInvoiceRef))
                    return BrushCache.Red;

                // YELLOW: Linked to DWINGS but not grouped (no counterpart)
                    return BrushCache.Yellow;

                // GREEN: Fully reconciled (linked and grouped)
                return BrushCache.Green;
            }
            public object[] ConvertBack(object value, Type[] targetTypes, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }

    /// <summary>
{{ ... }}
        // values[2] = IsMatchedAcrossAccounts (bool)
        public object Convert(object[] values, Type targetType, object parameter, CultureInfo culture)
        {
            try
            {
                if (values == null || values.Length < 3) return Brushes.Black;

                var dwingsInvoiceId = values[0] as string;
                var internalInvoiceRef = values[1] as string;
                var isMatched = values[2] is bool matched && matched;

                // RED: Not linked to DWINGS at all (dark red background)
                if (string.IsNullOrWhiteSpace(dwingsInvoiceId) && string.IsNullOrWhiteSpace(internalInvoiceRef))
                    return Brushes.White;

                // YELLOW: Linked to DWINGS but not grouped (yellow background)
                if (!isMatched)
                    return Brushes.Black;

                // GREEN: Fully reconciled (green background)
                return Brushes.White;
            }
            catch { return Brushes.Black; }
        }

        public object[] ConvertBack(object value, Type[] targetTypes, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }

    /// <summary>
    /// Converter for N/U column tooltip based on reconciliation status
    /// </summary>
    public class ReconciliationStatusTooltipConverter : IMultiValueConverter
    {
        // values[0] = DWINGS_InvoiceID (string)
        // values[1] = InternalInvoiceReference (string)
        // values[2] = IsMatchedAcrossAccounts (bool)
        public object Convert(object[] values, Type targetType, object parameter, CultureInfo culture)
        {
            try
            {
                if (values == null || values.Length < 3) return string.Empty;

                var dwingsInvoiceId = values[0] as string;
                var internalInvoiceRef = values[1] as string;
                var isMatched = values[2] is bool matched && matched;

                // RED: Not linked to DWINGS at all
                if (string.IsNullOrWhiteSpace(dwingsInvoiceId) && string.IsNullOrWhiteSpace(internalInvoiceRef))
                {
                    return "🔴 NOT MATCHED TO DWINGS\n\nThis line is not linked to any DWINGS invoice.\nPlease set DWINGS Invoice ID or Internal Invoice Reference.";
                }

                // YELLOW: Linked to DWINGS but not grouped (no counterpart)
                if (!isMatched)
                {
                    return "🟡 NOT GROUPED\n\nThis line is linked to DWINGS but has no counterpart\non the opposite account (Pivot/Receivable).\nGroup it with matching lines to reconcile.";
                }

                // GREEN: Fully reconciled (linked and grouped)
                return "🟢 FULLY RECONCILED\n\nThis line is linked to DWINGS and properly grouped\nwith its counterpart on the opposite account.";
            }
            catch { return string.Empty; }
        }

        public object[] ConvertBack(object value, Type[] targetTypes, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }

    /// <summary>
    /// Converter for N/U column text color based on background color for good contrast.
    /// Returns white text for dark backgrounds, black text for light backgrounds.
    /// </summary>
    public class ReconciliationStatusTextColorConverter : IMultiValueConverter
    {
        // values[0] = DWINGS_InvoiceID (string)
        // values[1] = InternalInvoiceReference (string)
        // values[2] = IsMatchedAcrossAccounts (bool)
        public object Convert(object[] values, Type targetType, object parameter, CultureInfo culture)
        {
            try
            {
                if (values == null || values.Length < 3) return Brushes.Black;

                var dwingsInvoiceId = values[0] as string;
                var internalInvoiceRef = values[1] as string;
                var isMatched = values[2] is bool matched && matched;

                // RED: Not linked to DWINGS at all (dark red background)
                if (string.IsNullOrWhiteSpace(dwingsInvoiceId) && string.IsNullOrWhiteSpace(internalInvoiceRef))
                    return Brushes.White;

                // YELLOW: Linked to DWINGS but not grouped (yellow background)
                if (!isMatched)
                    return Brushes.Black;

                // GREEN: Fully reconciled (green background)
                return Brushes.White;
            }
            catch { return Brushes.Black; }
        }

        public object[] ConvertBack(object value, Type[] targetTypes, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }

    #endregion

    /// <summary>
    /// Generates filtered UserField options for comboBoxes based on category and account side
{{ ... }}
    /// </summary>
    public class UserFieldOptionsConverter : IMultiValueConverter
    {
        // values: [0]=Account_ID (string), [1]=AllUserFields (IReadOnlyList<UserField>), [2]=CurrentCountry (Country)
        public object Convert(object[] values, Type targetType, object parameter, CultureInfo culture)
        {
            try
            {
                var accountId = values?.Length > 0 ? values[0]?.ToString() : null;
                var all = values?.Length > 1 ? values[1] as IReadOnlyList<UserField> : null;
                var country = values?.Length > 2 ? values[2] as Country : null;
                var category = parameter?.ToString();

                if (all == null || string.IsNullOrWhiteSpace(category))
                    return Array.Empty<object>();

                bool isPivot = country != null && string.Equals(accountId?.Trim(), country.CNT_AmbrePivot?.Trim(), StringComparison.OrdinalIgnoreCase);
                bool isReceivable = country != null && string.Equals(accountId?.Trim(), country.CNT_AmbreReceivable?.Trim(), StringComparison.OrdinalIgnoreCase);

                // Category mapping: handle synonyms (e.g., Incident Type vs INC)
                var query = FilterByCategory(all, category);

                // Apply Pivot/Receivable filtering only when we can resolve the account side
                if (!string.IsNullOrWhiteSpace(accountId) && country != null)
                {
                    if (isPivot)
                        query = query.Where(u => u.USR_Pivot);
                    else if (isReceivable)
                        query = query.Where(u => u.USR_Receivable);
                }

                return BuildOptionsList(query);
            }
            catch { return Array.Empty<object>(); }
        }

        private static IEnumerable<UserField> FilterByCategory(IReadOnlyList<UserField> all, string category)
        {
            bool isIncident = string.Equals(category, "Incident Type", StringComparison.OrdinalIgnoreCase)
                           || string.Equals(category, "INC", StringComparison.OrdinalIgnoreCase);

            return isIncident
                ? all.Where(u => string.Equals(u.USR_Category, "Incident Type", StringComparison.OrdinalIgnoreCase)
                             || string.Equals(u.USR_Category, "INC", StringComparison.OrdinalIgnoreCase))
                : all.Where(u => string.Equals(u.USR_Category, category, StringComparison.OrdinalIgnoreCase));
        }

        private static List<UserFieldOption> BuildOptionsList(IEnumerable<UserField> query)
        {
            var list = new List<UserFieldOption>
            {
                new UserFieldOption { USR_ID = null, USR_FieldName = string.Empty }
            };

            list.AddRange(query
                .OrderBy(u => u.USR_FieldName)
                .Select(u => new UserFieldOption { USR_ID = u.USR_ID, USR_FieldName = u.USR_FieldName }));

            return list;
        }

        public object[] ConvertBack(object value, Type[] targetTypes, object parameter, CultureInfo culture)
            => throw new NotSupportedException();
    }

    /// <summary>
    /// Converts UserField ID to display name
    /// </summary>
    public class UserFieldIdToNameConverter : IMultiValueConverter
    {
        // values: [0]=int? id, [1]=AllUserFields
        public object Convert(object[] values, Type targetType, object parameter, CultureInfo culture)
        {
            try
            {
                int? id = ExtractId(values);
                var all = values?.Length > 1 ? values[1] as IReadOnlyList<UserField> : null;

                if (id == null || all == null) return string.Empty;

                var match = all.FirstOrDefault(u => u.USR_ID == id.Value);
                return match?.USR_FieldName ?? string.Empty;
            }
            catch { return string.Empty; }
        }

        private static int? ExtractId(object[] values)
        {
            if (values == null || values.Length == 0 || values[0] == null) return null;

            if (values[0] is int iid) return iid;
            if (int.TryParse(values[0].ToString(), out var parsed)) return parsed;

            return null;
        }

        public object[] ConvertBack(object value, Type[] targetTypes, object parameter, CultureInfo culture)
            => throw new NotSupportedException();
    }

    /// <summary>
    /// Converts nullable ID to/from sentinel value for ComboBox bindings
    /// </summary>
    public class NullableIdSentinelConverter : IValueConverter
    {
        public int Sentinel { get; set; } = -1;

        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            try
            {
                if (value == null) return Sentinel;
                if (value is int i) return i;
                if (int.TryParse(value?.ToString(), out var parsed)) return parsed;
                return Sentinel;
            }
            catch { return Sentinel; }
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            try
            {
                if (value == null) return null;
                if (value is int i) return i == Sentinel ? (int?)null : i;
                if (int.TryParse(value?.ToString(), out var parsed)) return parsed == Sentinel ? (int?)null : parsed;
                return null;
            }
            catch { return null; }
        }
    }

    #endregion

    #region ID to Name Converters

    /// <summary>
    /// Converts assignee ID to display name using AssigneeOptions
    /// </summary>
    public class AssigneeIdToNameConverter : IMultiValueConverter
    {
        // values: [0]=Assignee (string), [1]=AssigneeOptions (IEnumerable with Id/Name)
        public object Convert(object[] values, Type targetType, object parameter, CultureInfo culture)
        {
            try
            {
                string id = values?.Length > 0 ? values[0]?.ToString() : null;
                var options = values?.Length > 1 ? values[1] as System.Collections.IEnumerable : null;

                if (string.IsNullOrWhiteSpace(id) || options == null) return string.Empty;

                return FindNameById(options, id);
            }
            catch { return string.Empty; }
        }

        private static string FindNameById(System.Collections.IEnumerable options, string id)
        {
            foreach (var o in options)
            {
                if (o == null) continue;
                try
                {
                    var t = o.GetType();
                    var oid = t.GetProperty("Id")?.GetValue(o)?.ToString();

                    if (!string.IsNullOrEmpty(oid) && string.Equals(oid, id, StringComparison.OrdinalIgnoreCase))
                        return t.GetProperty("Name")?.GetValue(o)?.ToString() ?? string.Empty;
                }
                catch { }
            }
            return string.Empty;
        }

        public object[] ConvertBack(object value, Type[] targetTypes, object parameter, CultureInfo culture)
            => throw new NotSupportedException();
    }

    /// <summary>
    /// Maps integer ID to option name from a list of objects with Id/Name properties
    /// </summary>
    public class IdToOptionNameConverter : IMultiValueConverter
    {
        public object Convert(object[] values, Type targetType, object parameter, CultureInfo culture)
        {
            try
            {
                if (values == null || values.Length < 2 || values[0] == null) return null;

                int id = ExtractIntId(values[0]);
                if (!(values[1] is System.Collections.IEnumerable seq)) return null;

                return FindOptionNameById(seq, id);
            }
            catch { return null; }
        }

        private static int ExtractIntId(object value)
        {
            if (value is int i) return i;
            if (int.TryParse(value?.ToString(), out var parsed)) return parsed;
            throw new InvalidOperationException("Cannot extract int ID");
        }

        private static string FindOptionNameById(System.Collections.IEnumerable seq, int targetId)
        {
            foreach (var item in seq)
            {
                if (item == null) continue;

                var t = item.GetType();
                var idProp = t.GetProperty("Id");
                if (idProp == null) continue;

                var rawId = idProp.GetValue(item);
                int itemId;

                if (rawId is int ii) itemId = ii;
                else if (!int.TryParse(rawId?.ToString(), out itemId)) continue;

                if (itemId == targetId)
                {
                    var name = t.GetProperty("Name")?.GetValue(item)?.ToString();
                    return string.IsNullOrWhiteSpace(name) ? item.ToString() : name;
                }
            }
            return null;
        }

        public object[] ConvertBack(object value, Type[] targetTypes, object parameter, CultureInfo culture)
            => throw new NotSupportedException();
    }

    #endregion

    #region Enum-like String Converters

    /// <summary>
    /// Maps guarantee type codes to friendly display labels
    /// </summary>
    public class GuaranteeTypeDisplayConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            try
            {
                var s = value?.ToString()?.Trim();
                if (string.IsNullOrEmpty(s)) return string.Empty;

                switch (s.ToUpperInvariant())
                {
                    case "REISSU": return "REISSUANCE";
                    case "ISSU": return "ISSUANCE";
                    case "NOTIF": return "ADVISING";
                    default: return s;
                }
            }
            catch { return value?.ToString() ?? string.Empty; }
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            try
            {
                var s = value?.ToString()?.Trim();
                if (string.IsNullOrEmpty(s)) return null;

                switch (s.ToUpperInvariant())
                {
                    case "REISSUANCE": return "REISSU";
                    case "ISSUANCE": return "ISSU";
                    case "ADVISING": return "NOTIF";
                    default: return s;
                }
            }
            catch { return value; }
        }
    }

    public class AccountSideToFriendlyConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            try
            {
                var s = value?.ToString()?.Trim();
                if (string.IsNullOrWhiteSpace(s)) return string.Empty;

                switch (s.ToUpperInvariant())
                {
                    case "P": return "Pivot";
                    case "R": return "Receivable";
                    case "*": return "Any (*)";
                    default: return s;
                }
            }
            catch { return string.Empty; }
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
            => throw new NotImplementedException();
    }

    public class SignToFriendlyConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            try
            {
                var s = value?.ToString()?.Trim();
                if (string.IsNullOrWhiteSpace(s)) return string.Empty;

                switch (s.ToUpperInvariant())
                {
                    case "C": return "Credit";
                    case "D": return "Debit";
                    case "*": return "Any (*)";
                    default: return s;
                }
            }
            catch { return string.Empty; }
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
            => throw new NotImplementedException();
    }

    public class GuaranteeTypeToFriendlyConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            try
            {
                var s = value?.ToString()?.Trim();
                if (string.IsNullOrWhiteSpace(s)) return string.Empty;

                var u = s.ToUpperInvariant();
                if (u == "*") return "Any (*)";

                return CultureInfo.CurrentCulture.TextInfo.ToTitleCase(u.ToLowerInvariant().Replace('_', ' '));
            }
            catch { return string.Empty; }
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
            => throw new NotImplementedException();
    }

    public class TransactionTypeToFriendlyConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            try
            {
                var s = value?.ToString()?.Trim();
                if (string.IsNullOrWhiteSpace(s)) return string.Empty;

                var u = s.ToUpperInvariant();
                if (u == "*") return "Any (*)";

                return CultureInfo.CurrentCulture.TextInfo.ToTitleCase(u.ToLowerInvariant().Replace('_', ' '));
            }
            catch { return string.Empty; }
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
            => throw new NotImplementedException();
    }

    public class ApplyToToFriendlyConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            try
            {
                var s = value?.ToString()?.Trim();
                if (string.IsNullOrWhiteSpace(s)) return string.Empty;

                switch (s.ToUpperInvariant())
                {
                    case "SELF": return "Self";
                    case "COUNTERPART": return "Counterpart";
                    case "BOTH": return "Both";
                    default: return s;
                }
            }
            catch { return string.Empty; }
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
            => throw new NotImplementedException();
    }

    #endregion

    #region Badge and Status Converters

    public class ScopeToBadgeBrushConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            try
            {
                var s = value?.ToString()?.Trim();
                if (string.IsNullOrWhiteSpace(s)) return Brushes.Transparent;

                switch (s.ToUpperInvariant())
                {
                    case "BOTH": return new SolidColorBrush(Color.FromArgb(255, 204, 229, 255)); // light blue
                    case "IMPORT": return new SolidColorBrush(Color.FromArgb(255, 204, 255, 204)); // light green
                    case "EDIT": return new SolidColorBrush(Color.FromArgb(255, 255, 242, 204)); // light yellow
                    default: return Brushes.Transparent;
                }
            }
            catch { return Brushes.Transparent; }
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
            => throw new NotImplementedException();
    }

    public class PriorityToBadgeBrushConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            try
            {
                int p = value is int i ? i : (int.TryParse(value?.ToString() ?? "0", out var parsed) ? parsed : 0);

                if (p <= 25) return new SolidColorBrush(Color.FromArgb(255, 255, 204, 204)); // redish
                if (p <= 50) return new SolidColorBrush(Color.FromArgb(255, 255, 224, 178)); // orange
                if (p <= 100) return new SolidColorBrush(Color.FromArgb(255, 255, 251, 204)); // yellow
                if (p <= 200) return new SolidColorBrush(Color.FromArgb(255, 230, 244, 234)); // pale green

                return new SolidColorBrush(Color.FromArgb(255, 240, 240, 240)); // neutral gray
            }
            catch { return Brushes.Transparent; }
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
            => throw new NotImplementedException();
    }

    public class BoolToPendingDoneConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value == null || value is DBNull) return string.Empty;
            if (value is bool b) return b ? "DONE" : "PENDING";
            if (bool.TryParse(value.ToString(), out var parsed)) return parsed ? "DONE" : "PENDING";
            return string.Empty;
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            var s = value?.ToString();
            if (string.Equals(s, "DONE", StringComparison.OrdinalIgnoreCase)) return true;
            if (string.Equals(s, "PENDING", StringComparison.OrdinalIgnoreCase)) return false;
            return null;
        }
    }

    #endregion

    #region Numeric Comparison Converters

    public class IsPositiveConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value is decimal d) return d > 0;
            if (value is double db) return db > 0;
            if (value is int i) return i > 0;
            return false;
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }

    public class IsNegativeConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value is decimal d) return d < 0;
            if (value is double db) return db < 0;
            if (value is int i) return i < 0;
            return false;
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }

    #endregion

    #region Utility Converters

    public class PluralConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value == null) return string.Empty;

            int count = 0;
            if (value is int i) count = i;
            else if (value is long l) count = (int)l;
            else if (int.TryParse(value.ToString(), out var parsed)) count = parsed;

            if (parameter is string param)
            {
                var parts = param.Split('|');
                if (parts.Length == 2)
                    return count == 1 ? parts[0] : parts[1];
            }

            return count == 1 ? "item" : "items";
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }

    #endregion

    #region Static Converter Instances

    /// <summary>
    /// Exposes static converter instances for XAML x:Static usage
    /// </summary>
    public static class UIConverters
    {
        public static readonly IValueConverter ScopeToBadgeBrush = new ScopeToBadgeBrushConverter();
        public static readonly IValueConverter PriorityToBadgeBrush = new PriorityToBadgeBrushConverter();
        public static readonly IMultiValueConverter IdToOptionName = new IdToOptionNameConverter();
        public static readonly IValueConverter AccountSideToFriendly = new AccountSideToFriendlyConverter();
        public static readonly IValueConverter SignToFriendly = new SignToFriendlyConverter();
        public static readonly IValueConverter GuaranteeTypeToFriendly = new GuaranteeTypeToFriendlyConverter();
        public static readonly IValueConverter TransactionTypeToFriendly = new TransactionTypeToFriendlyConverter();
        public static readonly IValueConverter ApplyToToFriendly = new ApplyToToFriendlyConverter();
        public static readonly IValueConverter NullableIdToSentinel = new NullableIdSentinelConverter { Sentinel = -1 };
    }

    #endregion
}