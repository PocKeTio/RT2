using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Windows.Data;
using System.Windows.Media;
using RecoTool.Models;

namespace RecoTool.Windows
{
    // Converters extracted from ReconciliationView for clean separation
    public class ActionColorConverter : IMultiValueConverter
    {
        private static readonly Brush Transparent = Brushes.Transparent;
        private static readonly Brush LightYellow = Brushes.LightYellow;
        private static readonly Brush LightBlue = Brushes.LightBlue;
        private static readonly Brush LightGreen = Brushes.LightGreen;
        private static readonly Brush LightRed = Brushes.LightCoral;

        // Cache: actionId -> brush, rebuilt when AllUserFields reference changes
        private IReadOnlyList<UserField> _lastAllRef;
        private Dictionary<int, Brush> _cacheByActionId = new Dictionary<int, Brush>();

        public object Convert(object[] values, Type targetType, object parameter, CultureInfo culture)
        {
            try
            {
                if (values == null || values.Length < 2) return Transparent;
                int? actionId = null;
                if (values[0] is int i0) actionId = i0;
                else if (values[0] is int?) actionId = (int?)values[0];
                else if (values[0] != null && int.TryParse(values[0].ToString(), out var parsed)) actionId = parsed;

                var all = values[1] as IReadOnlyList<UserField>;
                if (actionId == null || all == null) return Transparent;
                
                // CRITICAL: Only show color if action is PENDING (not DONE)
                // values[2] is ActionStatus (true = DONE, false/null = PENDING)
                if (values.Length >= 3 && values[2] is bool actionStatus && actionStatus == true)
                {
                    return Transparent; // Action is DONE, no color
                }

                // Rebuild cache only when the source list instance changes (cheap reference check)
                if (!ReferenceEquals(all, _lastAllRef))
                {
                    _cacheByActionId.Clear();
                    // Precompute brushes for each action id
                    foreach (var uf in all)
                    {
                        if (!_cacheByActionId.ContainsKey(uf.USR_ID))
                        {
                            var brush = ToBrush(uf?.USR_Color);
                            _cacheByActionId[uf.USR_ID] = brush;
                        }
                    }
                    _lastAllRef = all;
                }

                if (_cacheByActionId.TryGetValue(actionId.Value, out var cached))
                    return cached ?? Transparent;

                return Transparent;
            }
            catch { return Transparent; }
        }

        private static Brush ToBrush(string colorRaw)
        {
            var color = colorRaw?.Trim()?.ToUpperInvariant();
            if (string.IsNullOrEmpty(color)) return Transparent;
            switch (color)
            {
                case "RED": return LightRed;
                case "GREEN": return LightGreen;
                case "YELLOW": return LightYellow;
                case "BLUE": return LightBlue;
                default:
                    try
                    {
                        var conv = new BrushConverter();
                        var b = conv.ConvertFromString(color) as Brush;
                        return b ?? Transparent;
                    }
                    catch { return Transparent; }
            }
        }

        public object[] ConvertBack(object value, Type[] targetTypes, object parameter, CultureInfo culture)
        {
            throw new NotSupportedException();
        }
    }

    // Converter: map DB guarantee type codes to friendly labels and back
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

    // Converters for UserField ComboBoxes
    public class UserFieldOption
    {
        public int? USR_ID { get; set; }
        public string USR_FieldName { get; set; }
    }

    public class UserFieldOptionsConverter : IMultiValueConverter
    {
        // values: [0]=Account_ID (string), [1]=AllUserFields (IReadOnlyList<UserField>), [2]=CurrentCountry (Country)
        public object Convert(object[] values, Type targetType, object parameter, CultureInfo culture)
        {
            try
            {
                var accountId = values != null && values.Length > 0 ? values[0]?.ToString() : null;
                var all = values != null && values.Length > 1 ? values[1] as IReadOnlyList<UserField> : null;
                var country = values != null && values.Length > 2 ? values[2] as Country : null;
                var category = parameter?.ToString();

                if (all == null || string.IsNullOrWhiteSpace(category))
                    return Array.Empty<object>();

                bool isPivot = country != null && string.Equals(accountId?.Trim(), country.CNT_AmbrePivot?.Trim(), StringComparison.OrdinalIgnoreCase);
                bool isReceivable = country != null && string.Equals(accountId?.Trim(), country.CNT_AmbreReceivable?.Trim(), StringComparison.OrdinalIgnoreCase);

                // Category mapping: handle synonyms (e.g., Incident Type vs INC)
                bool incident = string.Equals(category, "Incident Type", StringComparison.OrdinalIgnoreCase)
                                 || string.Equals(category, "INC", StringComparison.OrdinalIgnoreCase);
                IEnumerable<UserField> query = incident
                    ? all.Where(u => string.Equals(u.USR_Category, "Incident Type", StringComparison.OrdinalIgnoreCase)
                                     || string.Equals(u.USR_Category, "INC", StringComparison.OrdinalIgnoreCase))
                    : all.Where(u => string.Equals(u.USR_Category, category, StringComparison.OrdinalIgnoreCase));
                // Apply Pivot/Receivable filtering only when we can resolve the account side.
                if (!string.IsNullOrWhiteSpace(accountId) && country != null)
                {
                    if (isPivot)
                        query = query.Where(u => u.USR_Pivot);
                    else if (isReceivable)
                        query = query.Where(u => u.USR_Receivable);
                    // else unknown account side: keep all items for that category
                }

                // Build a list of UserFieldOption and prepend a placeholder with nullable USR_ID
                var list = new List<UserFieldOption>();
                list.Add(new UserFieldOption { USR_ID = null, USR_FieldName = string.Empty });
                list.AddRange(query
                    .OrderBy(u => u.USR_FieldName)
                    .Select(u => new UserFieldOption { USR_ID = u.USR_ID, USR_FieldName = u.USR_FieldName }));
                return list;
            }
            catch { return Array.Empty<object>(); }
        }

        public object[] ConvertBack(object value, Type[] targetTypes, object parameter, CultureInfo culture)
            => throw new NotSupportedException();
    }

    // Converter: map int? <-> sentinel (e.g., -1) for ComboBox SelectedValue bindings
    // Use case: allow a "— (None) —" item with Id = Sentinel to represent null in the model
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

    public class UserFieldIdToNameConverter : IMultiValueConverter
    {
        // values: [0]=int? id, [1]=AllUserFields
        public object Convert(object[] values, Type targetType, object parameter, CultureInfo culture)
        {
            try
            {
                int? id = null;
                if (values != null && values.Length > 0 && values[0] != null)
                {
                    if (values[0] is int iid) id = iid;
                    else if (int.TryParse(values[0].ToString(), out var parsed)) id = parsed;
                }
                var all = values != null && values.Length > 1 ? values[1] as IReadOnlyList<UserField> : null;
                if (id == null || all == null) return string.Empty;
                var match = all.FirstOrDefault(u => u.USR_ID == id.Value);
                return match?.USR_FieldName ?? string.Empty;
            }
            catch { return string.Empty; }
        }

        public object[] ConvertBack(object value, Type[] targetTypes, object parameter, CultureInfo culture)
            => throw new NotSupportedException();
    }

    // Converter: map Assignee (string user ID) to display name using AssigneeOptions
    public class AssigneeIdToNameConverter : IMultiValueConverter
    {
        // values: [0]=Assignee (string), [1]=AssigneeOptions (IEnumerable with Id/Name)
        public object Convert(object[] values, Type targetType, object parameter, CultureInfo culture)
        {
            try
            {
                string id = null;
                if (values != null && values.Length > 0 && values[0] != null)
                    id = values[0].ToString();
                var options = values != null && values.Length > 1 ? values[1] as System.Collections.IEnumerable : null;
                if (string.IsNullOrWhiteSpace(id) || options == null) return string.Empty;
                foreach (var o in options)
                {
                    if (o == null) continue;
                    try
                    {
                        var t = o.GetType();
                        var pid = t.GetProperty("Id");
                        var pname = t.GetProperty("Name");
                        var oid = pid?.GetValue(o)?.ToString();
                        if (!string.IsNullOrEmpty(oid) && string.Equals(oid, id, StringComparison.OrdinalIgnoreCase))
                            return pname?.GetValue(o)?.ToString() ?? string.Empty;
                    }
                    catch { }
                }
                return string.Empty;
            }
            catch { return string.Empty; }
        }

        public object[] ConvertBack(object value, Type[] targetTypes, object parameter, CultureInfo culture)
            => throw new NotSupportedException();
    }

    // Badge background for Scope values (Both/Import/Edit)
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

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture) => throw new NotImplementedException();
    }

    // Badge background for Priority values (lower = more important)
    public class PriorityToBadgeBrushConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            try
            {
                int p = 0;
                if (value is int i) p = i;
                else int.TryParse(value?.ToString() ?? "0", out p);
                if (p <= 25) return new SolidColorBrush(Color.FromArgb(255, 255, 204, 204)); // redish
                if (p <= 50) return new SolidColorBrush(Color.FromArgb(255, 255, 224, 178)); // orange
                if (p <= 100) return new SolidColorBrush(Color.FromArgb(255, 255, 251, 204)); // yellow
                if (p <= 200) return new SolidColorBrush(Color.FromArgb(255, 230, 244, 234)); // pale green
                return new SolidColorBrush(Color.FromArgb(255, 240, 240, 240)); // neutral gray
            }
            catch { return Brushes.Transparent; }
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture) => throw new NotImplementedException();
    }

    // Map an integer (or nullable) id and a list of OptionItem-like objects (Id/Name) to the display Name
    public class IdToOptionNameConverter : IMultiValueConverter
    {
        public object Convert(object[] values, Type targetType, object parameter, CultureInfo culture)
        {
            try
            {
                if (values == null || values.Length < 2) return null;
                // Value 0: the id (int or string)
                if (values[0] == null) return null;
                int id;
                if (values[0] is int i) id = i;
                else if (values[0] is int ni) id = ni;
                else if (!int.TryParse(values[0]?.ToString(), out id)) return null;

                // Value 1: the options (IEnumerable of objects with Id/Name)
                if (!(values[1] is System.Collections.IEnumerable seq)) return null;
                foreach (var item in seq)
                {
                    if (item == null) continue;
                    var t = item.GetType();
                    var idProp = t.GetProperty("Id");
                    var nameProp = t.GetProperty("Name");
                    if (idProp == null) continue;
                    var rawId = idProp.GetValue(item);
                    int itemId;
                    if (rawId is int ii) itemId = ii;
                    else if (rawId is int ni2) itemId = ni2;
                    else if (!int.TryParse(rawId?.ToString(), out itemId)) continue;
                    if (itemId == id)
                    {
                        var name = nameProp?.GetValue(item)?.ToString();
                        return string.IsNullOrWhiteSpace(name) ? item.ToString() : name;
                    }
                }
                return null;
            }
            catch { return null; }
        }

        public object[] ConvertBack(object value, Type[] targetTypes, object parameter, CultureInfo culture) => throw new NotSupportedException();
    }

    // Expose static instances for XAML x:Static usage to avoid designer construction issues
    public static class UIConverters
    {
        public static readonly IValueConverter ScopeToBadgeBrush = new ScopeToBadgeBrushConverter();
        public static readonly IValueConverter PriorityToBadgeBrush = new PriorityToBadgeBrushConverter();
        public static readonly IMultiValueConverter IdToOptionName = new IdToOptionNameConverter();
        public static readonly IValueConverter AccountSideToFriendly = new global::AccountSideToFriendlyConverter();
        public static readonly IValueConverter SignToFriendly = new global::SignToFriendlyConverter();
        public static readonly IValueConverter GuaranteeTypeToFriendly = new global::GuaranteeTypeToFriendlyConverter();
        public static readonly IValueConverter TransactionTypeToFriendly = new global::TransactionTypeToFriendlyConverter();
        public static readonly IValueConverter ApplyToToFriendly = new global::ApplyToToFriendlyConverter();
        public static readonly IValueConverter NullableIdToSentinel = new NullableIdSentinelConverter { Sentinel = -1 };
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
    
    // Converter: check if decimal? is positive (> 0)
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

    // Converter: check if decimal? is negative (< 0)
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

    // Converter: pluralize text based on count (e.g., "1 line" vs "2 lines")
    public class PluralConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value == null) return "";
            
            int count = 0;
            if (value is int i) count = i;
            else if (value is long l) count = (int)l;
            else if (int.TryParse(value.ToString(), out var parsed)) count = parsed;
            
            if (parameter is string param)
            {
                var parts = param.Split('|');
                if (parts.Length == 2)
                {
                    return count == 1 ? parts[0] : parts[1];
                }
            }
            
            return count == 1 ? "item" : "items";
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }
}

// Converter: map AccountSide to friendly display name
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

    public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture) => throw new NotImplementedException();
}

// Converter: map Sign to friendly display name
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

    public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture) => throw new NotImplementedException();
}

// Converter: map GuaranteeType to friendly display name
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
            var pretty = CultureInfo.CurrentCulture.TextInfo.ToTitleCase(u.ToLowerInvariant().Replace('_', ' '));
            return pretty;
        }
        catch { return string.Empty; }
    }

    public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture) => throw new NotImplementedException();
}

// Converter: map TransactionType (enum-like string) to friendly text
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
            var pretty = CultureInfo.CurrentCulture.TextInfo.ToTitleCase(u.ToLowerInvariant().Replace('_', ' '));
            return pretty;
        }
        catch { return string.Empty; }
    }

    public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture) => throw new NotImplementedException();
}

// Converter: map ApplyTo to friendly display name
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

    public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture) => throw new NotImplementedException();
}