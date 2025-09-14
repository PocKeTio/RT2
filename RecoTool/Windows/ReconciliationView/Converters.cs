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
}
