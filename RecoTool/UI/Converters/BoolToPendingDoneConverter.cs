using System;
using System.Globalization;
using System.Windows.Data;

namespace RecoTool.Converters
{
    // Converts nullable bool to "PENDING"/"DONE"/empty string for display
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
}
