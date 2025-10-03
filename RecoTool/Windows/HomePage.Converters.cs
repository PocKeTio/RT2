using System;
using System.Globalization;
using System.Windows.Data;
using System.Windows.Media;
using RecoTool.Services.Analytics;

namespace RecoTool.Windows
{
    /// <summary>
    /// Converts AlertType to border color
    /// </summary>
    public class AlertTypeToBorderColorConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value is AlertType alertType)
            {
                switch (alertType)
                {
                    case AlertType.Critical:
                        return Color.FromRgb(244, 67, 54); // Red
                    case AlertType.Warning:
                        return Color.FromRgb(255, 152, 0); // Orange
                    case AlertType.Info:
                        return Color.FromRgb(33, 150, 243); // Blue
                }
            }
            return Color.FromRgb(158, 158, 158); // Gray
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }

    /// <summary>
    /// Converts AlertType to icon (Segoe MDL2 Assets)
    /// </summary>
    public class AlertTypeToIconConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value is AlertType alertType)
            {
                switch (alertType)
                {
                    case AlertType.Critical:
                        return "\uE7BA"; // ErrorBadge
                    case AlertType.Warning:
                        return "\uE7BA"; // Warning
                    case AlertType.Info:
                        return "\uE946"; // Info
                }
            }
            return "\uE946"; // Info
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }
}
