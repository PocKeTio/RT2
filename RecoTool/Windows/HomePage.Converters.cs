using System;
using System.Globalization;
using System.Windows;
using System.Windows.Data;
using System.Windows.Media;
using RecoTool.Services.Analytics;

namespace RecoTool.Windows
{
    /// <summary>
    /// Converts AlertType to border color (returns Brush to avoid WPF binding warnings)
    /// </summary>
    public class AlertTypeToBorderColorConverter : IValueConverter
    {
        // Cache brushes to avoid creating new instances
        private static readonly SolidColorBrush CriticalBrush = new SolidColorBrush(Color.FromRgb(244, 67, 54)); // Red
        private static readonly SolidColorBrush WarningBrush = new SolidColorBrush(Color.FromRgb(255, 152, 0)); // Orange
        private static readonly SolidColorBrush InfoBrush = new SolidColorBrush(Color.FromRgb(33, 150, 243)); // Blue
        private static readonly SolidColorBrush DefaultBrush = new SolidColorBrush(Color.FromRgb(158, 158, 158)); // Gray

        static AlertTypeToBorderColorConverter()
        {
            // Freeze brushes for better performance
            CriticalBrush.Freeze();
            WarningBrush.Freeze();
            InfoBrush.Freeze();
            DefaultBrush.Freeze();
        }

        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            // Return Brush instead of Color to avoid WPF binding warnings
            if (value is AlertType alertType)
            {
                switch (alertType)
                {
                    case AlertType.Critical:
                        return CriticalBrush;
                    case AlertType.Warning:
                        return WarningBrush;
                    case AlertType.Info:
                        return InfoBrush;
                }
            }
            return DefaultBrush;
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

    /// <summary>
    /// Inverts a boolean value and converts to Visibility
    /// True -> Collapsed, False -> Visible
    /// </summary>
    public class InverseBooleanToVisibilityConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value is bool boolValue)
            {
                return boolValue ? Visibility.Collapsed : Visibility.Visible;
            }
            return Visibility.Collapsed;
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value is Visibility visibility)
            {
                return visibility != Visibility.Visible;
            }
            return true;
        }
    }
}
