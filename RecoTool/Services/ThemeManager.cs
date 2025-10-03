using System;
using System.Windows;
using System.Windows.Media;

namespace RecoTool.Services
{
    /// <summary>
    /// Manages application theme (Light/Dark mode)
    /// </summary>
    public static class ThemeManager
    {
        private static bool _isDarkMode = false;

        public static bool IsDarkMode
        {
            get => _isDarkMode;
            set
            {
                if (_isDarkMode != value)
                {
                    _isDarkMode = value;
                    ApplyTheme();
                    ThemeChanged?.Invoke(null, EventArgs.Empty);
                }
            }
        }

        public static event EventHandler ThemeChanged;

        /// <summary>
        /// Toggle between light and dark mode
        /// </summary>
        public static void ToggleTheme()
        {
            IsDarkMode = !IsDarkMode;
        }

        /// <summary>
        /// Apply the current theme to the application
        /// </summary>
        private static void ApplyTheme()
        {
            var app = Application.Current;
            if (app == null) return;

            // Update dynamic resources
            if (_isDarkMode)
            {
                ApplyDarkTheme(app);
            }
            else
            {
                ApplyLightTheme(app);
            }
        }

        private static void ApplyLightTheme(Application app)
        {
            // Background colors
            app.Resources["BNPBackground"] = new SolidColorBrush(Color.FromRgb(245, 245, 245));
            app.Resources["BNPCardBackground"] = Brushes.White;
            app.Resources["BNPBorderBrush"] = new SolidColorBrush(Color.FromRgb(224, 224, 224));
            
            // Text colors
            app.Resources["BNPTextPrimary"] = new SolidColorBrush(Color.FromRgb(34, 34, 34));
            app.Resources["BNPTextSecondary"] = new SolidColorBrush(Color.FromRgb(102, 102, 102));
            app.Resources["BNPTextTertiary"] = new SolidColorBrush(Color.FromRgb(153, 153, 153));
            
            // Keep brand colors the same
            // app.Resources["BNPMainGreenBrush"] stays as is
        }

        private static void ApplyDarkTheme(Application app)
        {
            // Background colors
            app.Resources["BNPBackground"] = new SolidColorBrush(Color.FromRgb(18, 18, 18));
            app.Resources["BNPCardBackground"] = new SolidColorBrush(Color.FromRgb(30, 30, 30));
            app.Resources["BNPBorderBrush"] = new SolidColorBrush(Color.FromRgb(60, 60, 60));
            
            // Text colors
            app.Resources["BNPTextPrimary"] = new SolidColorBrush(Color.FromRgb(255, 255, 255));
            app.Resources["BNPTextSecondary"] = new SolidColorBrush(Color.FromRgb(200, 200, 200));
            app.Resources["BNPTextTertiary"] = new SolidColorBrush(Color.FromRgb(150, 150, 150));
            
            // Keep brand colors the same
            // app.Resources["BNPMainGreenBrush"] stays as is
        }

        /// <summary>
        /// Initialize theme from saved preferences
        /// </summary>
        public static void Initialize()
        {
            try
            {
                // Load saved preference (you can save to settings file)
                // For now, default to light mode
                IsDarkMode = false;
            }
            catch
            {
                IsDarkMode = false;
            }
        }

        /// <summary>
        /// Save theme preference
        /// </summary>
        public static void SavePreference()
        {
            try
            {
                // Save to settings file or registry
                // Properties.Settings.Default.IsDarkMode = IsDarkMode;
                // Properties.Settings.Default.Save();
            }
            catch
            {
                // Ignore save errors
            }
        }
    }
}
