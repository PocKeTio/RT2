using System;
using System.Globalization;
using System.Windows;
using System.Windows.Data;

namespace RecoTool.Converters
{
    /// <summary>
    /// Convertit une valeur booléenne en Visibility
    /// </summary>
    public class BooleanToVisibilityConverter : IValueConverter
    {
        /// <summary>
        /// Convertit une valeur booléenne en Visibility (true = Visible, false = Collapsed)
        /// </summary>
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value is bool boolValue)
            {
                // Si le paramètre est spécifié et égal à "inverse", on inverse la logique
                bool invert = parameter != null && parameter.ToString().ToLower() == "inverse";
                
                if (invert)
                    return boolValue ? Visibility.Collapsed : Visibility.Visible;
                else
                    return boolValue ? Visibility.Visible : Visibility.Collapsed;
            }

            // Par défaut, si la valeur n'est pas un booléen ou est null
            return Visibility.Collapsed;
        }

        /// <summary>
        /// Convertit une Visibility en valeur booléenne (Visible = true, sinon false)
        /// </summary>
        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value is Visibility visibility)
            {
                // Si le paramètre est spécifié et égal à "inverse", on inverse la logique
                bool invert = parameter != null && parameter.ToString().ToLower() == "inverse";
                
                if (invert)
                    return visibility != Visibility.Visible;
                else
                    return visibility == Visibility.Visible;
            }

            // Par défaut, retourne false
            return false;
        }
    }
}
