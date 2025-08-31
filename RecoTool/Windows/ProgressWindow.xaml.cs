using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Shapes;

namespace RecoTool.Windows
{
    /// <summary>
    /// Fenêtre de progression simple
    /// </summary>
    public partial class ProgressWindow : Window
    {
        public ProgressWindow(string title)
        {
            InitializeComponent();
            Title = title;

            // Initialisation des valeurs par défaut
            MainProgressBar.Value = 0;
            StatusMessage.Text = "Preparing...";
            PercentageText.Text = "0%";
        }

        /// <summary>
        /// Met à jour la progression avec un message et un pourcentage
        /// </summary>
        /// <param name="message">Message de statut à afficher</param>
        /// <param name="progress">Pourcentage de progression (0-100)</param>
        public void UpdateProgress(string message, int progress)
        {
            // S'assurer que nous sommes sur le thread UI
            if (!Dispatcher.CheckAccess())
            {
                Dispatcher.Invoke(() => UpdateProgress(message, progress));
                return;
            }

            // Mettre à jour le titre de la fenêtre
            Title = $"{message} ({progress}%)";

            // Mettre à jour les éléments visuels
            StatusMessage.Text = message;
            MainProgressBar.Value = Math.Max(0, Math.Min(100, progress)); // Clamp entre 0 et 100
            PercentageText.Text = $"{progress}%";
        }

        /// <summary>
        /// Met à jour uniquement le message sans changer la progression
        /// </summary>
        /// <param name="message">Nouveau message de statut</param>
        public void UpdateMessage(string message)
        {
            if (!Dispatcher.CheckAccess())
            {
                Dispatcher.Invoke(() => UpdateMessage(message));
                return;
            }

            StatusMessage.Text = message;
        }

        /// <summary>
        /// Met à jour uniquement la progression sans changer le message
        /// </summary>
        /// <param name="progress">Pourcentage de progression (0-100)</param>
        public void UpdateProgress(int progress)
        {
            if (!Dispatcher.CheckAccess())
            {
                Dispatcher.Invoke(() => UpdateProgress(progress));
                return;
            }

            var clampedProgress = Math.Max(0, Math.Min(100, progress));
            MainProgressBar.Value = clampedProgress;
            PercentageText.Text = $"{clampedProgress}%";
            Title = $"{StatusMessage.Text} ({clampedProgress}%)";
        }

        /// <summary>
        /// Marque la progression comme terminée
        /// </summary>
        public void SetCompleted(string completionMessage = "Completed")
        {
            if (!Dispatcher.CheckAccess())
            {
                Dispatcher.Invoke(() => SetCompleted(completionMessage));
                return;
            }

            UpdateProgress(completionMessage, 100);
        }

        /// <summary>
        /// Displays an error in the progress window
        /// </summary>
        /// <param name="errorMessage">Error message</param>
        public void SetError(string errorMessage)
        {
            if (!Dispatcher.CheckAccess())
            {
                Dispatcher.Invoke(() => SetError(errorMessage));
                return;
            }

            StatusMessage.Text = errorMessage;
            StatusMessage.Foreground = new SolidColorBrush(Colors.Red);
            Title = "Error";
        }
    }
}