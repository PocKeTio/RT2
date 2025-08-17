using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Controls.Primitives;
using System.Configuration;
using System.ComponentModel;
using RecoTool.Models;
using RecoTool.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System.Windows.Media;

namespace RecoTool.Windows
{
    /// <summary>
    /// Logique d'interaction pour MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window, INotifyPropertyChanged
    {
        private readonly OfflineFirstService _offlineFirstService;
        private AmbreImportService _ambreImportService;
        private ReconciliationService _reconciliationService;
        private List<Country> _countries;
        private string _currentCountryId;
        private string _previousCountryId;
        private UserControl _currentPage;
        private bool _isChangingCountrySelection;

        // La DI va appeler ce constructeur en lui fournissant IConfiguration et votre service
        public MainWindow(
            OfflineFirstService offlineService)
        {
            InitializeComponent();

            _offlineFirstService = offlineService;

            // DataContext pour bindings (OFFLINE, etc.)
            this.DataContext = this;

            InitializeServices();
            SetupEventHandlers();
        }

        // Allow dragging the window by holding mouse on the header area
        private void Header_MouseLeftButtonDown(object sender, MouseButtonEventArgs e)
        {
            try
            {
                if (e.ButtonState == MouseButtonState.Pressed)
                {
                    this.DragMove();
                }
            }
            catch { /* ignore */ }
        }
        
        public event PropertyChangedEventHandler PropertyChanged;
        private void OnPropertyChanged(string name) => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));

        private bool _isOffline = true;
        public bool IsOffline
        {
            get => _isOffline;
            set { _isOffline = value; OnPropertyChanged(nameof(IsOffline)); UpdateUiForConnectivity(); }
        }

        // Barre de statut (bas à droite)
        private string _initializationStatus = "Prêt";
        public string InitializationStatus
        {
            get => _initializationStatus;
            set { _initializationStatus = value; OnPropertyChanged(nameof(InitializationStatus)); }
        }

        private Brush _initializationBrush = Brushes.Gray;
        public Brush InitializationBrush
        {
            get => _initializationBrush;
            set { _initializationBrush = value; OnPropertyChanged(nameof(InitializationBrush)); }
        }

        private string _operationalDataStatus = "OFFLINE";
        public string OperationalDataStatus
        {
            get => _operationalDataStatus;
            set { _operationalDataStatus = value; OnPropertyChanged(nameof(OperationalDataStatus)); }
        }

        private bool _isInitializing;
        public bool IsInitializing
        {
            get => _isInitializing;
            set { _isInitializing = value; OnPropertyChanged(nameof(IsInitializing)); }
        }

        private void SetInitializationState(string text, Brush brush)
        {
            InitializationStatus = text;
            InitializationBrush = brush ?? Brushes.Gray;
        }

        // Référentiel (cache de données de référence)
        private string _referentialCacheStatus = "Indisponible";
        public string ReferentialCacheStatus
        {
            get => _referentialCacheStatus;
            set { _referentialCacheStatus = value; OnPropertyChanged(nameof(ReferentialCacheStatus)); }
        }

        private bool _referentialCacheAvailable;
        public bool ReferentialCacheAvailable
        {
            get => _referentialCacheAvailable;
            set { _referentialCacheAvailable = value; OnPropertyChanged(nameof(ReferentialCacheAvailable)); }
        }

        private Brush _referentialBrush = Brushes.Gray;
        public Brush ReferentialBrush
        {
            get => _referentialBrush;
            set { _referentialBrush = value; OnPropertyChanged(nameof(ReferentialBrush)); }
        }

        private void SetReferentialState(string text, Brush brush, bool available)
        {
            ReferentialCacheStatus = text;
            ReferentialBrush = brush ?? Brushes.Gray;
            ReferentialCacheAvailable = available;
        }
        #region Initialization

        /// <summary>
        /// Initialise les services avec les chaînes de connexion
        /// </summary>
        private async void InitializeServices()
        {
            try
            {
                IsInitializing = true;
                SetInitializationState("Initialisation...", Brushes.DarkOrange);
                SetReferentialState("Initialisation...", Brushes.DarkOrange, false);

                // Chargez d'abord la liste des pays via votre service
                _countries = await _offlineFirstService.GetCountries();

                if (!_countries.Any())
                {
                    ShowError("Erreur", "Aucun pays disponible.");
                    return;
                }
                else if (CountryComboBox.SelectedItem == null)
                {
                    _currentCountryId = null;
                    IsOffline = true;
                }

                // 1) S'assurer que la base DW locale est prête (copie depuis le réseau si nécessaire)
                await CopyDwingsDatabaseWithProgressAsync();

                // 2) Ne pas créer les services métiers tant que le pays n'est pas déterminé
                _ambreImportService = null;
                _reconciliationService = null;

                // Configurez la ComboBox
                CountryComboBox.ItemsSource = _countries;
                CountryComboBox.DisplayMemberPath = "CNT_Name";
                CountryComboBox.SelectedValuePath = "CNT_Id";
                
                // Récupérer la dernière country utilisée ou utiliser la première
                await SetInitialCountrySelection();

                // Fin d'init de la fenêtre, en attente d'une sélection de pays
                SetInitializationState("En attente de sélection du pays", Brushes.Gray);
                SetReferentialState("Indisponible", Brushes.Gray, false);
                IsInitializing = false;
            }
            catch (Exception ex)
            {
                ShowError("Erreur d'initialisation", ex.Message);
                SetInitializationState("Erreur d'initialisation", Brushes.Crimson);
                SetReferentialState("Erreur", Brushes.Crimson, false);
                IsInitializing = false;
            }
        }

        /// <summary>
        /// Copie la base DW (réseau -> local) avec une fenêtre de progression si nécessaire
        /// </summary>
        private async Task CopyDwingsDatabaseWithProgressAsync()
        {
            var progressWindow = new ProgressWindow("Préparation de la base DW...");
            // Définir l'owner uniquement si la fenêtre principale est déjà affichée
            if (this.IsVisible || this.IsLoaded)
            {
                progressWindow.Owner = this;
                progressWindow.WindowStartupLocation = WindowStartupLocation.CenterOwner;
            }
            else
            {
                progressWindow.WindowStartupLocation = WindowStartupLocation.CenterScreen;
            }
            progressWindow.Show();

            try
            {
                await _offlineFirstService.EnsureLocalDWCopyAsync((progress, message) =>
                {
                    Dispatcher.Invoke(() =>
                    {
                        progressWindow.UpdateProgress(message ?? "", progress);
                    });
                });
            }
            catch (Exception ex)
            {
                // Fermer la fenêtre avant d'afficher l'erreur
                progressWindow.Close();
                ShowError("Erreur", $"Copie de la base DW échouée: {ex.Message}");
                return;
            }

            progressWindow.Close();
        }

        /// <summary>
        /// Définit la sélection initiale du pays (détermine quel pays utiliser)
        /// </summary>
        private async Task SetInitialCountrySelection()
        {
            try
            {
                // Ne pas sélectionner de pays par défaut: laisser vide et forcer l'utilisateur à choisir
                _currentCountryId = null;

                // Appliquer l'absence de sélection dans l'UI
                if (CountryComboBox != null)
                {
                    CountryComboBox.SelectedIndex = -1;
                    CountryComboBox.SelectedValue = null;
                }

                // Ne pas initialiser les services ici; ils seront créés après sélection utilisateur
                IsOffline = true; // pas de pays -> OFFLINE (désactive l'UI dépendante du pays)

                // Naviguer vers la Home (fonctionne sans services pays)
                NavigateToHomePage();
            }
            catch (Exception ex)
            {
                ShowError("Avertissement", $"Impossible de définir la country initiale: {ex.Message}");
            }
        }

        /// <summary>
        /// Configure les gestionnaires d'événements
        /// </summary>
        private void SetupEventHandlers()
        {
            if (CountryComboBox != null)
            {
                CountryComboBox.SelectionChanged += CountryComboBox_SelectionChanged;
            }
        }

        /// <summary>
        /// Met à jour l'UI selon l'état de connectivité (online/offline)
        /// </summary>
        private void UpdateUiForConnectivity()
        {
            try
            {
                bool enable = !IsOffline;
                if (HomeButton != null) HomeButton.IsEnabled = true; // Home toujours accessible
                if (ReconciliationButton != null) ReconciliationButton.IsEnabled = enable;
                if (ReportsButton != null) ReportsButton.IsEnabled = enable;
                if (AccountConfigButton != null) AccountConfigButton.IsEnabled = enable;
                if (SettingsButton != null) SettingsButton.IsEnabled = true; // paramètres toujours accessibles
                if (SynchronizeButton != null) SynchronizeButton.IsEnabled = enable;
            }
            catch { /* no-op */ }
        }

        /// <summary>
        /// Gestion de la sélection de la country (booking) - obligatoire pour activer l'application
        /// </summary>
        private async void CountryComboBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            try
            {
                if (_isChangingCountrySelection) return;
                var selected = CountryComboBox?.SelectedItem as Country;
                if (selected == null)
                {
                    // Pas de sélection -> OFFLINE et services indisponibles
                    _previousCountryId = _currentCountryId;
                    _currentCountryId = null;
                    _ambreImportService = null;
                    _reconciliationService = null;
                    IsOffline = true;
                    OperationalDataStatus = "OFFLINE";
                    SetInitializationState("Aucun pays sélectionné", Brushes.Gray);
                    SetReferentialState("Indisponible", Brushes.Gray, false);
                    return;
                }

                // Initialiser les services pour le pays choisi
                var newCountryId = selected.CNT_Id;
                if (newCountryId == _currentCountryId) return;

                _previousCountryId = _currentCountryId;
                var ok = await UpdateServicesForCountry(newCountryId);
                if (!ok)
                {
                    // Echec d'initialisation (base manquante/verrouillée). Revenir à la sélection précédente ou vider.
                    ShowError("Base pays indisponible", "La base locale/réseau pour ce pays est introuvable ou verrouillée. Veuillez sélectionner un autre pays.");
                    _isChangingCountrySelection = true;
                    try
                    {
                        if (!string.IsNullOrEmpty(_previousCountryId) && _countries?.Any(c => c.CNT_Id == _previousCountryId) == true)
                        {
                            CountryComboBox.SelectedValue = _previousCountryId;
                        }
                        else
                        {
                            CountryComboBox.SelectedIndex = -1;
                        }
                    }
                    finally
                    {
                        _isChangingCountrySelection = false;
                    }
                    IsOffline = true;
                    OperationalDataStatus = "OFFLINE";
                    SetInitializationState("Erreur pays", Brushes.Crimson);
                    SetReferentialState("Erreur", Brushes.Crimson, false);
                    return;
                }
                _currentCountryId = newCountryId;
                await NotifyCurrentPageOfCountryChange();
                SetInitializationState($"Pays sélectionné: {selected.CNT_Name}", Brushes.DarkGreen);
                OperationalDataStatus = "ONLINE";
                SetReferentialState("OK", Brushes.DarkGreen, true);
            }
            catch (Exception ex)
            {
                ShowError("Erreur", $"Impossible d'initialiser le pays sélectionné: {ex.Message}");
                IsOffline = true;
                OperationalDataStatus = "OFFLINE";
                SetInitializationState("Erreur pays", Brushes.Crimson);
                SetReferentialState("Erreur", Brushes.Crimson, false);
            }
        }

        /// <summary>
        /// Met à jour les services avec la chaîne de connexion du pays sélectionné
        /// </summary>
        private async Task<bool> UpdateServicesForCountry(string countryId)
        {
            try
            {
                if (string.IsNullOrEmpty(countryId)) { IsOffline = true; return false; }

                // Mettre à jour OfflineFirstService avec le nouveau pays
                var setOk = await _offlineFirstService.SetCurrentCountryAsync(countryId);
                if (!setOk)
                {
                    // Echec de préparation de la base locale/réseau pour ce pays
                    IsOffline = true;
                    OperationalDataStatus = "OFFLINE";
                    return false;
                }

                // Récupérer la nouvelle chaîne de connexion
                var connectionString = _offlineFirstService.GetCountryConnectionString(countryId);
                var user = Environment.UserName;

                // Recréer les services avec la nouvelle chaîne
                _ambreImportService = new AmbreImportService(_offlineFirstService);
                _reconciliationService = new ReconciliationService(connectionString, user, _countries, _offlineFirstService);
                IsOffline = false; // services prêts -> ONLINE
                OperationalDataStatus = "ONLINE";
                SetReferentialState("OK", Brushes.DarkGreen, true);
                return true;
            }
            catch (Exception ex)
            {
                ShowError("Erreur", $"Impossible de mettre à jour les services pour le pays {countryId}: {ex.Message}");
                IsOffline = true;
                OperationalDataStatus = "OFFLINE";
                SetReferentialState("Erreur", Brushes.Crimson, false);
                return false;
            }
        }

        /// <summary>
        /// Notifie la page courante du changement de pays
        /// </summary>
        private async Task NotifyCurrentPageOfCountryChange()
        {
            try
            {
                // Mettre à jour les références de services si la page est HomePage
                if (_currentPage is HomePage home)
                {
                    home.UpdateServices(_offlineFirstService, _reconciliationService);
                }

                // Si la page courante implémente IRefreshable, la rafraîchir
                if (_currentPage is IRefreshable refreshablePage)
                {
                    refreshablePage.Refresh();
                }
            }
            catch (Exception ex)
            {
                // Log mais ne pas afficher d'erreur à l'utilisateur pour éviter les popups intempestifs
                System.Diagnostics.Debug.WriteLine($"Erreur lors de la notification du changement de pays: {ex.Message}");
            }
        }

        #endregion

        #region Navigation

        /// <summary>
        /// Navigue vers la page d'accueil
        /// </summary>
        private void NavigateToHomePage()
        {
            try
            {
                var homePage = new HomePage(_offlineFirstService, _reconciliationService);
                NavigateToPage(homePage);
                UpdateNavigationButtons("Home");

                // Si un pays est déjà sélectionné, rafraîchir immédiatement le dashboard
                if (!string.IsNullOrEmpty(_offlineFirstService?.CurrentCountryId))
                {
                    homePage.Refresh(); // lance le chargement async des données
                }
            }
            catch (Exception ex)
            {
                ShowError("Erreur de navigation", $"Impossible de naviguer vers la page d'accueil: {ex.Message}");
            }
        }

        /// <summary>
        /// Navigue vers une page donnée
        /// </summary>
        private void NavigateToPage(UserControl page)
        {
            if (MainContent != null)
            {
                _currentPage = page;
                MainContent.Content = page;
            }
        }

        /// <summary>
        /// Met à jour l'état des boutons de navigation
        /// </summary>
        private void UpdateNavigationButtons(string activePage)
        {
            // Reset all buttons
            if (HomeButton != null) HomeButton.Tag = "Inactive";
            if (ReconciliationButton != null) ReconciliationButton.Tag = "Inactive";
            if (ReportsButton != null) ReportsButton.Tag = "Inactive";
            if (AccountConfigButton != null) AccountConfigButton.Tag = "Inactive";
            if (SettingsButton != null) SettingsButton.Tag = "Inactive";

            // Set active button
            switch (activePage)
            {
                case "Home":
                    if (HomeButton != null) HomeButton.Tag = "Active";
                    break;
                case "Reconciliation":
                    if (ReconciliationButton != null) ReconciliationButton.Tag = "Active";
                    break;
                case "Reports":
                    if (ReportsButton != null) ReportsButton.Tag = "Active";
                    break;
                case "AccountConfig":
                    if (AccountConfigButton != null) AccountConfigButton.Tag = "Active";
                    break;
                case "Settings":
                    if (SettingsButton != null) SettingsButton.Tag = "Active";
                    break;
            }
        }

        #endregion

/// <summary>
/// Navigation vers la page d'accueil
/// </summary>
private void HomeButton_Click(object sender, RoutedEventArgs e)
{
    NavigateToHomePage();
}

/// <summary>
/// Lance une synchronisation manuelle offline-first
/// </summary>
private async void SynchronizeButton_Click(object sender, RoutedEventArgs e)
{
    try
    {
        if (string.IsNullOrEmpty(_currentCountryId))
        {
            ShowWarning("Sélection requise", "Veuillez sélectionner un pays avant de synchroniser.");
            return;
        }

        var button = sender as Button;
        if (button != null) button.IsEnabled = false;

        var progressWindow = new ProgressWindow("Synchronisation en cours...");
        progressWindow.Owner = this;
        progressWindow.Show();

        try
        {
            OperationalDataStatus = "Synchronisation...";
            SetReferentialState("Mise à jour...", Brushes.DarkOrange, true);
            var result = await _offlineFirstService.SynchronizeAsync(
                _currentCountryId,
                null,
                (progress, message) =>
                {
                    Dispatcher.Invoke(() =>
                    {
                        // ProgressWindow.UpdateProgress attend (message, progress)
                        progressWindow.UpdateProgress(message ?? "En cours...", progress);
                    });
                });

            progressWindow.Close();

            if (result != null && result.Success)
            {
                ShowInfo("Synchronisation", "Synchronisation terminée avec succès.");
                RefreshCurrentPage();
                OperationalDataStatus = "Données à jour";
                SetReferentialState("OK", Brushes.DarkGreen, true);
            }
            else
            {
                var msg = result?.Message ?? "La synchronisation a échoué.";
                ShowError("Erreur de synchronisation", msg);
                OperationalDataStatus = "Erreur";
                SetReferentialState("Erreur", Brushes.Crimson, true);
            }
        }
        catch (Exception syncEx)
        {
            progressWindow.Close();
            ShowError("Erreur de synchronisation", syncEx.Message);
            OperationalDataStatus = "Erreur";
            SetReferentialState("Erreur", Brushes.Crimson, true);
        }
        finally
        {
            if (button != null) button.IsEnabled = true;
        }
    }
    catch (Exception ex)
    {
        ShowError("Erreur", $"Erreur lors de la synchronisation: {ex.Message}");
        OperationalDataStatus = "Erreur";
        SetReferentialState("Erreur", Brushes.Crimson, false);
    }
}

        /// <summary>
        /// Navigation vers la page de réconciliation
        /// </summary>
        private void ReconciliationButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if (string.IsNullOrEmpty(_currentCountryId))
                {
                    ShowWarning("Sélection requise", "Veuillez sélectionner un pays avant d'accéder à la réconciliation.");
                    return;
                }

                var reconciliationPage = App.ServiceProvider.GetRequiredService<ReconciliationPage>();
                NavigateToPage(reconciliationPage);
                UpdateNavigationButtons("Reconciliation");
            }
            catch (Exception ex)
            {
                ShowError("Erreur de navigation", $"Impossible d'ouvrir la page de réconciliation: {ex.Message}");
            }
        }

        /// <summary>
        /// Navigation vers la page de rapports
        /// </summary>
        private void ReportsButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if (string.IsNullOrEmpty(_currentCountryId))
                {
                    ShowWarning("Sélection requise", "Veuillez sélectionner un pays avant d'accéder aux rapports.");
                    return;
                }

                // Créer la fenêtre de rapports en lui passant les services courants pour réutiliser la country sélectionnée
                var reportsWindow = new ReportsWindow(_reconciliationService, _offlineFirstService);
                reportsWindow.Owner = this;
                reportsWindow.ShowDialog();
            }
            catch (Exception ex)
            {
                ShowError("Erreur", $"Impossible d'ouvrir la fenêtre de rapports: {ex.Message}");
            }
        }

        /// <summary>
        /// Navigation vers la page de configuration des comptes
        /// </summary>
        private void AccountConfigButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                // TODO: Implémenter la page de configuration des comptes
                ShowInfo("Information", "La page de configuration des comptes sera bientôt disponible.");
            }
            catch (Exception ex)
            {
                ShowError("Erreur", $"Impossible d'ouvrir la page de configuration des comptes: {ex.Message}");
            }
        }

        /// <summary>
        /// Navigation vers la page des paramètres
        /// </summary>
        private void SettingsButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                // TODO: Implémenter la page des paramètres
                ShowInfo("Information", "La page des paramètres sera bientôt disponible.");
            }
            catch (Exception ex)
            {
                ShowError("Erreur", $"Impossible d'ouvrir la page des paramètres: {ex.Message}");
            }
        }

        /// <summary>
        /// Import de fichier Ambre
        /// </summary>
        private async void ImportButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if (string.IsNullOrEmpty(_currentCountryId))
                {
                    ShowWarning("Sélection requise", "Veuillez sélectionner un pays avant d'importer.");
                    return;
                }

                var openFileDialog = new Microsoft.Win32.OpenFileDialog
                {
                    Title = "Sélectionner le fichier Ambre à importer",
                    Filter = "Fichiers Excel (*.xlsx)|*.xlsx|Tous les fichiers (*.*)|*.*",
                    RestoreDirectory = true
                };

                if (openFileDialog.ShowDialog() == true)
                {
                    // Afficher une fenêtre de progression
                    var progressWindow = new ProgressWindow("Import en cours...");
                    progressWindow.Owner = this;
                    progressWindow.Show();

                    try
                    {
                        var result = await _ambreImportService.ImportAmbreFile(
                            openFileDialog.FileName,
                            _currentCountryId,
                            (message, progress) =>
                            {
                                Dispatcher.Invoke(() =>
                                {
                                    progressWindow.UpdateProgress(message, progress);
                                });
                            });

                        progressWindow.Close();

                        if (result.IsSuccess)
                        {
                            ShowInfo("Import réussi", $"Import terminé avec succès.\n" +
                                   $"Lignes ajoutées: {result.NewRecords}\n" +
                                   $"Lignes modifiées: {result.ProcessedRecords}\n" +
                                   $"Lignes supprimées: {result.DeletedRecords}");

                            RefreshCurrentPage();
                        }
                        else
                        {
                            ShowError("Erreur d'import", $"L'import a échoué:\n{string.Join("\n", result.Errors)}");
                        }
                    }
                    catch (Exception importEx)
                    {
                        progressWindow.Close();
                        ShowError("Erreur d'import", $"Erreur lors de l'import: {importEx.Message}");
                    }
                }
            }
            catch (Exception ex)
            {
                ShowError("Erreur", $"Erreur lors de l'import: {ex.Message}");
            }
        }

        /// <summary>
        /// Rafraîchit la page courante
        /// </summary>
        private void RefreshCurrentPage()
        {
            try
            {
                if (_currentPage is IRefreshable refreshablePage)
                {
                    refreshablePage.Refresh();
                }
            }
            catch (Exception ex)
            {
                ShowError("Erreur", $"Erreur lors du rafraîchissement: {ex.Message}");
            }
        }

        #region Window Controls

        /// <summary>
        /// Minimiser la fenêtre
        /// </summary>
        private void MinimizeButton_Click(object sender, RoutedEventArgs e)
        {
            WindowState = WindowState.Minimized;
        }

        /// <summary>
        /// Maximiser/Restaurer la fenêtre
        /// </summary>
        private void MaximizeButton_Click(object sender, RoutedEventArgs e)
        {
            WindowState = WindowState == WindowState.Maximized ? WindowState.Normal : WindowState.Maximized;
        }

        /// <summary>
        /// Fermer la fenêtre
        /// </summary>
        private void CloseButton_Click(object sender, RoutedEventArgs e)
        {
            Close();
        }


        /// <summary>
        /// Gestion du redimensionnement de la fenêtre via le Thumb
        /// </summary>
        private void Thumb_DragDelta(object sender, DragDeltaEventArgs e)
        {
            var thumb = sender as Thumb;
            if (thumb != null)
            {
                var newWidth = Width + e.HorizontalChange;
                var newHeight = Height + e.VerticalChange;

                if (newWidth > MinWidth)
                    Width = newWidth;
                if (newHeight > MinHeight)
                    Height = newHeight;
            }
        }

        #endregion

        #region Helper Methods

        /// <summary>
        /// Affiche un message d'erreur
        /// </summary>
        private void ShowError(string title, string message)
        {
            MessageBox.Show(message, title, MessageBoxButton.OK, MessageBoxImage.Error);
        }

        /// <summary>
        /// Affiche un message d'avertissement
        /// </summary>
        private void ShowWarning(string title, string message)
        {
            MessageBox.Show(message, title, MessageBoxButton.OK, MessageBoxImage.Warning);
        }

        /// <summary>
        /// Affiche un message d'information
        /// </summary>
        private void ShowInfo(string title, string message)
        {
            MessageBox.Show(message, title, MessageBoxButton.OK, MessageBoxImage.Information);
        }

        #endregion
    }
}