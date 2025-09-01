using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Controls.Primitives;
using System.ComponentModel;
using RecoTool.Models;
using RecoTool.Services;
using System.Windows.Media;
using System.IO;
using System.Windows.Media.Imaging;
using Microsoft.Extensions.DependencyInjection;

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
        private bool _isCountryInitializing;

        // La DI appelle ce constructeur en fournissant les services nécessaires (ici OfflineFirstService)
        public MainWindow(
            OfflineFirstService offlineService)
        {
            InitializeComponent();

            _offlineFirstService = offlineService;

            // DataContext pour bindings (OFFLINE, etc.)
            this.DataContext = this;

            // Appliquer une icône personnalisée si paramétrée
            ApplyCustomIconIfAny();

            InitializeServices();
            SetupEventHandlers();
            SetupSyncMonitor();
            this.Closed += (s, e) =>
            {
                try
                {
                    // Best-effort final push on app exit
                    var cid = _currentCountryId;
                    if (!string.IsNullOrWhiteSpace(cid))
                    {
                        try { _ = _offlineFirstService?.PushReconciliationIfPendingAsync(cid); } catch { }
                    }
                }
                catch { }
                finally
                {
                    try { SyncMonitorService.Instance.Stop(); } catch { }
                }
            };
        }

        /// <summary>
        /// Attend (au plus timeout) que la page courante signale RefreshCompleted si elle l'expose.
        /// Supporte HomePage et ReconciliationPage; sinon, no-op.
        /// </summary>
        private async Task WaitForCurrentPageRefreshAsync(TimeSpan timeout)
        {
            try
            {
                var tcs = new TaskCompletionSource<bool>();
                EventHandler handler = null;

                // Abonnement selon le type connu
                if (_currentPage is HomePage home)
                {
                    // Si déjà non-chargement, sortir tout de suite
                    if (!home.IsLoading) return;
                    handler = (_, __) => { try { home.RefreshCompleted -= handler; } catch { } tcs.TrySetResult(true); };
                    home.RefreshCompleted += handler;
                }
                else if (_currentPage is ReconciliationPage rec)
                {
                    if (!rec.IsLoading) return;
                    handler = (_, __) => { try { rec.RefreshCompleted -= handler; } catch { } tcs.TrySetResult(true); };
                    rec.RefreshCompleted += handler;
                }
                else
                {
                    // Page ne supporte pas l'événement: petite pause visuelle
                    await Task.Delay(200);
                    return;
                }

                // Fail-safe: attendre soit l'événement, soit que IsLoading devienne false, soit le timeout
                using var cts = new System.Threading.CancellationTokenSource(timeout);
                var token = cts.Token;

                var pollTask = Task.Run(async () =>
                {
                    try
                    {
                        while (!token.IsCancellationRequested)
                        {
                            if (_currentPage is HomePage h)
                            {
                                if (!h.IsLoading) { tcs.TrySetResult(true); break; }
                            }
                            else if (_currentPage is ReconciliationPage r)
                            {
                                if (!r.IsLoading) { tcs.TrySetResult(true); break; }
                            }
                            await Task.Delay(100, token);
                        }
                    }
                    catch { tcs.TrySetResult(true); }
                }, token);

                using (token.Register(() => tcs.TrySetResult(true)))
                {
                    await tcs.Task.ConfigureAwait(true); // revenir au contexte UI
                }
            }
            catch { }
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
        private string _initializationStatus = "Ready";
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

        // Header ribbon network badge
        private string _networkStatusText = "OFFLINE";
        public string NetworkStatusText
        {
            get => _networkStatusText;
            set { _networkStatusText = value; OnPropertyChanged(nameof(NetworkStatusText)); }
        }

        private Brush _networkStatusBrush = Brushes.OrangeRed;
        public Brush NetworkStatusBrush
        {
            get => _networkStatusBrush;
            set { _networkStatusBrush = value; OnPropertyChanged(nameof(NetworkStatusBrush)); }
        }

        // Sync status indicator (status bar)
        private string _syncStatusText = "";
        public string SyncStatusText
        {
            get => _syncStatusText;
            set { _syncStatusText = value; OnPropertyChanged(nameof(SyncStatusText)); }
        }

        private Brush _syncStatusBrush = Brushes.Gray;
        public Brush SyncStatusBrush
        {
            get => _syncStatusBrush;
            set { _syncStatusBrush = value; OnPropertyChanged(nameof(SyncStatusBrush)); }
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
                SetInitializationState("Initializing...", Brushes.DarkOrange);
                SetReferentialState("Initializing...", Brushes.DarkOrange, false);

                // Chargez d'abord la liste des pays via votre service
                _countries = await _offlineFirstService.GetCountries();

                if (!_countries.Any())
                {
                    ShowError("Error", "No country available.");
                    return;
                }
                else if (CountryComboBox.SelectedItem == null)
                {
                    _currentCountryId = null;
                    IsOffline = true;
                }

                // Ne pas copier DW au démarrage: cela sera géré après sélection de pays via OfflineFirstService.SetCurrentCountryAsync

                // Ne pas créer les services métiers tant que le pays n'est pas déterminé
                _ambreImportService = null;
                _reconciliationService = null;

                // Configurez la ComboBox
                CountryComboBox.ItemsSource = _countries;
                CountryComboBox.DisplayMemberPath = "CNT_Name";
                CountryComboBox.SelectedValuePath = "CNT_Id";
                
                // Récupérer la dernière country utilisée ou utiliser la première
                await SetInitialCountrySelection();

                // Fin d'init de la fenêtre, en attente d'une sélection de pays
                SetInitializationState("Waiting for country selection", Brushes.Gray);
                SetReferentialState("Indisponible", Brushes.Gray, false);
                IsInitializing = false;
            }
            catch (Exception ex)
            {
                ShowError("Initialization error", ex.Message);
                SetInitializationState("Initialization error", Brushes.Crimson);
                SetReferentialState("Error", Brushes.Crimson, false);
                IsInitializing = false;
            }
        }

        /// <summary>
        /// Copie la base DW (réseau -> local) avec une fenêtre de progression si nécessaire
        /// </summary>
        private async Task CopyDwingsDatabaseWithProgressAsync()
        {
            var progressWindow = new ProgressWindow("Preparing DW database...");
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
            // S'assurer qu'elle reste au premier plan pendant la copie initiale
            progressWindow.Topmost = true;
            progressWindow.ShowActivated = true;
            progressWindow.Show();
            progressWindow.Activate();

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
                progressWindow.Topmost = false;
                progressWindow.Close();
                ShowError("Error", $"DW database copy failed: {ex.Message}");
                return;
            }

            // Rétablir avant fermeture
            progressWindow.Topmost = false;
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
                ShowError("Warning", $"Unable to set initial country: {ex.Message}");
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
        /// Configure and start SyncMonitorService to opportunistically push reconciliation changes.
        /// </summary>
        private void SetupSyncMonitor()
        {
            try
            {
                var monitor = SyncMonitorService.Instance;
                monitor.Initialize(() => _offlineFirstService);

                // Initialize the network indicator immediately so UI matches current availability
                try
                {
                    var online = _offlineFirstService?.IsNetworkSyncAvailable == true;
                    IsOffline = !online;
                    NetworkStatusText = online ? "ONLINE" : "OFFLINE";
                    NetworkStatusBrush = online ? Brushes.MediumSeaGreen : Brushes.OrangeRed;
                }
                catch { }

                monitor.NetworkBecameAvailable += () => TryBackgroundPush();
                monitor.LockReleased += () => TryBackgroundPush();
                monitor.SyncSuggested += (_) => TryBackgroundPush();
                monitor.SyncStateChanged += (e) =>
                {
                    try
                    {
                        Dispatcher.Invoke(() =>
                        {
                            switch (e.State)
                            {
                                case OfflineFirstService.SyncStateKind.SyncInProgress:
                                    SyncStatusText = e.PendingCount > 0 ? $"🔄 Syncing... ({e.PendingCount})" : "🔄 Syncing...";
                                    SyncStatusBrush = Brushes.DarkOrange;
                                    break;
                                case OfflineFirstService.SyncStateKind.UpToDate:
                                    SyncStatusText = "✅ Up to date";
                                    SyncStatusBrush = Brushes.DarkGreen;
                                    break;
                                case OfflineFirstService.SyncStateKind.OfflinePending:
                                    SyncStatusText = e.PendingCount > 0 ? $"⚠️ Offline ({e.PendingCount} pending)" : "⚠️ Offline";
                                    SyncStatusBrush = Brushes.Goldenrod;
                                    break;
                                case OfflineFirstService.SyncStateKind.Error:
                                    var msg = e.LastError?.Message;
                                    SyncStatusText = string.IsNullOrWhiteSpace(msg) ? "⚠️ Error" : $"⚠️ Error: {msg}";
                                    SyncStatusBrush = Brushes.Crimson;
                                    break;
                            }
                        });
                    }
                    catch { }
                };
                monitor.NetworkAvailabilityChanged += (online) =>
                {
                    Dispatcher.Invoke(() =>
                    {
                        IsOffline = !online;
                        if (online)
                        {
                            NetworkStatusText = "ONLINE";
                            NetworkStatusBrush = Brushes.MediumSeaGreen;
                        }
                        else
                        {
                            NetworkStatusText = "OFFLINE";
                            NetworkStatusBrush = Brushes.OrangeRed;
                        }
                    });
                };
                monitor.Start();
            }
            catch { }
        }

        private void TryBackgroundPush()
        {
            try
            {
                var cid = _currentCountryId;
                if (string.IsNullOrWhiteSpace(cid)) return;
                if (_isCountryInitializing) return;
                _ = _offlineFirstService.PushReconciliationIfPendingAsync(cid);
            }
            catch { }
        }

        /// <summary>
        /// Open the DWINGS Buttons window to manage TRIGGER actions in bulk
        /// </summary>
        private void DwingsButtonsButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(_currentCountryId))
                {
                    ShowWarning("Selection required", "Please select a country before opening DWINGS BUTTONS.");
                    return;
                }

                var win = new DwingsButtonsWindow(_offlineFirstService, _reconciliationService, _currentCountryId)
                {
                    Owner = this,
                    WindowStartupLocation = WindowStartupLocation.CenterOwner
                };
                win.Show();
            }
            catch (Exception ex)
            {
                ShowError("Error", $"Unable to open DWINGS BUTTONS: {ex.Message}");
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
                    SetInitializationState("No country selected", Brushes.Gray);
                    SetReferentialState("Indisponible", Brushes.Gray, false);
                    return;
                }

                // Initialiser les services pour le pays choisi
                var newCountryId = selected.CNT_Id;
                if (newCountryId == _currentCountryId) return;

                _previousCountryId = _currentCountryId;
                _isChangingCountrySelection = true;
                Mouse.OverrideCursor = Cursors.Wait;
                ProgressWindow progressWindow = null;
                try
                {
                    // Afficher une ProgressWindow pendant tout le traitement (copie, synchro, etc.)
                    progressWindow = new ProgressWindow("Initializing country...");
                    progressWindow.Owner = this;
                    progressWindow.Show();
                    progressWindow.UpdateProgress("Preparing...", 5);

                    // Créer un callback de progression pour relayer les mises à jour vers la ProgressWindow
                    Action<int, string> onProgress = (progress, message) =>
                    {
                        try
                        {
                            Dispatcher.Invoke(() =>
                            {
                                var pct = Math.Max(0, Math.Min(99, progress)); // réserver 100% pour la fin
                                progressWindow.UpdateProgress(message ?? "En cours...", pct);
                            });
                        }
                        catch { /* best-effort UI update */ }
                    };

                    var ok = await UpdateServicesForCountry(newCountryId, onProgress);
                    if (!ok)
                    {
                        // Fermer la fenêtre avant retour à l'état précédent
                        try { progressWindow?.Close(); } catch { }
                        // Echec d'initialisation (base manquante/verrouillée). Revenir à la sélection précédente ou vider.
                        ShowError("Country database unavailable", "The local/network database for this country is missing or locked. Please select another country.");
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
                        SetInitializationState("Country error", Brushes.Crimson);
                        SetReferentialState("Error", Brushes.Crimson, false);
                        return;
                    }
                    progressWindow.UpdateProgress("Finalisation...", 95);
                    _currentCountryId = newCountryId;
                    await NotifyCurrentPageOfCountryChange();

                    // Attendre la fin du premier rafraîchissement de la page courante (si exposé)
                    progressWindow.UpdateProgress("Loading data...", 98);
                    await WaitForCurrentPageRefreshAsync(TimeSpan.FromSeconds(15));
                    SetInitializationState($"Country selected: {selected.CNT_Name}", Brushes.DarkGreen);
                    OperationalDataStatus = "ONLINE";
                    SetReferentialState("OK", Brushes.DarkGreen, true);
                }
                finally
                {
                    try { progressWindow?.Close(); } catch { }
                    Mouse.OverrideCursor = null; // restore default cursor
                    _isChangingCountrySelection = false;
                }
            }
            catch (Exception ex)
            {
                // S'assurer que toute fenêtre de progression résiduelle est fermée
                try
                {
                    foreach (Window w in Application.Current.Windows)
                    {
                        if (w is ProgressWindow pw && pw.Owner == this) { pw.Close(); }
                    }
                }
                catch { }
                ShowError("Error", $"Unable to initialize selected country: {ex.Message}");
                IsOffline = true;
                OperationalDataStatus = "OFFLINE";
                SetInitializationState("Country error", Brushes.Crimson);
                SetReferentialState("Error", Brushes.Crimson, false);
            }
        }

        /// <summary>
        /// Met à jour les services avec la chaîne de connexion du pays sélectionné
        /// </summary>
        private async Task<bool> UpdateServicesForCountry(string countryId, Action<int, string> onProgress = null)
        {
            try
            {
                if (string.IsNullOrEmpty(countryId)) { IsOffline = true; return false; }

                // Guard: we are initializing a new country; prevent page refresh until done
                _isCountryInitializing = true;

                // Mettre à jour OfflineFirstService avec le nouveau pays
                var setOk = await _offlineFirstService.SetCurrentCountryAsync(countryId, suppressPush: false, onProgress: onProgress);
                if (!setOk)
                {
                    // Echec de préparation de la base locale/réseau pour ce pays
                    IsOffline = true;
                    OperationalDataStatus = "OFFLINE";
                    return false;
                }

                // 0) Vérifier que la version du ZIP AMBRE local correspond à la version réseau
                bool zipOk = false;
                onProgress?.Invoke(82, "Checking AMBRE (ZIP)");
                try { zipOk = await _offlineFirstService.IsLocalAmbreZipInSyncWithNetworkAsync(countryId); } catch { zipOk = false; }
                if (!zipOk)
                {
                    // Tenter une mise à jour automatique depuis le réseau
                    try
                    {
                        onProgress?.Invoke(84, "Updating AMBRE from network...");
                        await _offlineFirstService.CopyNetworkToLocalAmbreAsync(countryId);
                        zipOk = await _offlineFirstService.IsLocalAmbreZipInSyncWithNetworkAsync(countryId);
                    }
                    catch { zipOk = false; }

                    if (!zipOk)
                    {
                        // Tentative d'initialisation AMBRE si le contenu réseau est absent
                        try
                        {
                            onProgress?.Invoke(85, "Initializing AMBRE (network creation)...");
                            var recreationService = new DatabaseRecreationService();
                            var report = await recreationService.RecreateAmbreAsync(_offlineFirstService, countryId);
                            if (!(report?.Success ?? false))
                            {
                                var details = string.Join("\n", report?.Errors ?? new List<string>());
                                if (!string.IsNullOrWhiteSpace(details))
                            ShowWarning("AMBRE initialization", details);
                            }
                            // Re-vérifier l'alignement ZIP après (éventuelle) création/publish
                            zipOk = await _offlineFirstService.IsLocalAmbreZipInSyncWithNetworkAsync(countryId);
                        }
                        catch { zipOk = false; }

                        if (!zipOk)
                        {
                            // Bloquer l'initialisation tant que la version locale ne correspond pas
                            IsOffline = true;
                            OperationalDataStatus = "OFFLINE";
                            SetReferentialState("AMBRE out of sync", Brushes.Crimson, false);
                            var ambreDiag = _offlineFirstService.GetAmbreZipDiagnostics(countryId);
                            ShowError("AMBRE data not up to date", "The local AMBRE ZIP does not match the network version. Please try again later or check network share access.\n\nDetails:\n" + ambreDiag);
                            return false;
                        }
                    }
                }
                onProgress?.Invoke(86, "AMBRE OK");

                // 0.b) Vérifier également la version du ZIP DW local vs réseau
                bool dwZipOk = false;
                onProgress?.Invoke(87, "Checking DW (ZIP)");
                try { dwZipOk = await _offlineFirstService.IsLocalDwZipInSyncWithNetworkAsync(countryId); } catch { dwZipOk = false; }
                if (!dwZipOk)
                {
                    try
                    {
                        onProgress?.Invoke(88, "Updating DW from network...");
                        await _offlineFirstService.CopyNetworkToLocalDwAsync(countryId);
                        dwZipOk = await _offlineFirstService.IsLocalDwZipInSyncWithNetworkAsync(countryId);
                    }
                    catch { dwZipOk = false; }

                    if (!dwZipOk)
                    {
                        IsOffline = true;
                        OperationalDataStatus = "OFFLINE";
                        SetReferentialState("DW out of sync", Brushes.Crimson, false);
                        var dwDiag = _offlineFirstService.GetDwZipDiagnostics(countryId);
                        ShowError("DW data not up to date", "The local DW ZIP does not match the network version. Please try again later or check network share access.\n\nDetails:\n" + dwDiag);
                        return false;
                    }
                }
                onProgress?.Invoke(90, "DW OK");

                // Optional: Recreate DWINGS databases at startup if the flag is enabled
                try
                {
                    var settings = RecoTool.Properties.Settings.Default;
                    if (settings != null && settings.RecreateDwingsDatabasesAtStartup)
                    {
                    onProgress?.Invoke(91, "Recreating DWINGS databases...");
                        // Obtain target directory and DW prefix from OfflineFirstService parameters
                        var dataDirectory = _offlineFirstService.GetParameter("DataDirectory");
                        var dwPrefix = _offlineFirstService.GetParameter("DWDatabasePrefix");
                        if (string.IsNullOrWhiteSpace(dwPrefix))
                            dwPrefix = _offlineFirstService.GetParameter("CountryDatabasePrefix") ?? "DB_";

                        // Execute recreation (non-blocking to the rest of init; we warn on failure but continue)
                        var recreationService = new DatabaseRecreationService();
                        var report = await recreationService.RecreateAllAsync(dataDirectory, dwPrefix, countryId);
                        if (!report.Success)
                        {
                            var details = string.Join("\n", report.Errors ?? new List<string>());
                            ShowWarning("DWINGS recreation", string.IsNullOrWhiteSpace(details)
                                ? "Recreating DWINGS databases encountered errors. Please check logs."
                                : ("Recreating DWINGS databases encountered errors:\n" + details));
                        }
                        else
                        {
                    onProgress?.Invoke(92, "DWINGS recreated");
                        }
                    }
                }
                catch (Exception ex)
                {
                    // Best-effort: do not block country initialization on recreation failure
                    ShowWarning("DWINGS recreation", $"Failed to recreate DWINGS databases: {ex.Message}");
                }

                // Vérifier/initialiser la base de Réconciliation (RECON)
                try
                {
                onProgress?.Invoke(92, "Checking RECON...");
                    try
                    {
                        // Tenter d'aligner la base locale depuis le réseau si elle existe déjà
                        await _offlineFirstService.CopyNetworkToLocalReconciliationAsync(countryId);
                        onProgress?.Invoke(92, "RECON OK");
                    }
                    catch (System.IO.FileNotFoundException)
                    {
                        // Si la base réseau est absente, créer localement et publier vers le réseau
                        onProgress?.Invoke(92, "Initializing RECON (network creation)...");
                        var recreationService = new DatabaseRecreationService();
                        var report = await recreationService.RecreateReconciliationAsync(_offlineFirstService, countryId);
                        if (!(report?.Success ?? false))
                        {
                            var details = string.Join("\n", report?.Errors ?? new List<string>());
                            ShowWarning("Reconciliation initialization", string.IsNullOrWhiteSpace(details)
                                ? "Creating the reconciliation database encountered errors. Please check logs."
                                : details);
                        }
                        else
                        {
                            onProgress?.Invoke(92, "RECON created");
                        }
                    }
                    catch (InvalidOperationException)
                    {
                        // Des changements locaux non synchronisés peuvent bloquer le refresh réseau->local.
                        // Dans ce cas, ne pas bloquer l'initialisation: la base locale existe et sera poussée plus tard.
                    }
                    catch (Exception)
                    {
                        // Best-effort: ne pas bloquer le flux si indisponibilité passagère du réseau
                    }
                }
                catch { }

                // 1) S'assurer que les instantanés locaux AMBRE et DW sont à jour
                try { await _offlineFirstService.EnsureLocalSnapshotsUpToDateAsync(countryId, onProgress); } catch { }

                // 2) Déclencher un push granulair en arrière-plan (ne bloque pas l'UI)
                try { _ = _offlineFirstService.PushReconciliationIfPendingAsync(countryId); } catch { }

                // Récupérer la nouvelle chaîne de connexion
                onProgress?.Invoke(96, "Initializing services...");
                var connectionString = _offlineFirstService.GetCountryConnectionString(countryId);
                var user = Environment.UserName;

                // Recréer les services avec la nouvelle chaîne
                _ambreImportService = new AmbreImportService(_offlineFirstService);
                _reconciliationService = new ReconciliationService(connectionString, user, _countries, _offlineFirstService);
                IsOffline = false; // services prêts -> ONLINE
                OperationalDataStatus = "ONLINE";
                SetReferentialState("OK", Brushes.DarkGreen, true);
                onProgress?.Invoke(99, "Finalisation...");
                return true;
            }
            catch (Exception ex)
            {
                ShowError("Error", $"Unable to update services for country {countryId}: {ex.Message}");
                IsOffline = true;
                OperationalDataStatus = "OFFLINE";
                SetReferentialState("Error", Brushes.Crimson, false);
                return false;
            }
            finally
            {
                _isCountryInitializing = false;
            }
        }

        /// <summary>
        /// Notifie la page courante du changement de pays
        /// </summary>
        private async Task NotifyCurrentPageOfCountryChange()
        {
            try
            {
                // Si l'initialisation du pays est encore en cours, ne pas rafraîchir maintenant
                if (_isCountryInitializing)
                {
                    System.Diagnostics.Debug.WriteLine("NotifyCurrentPageOfCountryChange: skipped refresh (country initialization in progress)");
                    return;
                }

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
                System.Diagnostics.Debug.WriteLine($"Error notifying country change: {ex.Message}");
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
                ShowError("Navigation error", $"Unable to navigate to home page: {ex.Message}");
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
                case "Settings":
                    if (SettingsButton != null) SettingsButton.Tag = "Active";
                    break;
            }
        }

        #endregion

/// <summary>
/// Navigation vers la page d'accueil
/// </summary>
private async void HomeButton_Click(object sender, RoutedEventArgs e)
{
    try
    {
        Mouse.OverrideCursor = Cursors.Wait;
        NavigateToHomePage();
        // Attendre que la Home termine son rafraîchissement initial si applicable
        await WaitForCurrentPageRefreshAsync(TimeSpan.FromSeconds(10));
    }
    finally
    {
        Mouse.OverrideCursor = null;
    }
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
            ShowWarning("Selection required", "Please select a country before synchronizing.");
            return;
        }

        var button = sender as Button;
        if (button != null) button.IsEnabled = false;

        var progressWindow = new ProgressWindow("Synchronization in progress...");
        progressWindow.Owner = this;
        progressWindow.Show();

        try
        {
            OperationalDataStatus = "Synchronizing...";
            SetReferentialState("Updating...", Brushes.DarkOrange, true);
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
                ShowInfo("Synchronization", "Synchronization completed successfully.");
                RefreshCurrentPage();
                OperationalDataStatus = "Data up to date";
                SetReferentialState("OK", Brushes.DarkGreen, true);
            }
            else
            {
                var msg = result?.Message ?? "Synchronization failed.";
                ShowError("Synchronization error", msg);
                OperationalDataStatus = "Error";
                SetReferentialState("Error", Brushes.Crimson, true);
            }
        }
        catch (Exception syncEx)
        {
            progressWindow.Close();
            ShowError("Synchronization error", syncEx.Message);
            OperationalDataStatus = "Error";
            SetReferentialState("Error", Brushes.Crimson, true);
        }
        finally
        {
            if (button != null) button.IsEnabled = true;
        }
    }
    catch (Exception ex)
    {
        ShowError("Error", $"Error during synchronization: {ex.Message}");
        OperationalDataStatus = "Error";
        SetReferentialState("Error", Brushes.Crimson, false);
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
                    ShowWarning("Selection required", "Please select a country before accessing reconciliation.");
                    return;
                }

                var reconciliationPage = App.ServiceProvider.GetRequiredService<ReconciliationPage>();
                NavigateToPage(reconciliationPage);
                UpdateNavigationButtons("Reconciliation");
            }
            catch (Exception ex)
            {
                ShowError("Navigation error", $"Unable to open reconciliation page: {ex.Message}");
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
                    ShowWarning("Selection required", "Please select a country before accessing reports.");
                    return;
                }

                // Créer la fenêtre de rapports en lui passant les services courants pour réutiliser la country sélectionnée
                var reportsWindow = new ReportsWindow(_reconciliationService, _offlineFirstService);
                reportsWindow.Owner = this;
                reportsWindow.ShowDialog();
            }
            catch (Exception ex)
            {
                ShowError("Error", $"Unable to open reports window: {ex.Message}");
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
            ShowInfo("Information", "The account configuration page will be available soon.");
            }
            catch (Exception ex)
            {
                ShowError("Error", $"Unable to open account configuration page: {ex.Message}");
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
            ShowInfo("Information", "The settings page will be available soon.");
            }
            catch (Exception ex)
            {
                ShowError("Error", $"Unable to open settings page: {ex.Message}");
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
                    ShowWarning("Selection required", "Please select a country before importing.");
                    return;
                }

                var openFileDialog = new Microsoft.Win32.OpenFileDialog
                {
                    Title = "Select Ambre file to import",
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
                            ShowInfo("Import successful", $"Import completed successfully.\n" +
                                   $"Rows added: {result.NewRecords}\n" +
                                   $"Rows updated: {result.ProcessedRecords}\n" +
                                   $"Rows deleted: {result.DeletedRecords}");

                            RefreshCurrentPage();
                        }
                        else
                        {
                            ShowError("Import error", $"Import failed:\n{string.Join("\n", result.Errors)}");
                        }
                    }
                    catch (Exception importEx)
                    {
                        progressWindow.Close();
                        ShowError("Import error", $"Error during import: {importEx.Message}");
                    }
                }
            }
            catch (Exception ex)
            {
                ShowError("Error", $"Error during import: {ex.Message}");
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
                ShowError("Error", $"Error during refresh: {ex.Message}");
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
        /// Applique l'icône personnalisée si la clé AppIcon est renseignée (chemin .ico)
        /// </summary>
        private void ApplyCustomIconIfAny()
        {
            try
            {
                string iconPath = null;

                try
                {
                    var settings = RecoTool.Properties.Settings.Default;
                    var prop = settings?.GetType()?.GetProperty("AppIcon");
                    if (prop != null)
                    {
                        iconPath = prop.GetValue(settings) as string;
                    }
                }
                catch { /* ignore */ }

                 if (!string.IsNullOrWhiteSpace(iconPath)
                    && File.Exists(iconPath)
                    && string.Equals(Path.GetExtension(iconPath), ".ico", StringComparison.OrdinalIgnoreCase))
                {
                    var uri = new Uri(iconPath, UriKind.Absolute);
                    this.Icon = BitmapFrame.Create(uri);
                }
            }
            catch { /* ne pas bloquer l'UI si l'icône est invalide */ }
        }

        /// <summary>
        /// Affiche un message d'erreur
        /// </summary>
        private void ShowError(string title, string message)
        {
            try { System.Diagnostics.Debug.WriteLine($"[ERROR] {title}: {message}"); } catch { }
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