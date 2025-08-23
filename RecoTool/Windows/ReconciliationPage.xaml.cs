using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Input;
using RecoTool.Models;
using RecoTool.Services;
using System.Windows.Threading;
using System.Windows.Controls.Primitives;

namespace RecoTool.Windows
{
    /// <summary>
    /// Logique d'interaction pour ReconciliationPage.xaml
    /// Page principale de réconciliation avec gestion des vues et filtres
    /// </summary>
    public partial class ReconciliationPage : UserControl, IRefreshable, INotifyPropertyChanged
    {
        #region Fields and Properties

        private readonly ReconciliationService _reconciliationService;
        private readonly OfflineFirstService _offlineFirstService;
        private string _currentCountryId;
        private List<Country> _availableCountries;
        private ObservableCollection<UserFilter> _savedFilters;
        private bool _isLoading;
        private string _currentFilter;
        private string _currentFilterName;
        private bool _isLoadingFilters;
        private ObservableCollection<string> _viewTypes;
        private string _selectedViewType;
        private ObservableCollection<string> _accounts;
        private string _selectedAccount;
        private ObservableCollection<UserViewPreset> _savedViews;
        private UserViewPreset _selectedSavedView;
        private ObservableCollection<string> _statuses;
        private string _selectedStatus;
        private bool _isGlobalLockActive;
        private bool _isAutoSyncRunning;
        private bool _prevNetworkAvailable;
        private DateTime _lastAutoSyncUtc = DateTime.MinValue;
        private static readonly TimeSpan AutoSyncCooldown = TimeSpan.FromSeconds(15);
        private Action<bool> _onLockStateChanged;
        private Action<string> _onSyncSuggested;
        private bool _isLoadingViews; // Guard for SavedViews repopulation
        private bool _skipReloadSavedLists; // Prevent reloading saved filters/views during data refresh

        /// <summary>
        /// Résout l'ID de compte réel à partir d'un libellé d'affichage (ex: "Pivot (ID)")
        /// </summary>
        private string ResolveSelectedAccountIdForFilter(string display)
        {
            // Si format "Label (ID)", extraire l'ID entre parenthèses
            var open = display.LastIndexOf('(');
            var close = display.LastIndexOf(')');
            if (open >= 0 && close > open)
            {
                var inner = display.Substring(open + 1, close - open - 1).Trim();
                if (!string.IsNullOrWhiteSpace(inner)) return inner;
            }

            // Fallback: si le texte est exactement Pivot/Receivable sans ID, renvoyer l'ID depuis le référentiel
            var country = _offlineFirstService?.CurrentCountry;
            if (country != null)
            {
                if (string.Equals(display, "Pivot", StringComparison.OrdinalIgnoreCase))
                    return country.CNT_AmbrePivot;
                if (string.Equals(display, "Receivable", StringComparison.OrdinalIgnoreCase))
                    return country.CNT_AmbreReceivable;
            }

            // Sinon, on considère que l'utilisateur a choisi un ID brut
            return display;
        }

        public List<Country> AvailableCountries
        {
            get => _availableCountries;
            set
            {
                _availableCountries = value;
                OnPropertyChanged(nameof(AvailableCountries));
            }
        }

        public Country CurrentCountry => _offlineFirstService?.CurrentCountry;

        public ObservableCollection<UserFilter> SavedFilters
        {
            get => _savedFilters;
            set
            {
                _savedFilters = value;
                OnPropertyChanged(nameof(SavedFilters));
            }
        }

        public bool IsLoading
        {
            get => _isLoading;
            set
            {
                _isLoading = value;
                OnPropertyChanged(nameof(IsLoading));
                _canRefresh = !value;
                OnPropertyChanged(nameof(CanRefresh));
                OnPropertyChanged(nameof(CanInteract));
            }
        }

        /// <summary>
        /// Indique si un verrou global d'import est actif côté serveur (UI restreinte)
        /// </summary>
        public bool IsGlobalLockActive
        {
            get => _isGlobalLockActive;
            private set
            {
                if (_isGlobalLockActive != value)
                {
                    _isGlobalLockActive = value;
                    OnPropertyChanged(nameof(IsGlobalLockActive));
                    OnPropertyChanged(nameof(CanInteract));
                }
            }
        }

        /// <summary>
        /// Contrôle global d'interaction de l'UI (désactive boutons/combos en cas de lock ou chargement)
        /// </summary>
        public bool CanInteract => !_isLoading && !_isGlobalLockActive;

        public ObservableCollection<string> Accounts
        {
            get => _accounts;
            set
            {
                _accounts = value;
                OnPropertyChanged(nameof(Accounts));
            }
        }

        public string SelectedAccount
        {
            get => _selectedAccount;
            set
            {
                // Conserver la valeur d'affichage (ex: "Pivot (ID)") et résoudre l'ID côté prédicat de filtre
                _selectedAccount = value;
                OnPropertyChanged(nameof(SelectedAccount));
                // Legacy page-level DataGrid removed; views handle their own filtering
            }
        }

        public ObservableCollection<UserViewPreset> SavedViews
        {
            get => _savedViews;
            set { _savedViews = value; OnPropertyChanged(nameof(SavedViews)); }
        }

        public UserViewPreset SelectedSavedView
        {
            get => _selectedSavedView;
            set { _selectedSavedView = value; OnPropertyChanged(nameof(SelectedSavedView)); /* TODO: Apply preset */ }
        }

        public ObservableCollection<string> Statuses
        {
            get => _statuses;
            set
            {
                _statuses = value;
                OnPropertyChanged(nameof(Statuses));
            }
        }

        public string SelectedStatus
        {
            get => _selectedStatus;
            set
            {
                _selectedStatus = value;
                OnPropertyChanged(nameof(SelectedStatus));
                // Legacy page-level DataGrid removed; views handle their own filtering
            }
        }

        public ObservableCollection<string> ViewTypes
        {
            get => _viewTypes;
            set
            {
                _viewTypes = value;
                OnPropertyChanged(nameof(ViewTypes));
            }
        }

        public string SelectedViewType
        {
            get => _selectedViewType;
            set
            {
                _selectedViewType = value;
                OnPropertyChanged(nameof(SelectedViewType));
                ApplyEmbeddedOrientation();
            }
        }

        // Propriétés et événements IRefreshable définis dans la section dédiée plus bas

        #endregion

        #region Constructor

        public ReconciliationPage()
        {
            InitializeComponent();
            DataContext = this;
            InitializeData();
            Loaded += ReconciliationPage_Loaded;
            Unloaded += ReconciliationPage_Unloaded;
        }

        public ReconciliationPage(ReconciliationService reconciliationService, OfflineFirstService offlineFirstService) : this()
        {
            _reconciliationService = reconciliationService;
            _offlineFirstService = offlineFirstService;
            
            // Synchroniser avec la country courante du service
            _currentCountryId = _offlineFirstService?.CurrentCountry?.CNT_Id;
            LoadAvailableCountries();
            // Le chargement initial est confirmé dans l'événement Loaded pour garantir que le pays a été initialisé
        }

        // Constructeur de compatibilité (obsolète)
        public ReconciliationPage(ReconciliationService reconciliationService, string countryId) : this()
        {
            _reconciliationService = reconciliationService;
            _offlineFirstService = null;
            _currentCountryId = countryId;
            // Le chargement initial est confirmé dans l'événement Loaded
        }

        #endregion

        #region UI Busy / Wait Cursor

        private sealed class DisposableAction : IDisposable
        {
            private readonly Action _onDispose;
            public DisposableAction(Action onDispose) { _onDispose = onDispose; }
            public void Dispose() { try { _onDispose?.Invoke(); } catch { } }
        }

        private IDisposable BeginWaitCursor()
        {
            try
            {
                // Ensure on UI thread
                if (!Dispatcher.CheckAccess())
                {
                    Dispatcher.Invoke(() => Mouse.OverrideCursor = Cursors.Wait);
                }
                else
                {
                    Mouse.OverrideCursor = Cursors.Wait;
                }
            }
            catch { }

            return new DisposableAction(() =>
            {
                try
                {
                    if (!Dispatcher.CheckAccess())
                        Dispatcher.Invoke(() => Mouse.OverrideCursor = null);
                    else
                        Mouse.OverrideCursor = null;
                }
                catch { }
            });
        }

        #endregion

        #region Initialization

        /// <summary>
        /// Initialise les données par défaut
        /// </summary>
        private void InitializeData()
        {
            SavedFilters = new ObservableCollection<UserFilter>();
            SavedViews = new ObservableCollection<UserViewPreset>();
            ViewTypes = new ObservableCollection<string>(new[] { "Embedded (Vertical)", "Popup" });
            SelectedViewType = ViewTypes.FirstOrDefault();
            Accounts = new ObservableCollection<string>();
            Statuses = new ObservableCollection<string>(new[] { "All", "Live", "Archived" });
            SelectedStatus = Statuses.FirstOrDefault();
            
            
        }

        

        #endregion

        #region Data Loading

        /// <summary>
        /// Charge la liste des pays disponibles depuis OfflineFirstService
        /// </summary>
        private async void LoadAvailableCountries()
        {
            try
            {
                using var _ = BeginWaitCursor();
                if (_offlineFirstService != null)
                {
                    var countries = await _offlineFirstService.GetCountries();
                    AvailableCountries = countries.ToList();
                    
                    // Sélectionner automatiquement la country courante
                    var currentCountry = _offlineFirstService.CurrentCountry;
                }
                else
                {
                    // Fallback: liste vide si pas de service
                    AvailableCountries = new List<Country>();
                }
            }
            catch (Exception ex)
            {
                ShowError($"Erreur lors du chargement des pays: {ex.Message}");
            }
        }

        /// <summary>
        /// Charge les données de réconciliation
        /// </summary>
        public async Task LoadDataAsync()
        {
            try
            {
                using var _ = BeginWaitCursor();
                IsLoading = true;
                // Toujours charger/rafraîchir les filtres/vues sauf si la recharge est déclenchée par une sélection
                if (!_skipReloadSavedLists)
                {
                    await LoadSavedFiltersAsync();
                    await LoadSavedViewsAsync();
                }
                // Mettre à jour les filtres de haut de page à partir du référentiel pays
                UpdateTopFiltersFromData();
            }
            catch (Exception ex)
            {
                ShowError($"Erreur lors du chargement des données: {ex.Message}");
            }
            finally
            {
                IsLoading = false;
            }
        }

        /// <summary>
        /// Charge les filtres sauvegardés
        /// </summary>
        private async Task LoadSavedFiltersAsync()
        {
            try
            {
                if (_offlineFirstService == null)
                    return;

                // Récupérer les filtres depuis les référentiels
                // Optionnel: filtrer par utilisateur courant (si nécessaire, remplacer par une propriété UserName)
                var filters = await _offlineFirstService.GetUserFilters();

                _isLoadingFilters = true;
                try
                {
                    SavedFilters.Clear();
                    foreach (var f in filters.OrderBy(x => x.UFI_Name))
                    {
                        SavedFilters.Add(f);
                    }

                    // Ajout d'un filtre par défaut "Tous"
                    if (!SavedFilters.Any(sf => string.IsNullOrWhiteSpace(sf.UFI_SQL)))
                    {
                        SavedFilters.Insert(0, new UserFilter { UFI_Name = "Tous", UFI_SQL = string.Empty });
                    }

                    // Sélection automatique uniquement si rien n'est sélectionné (évite de forcer et de boucler)
                    var combo = FindName("SavedFiltersComboBox") as ComboBox;
                    if (combo != null && SavedFilters.Any() && combo.SelectedIndex < 0)
                    {
                        combo.SelectedIndex = 0; // ignoré via _isLoadingFilters
                    }
                }
                finally
                {
                    _isLoadingFilters = false;
                }
            }
            catch (Exception ex)
            {
                ShowError($"Erreur lors du chargement des filtres: {ex.Message}");
            }
        }

        #endregion

        #region IRefreshable Implementation

        private bool _canRefresh = true;
        
        public bool CanRefresh => _canRefresh && _offlineFirstService.CurrentCountryId != null;

        public event EventHandler RefreshStarted;
        public event EventHandler RefreshCompleted;

        public void Refresh()
        {
            _ = RefreshAsync();
        }

        public async Task RefreshAsync()
        {
            if (!CanRefresh) return;

            try
            {
                using var _ = BeginWaitCursor();
                RefreshStarted?.Invoke(this, EventArgs.Empty);
                // Tenter une synchro réseau avant rechargement des données (si pas de verrou)
                await TrySynchronizeIfSafeAsync();
                await LoadDataAsync();
            }
            finally
            {
                RefreshCompleted?.Invoke(this, EventArgs.Empty);
            }
        }

        #endregion

        #region Filtering and Searching
        // Legacy page-level filtering removed. Each ReconciliationView handles its own data/filtering.

        private void UpdateTopFiltersFromData()
        {
            var previous = SelectedAccount; // conserver la sélection affichée

            // Référentiel pays (source de vérité pour Pivot/Receivable)
            var country = _offlineFirstService?.CurrentCountry;
            var pivotId = country?.CNT_AmbrePivot;
            var recvId = country?.CNT_AmbreReceivable;

            // Reconstituer la liste d'affichage à partir du référentiel (toujours inclure Pivot/Receivable si définis)
            var displayAccounts = new List<string>();
            if (!string.IsNullOrWhiteSpace(pivotId))
                displayAccounts.Add($"Pivot ({pivotId})");
            if (!string.IsNullOrWhiteSpace(recvId))
                displayAccounts.Add($"Receivable ({recvId})");

            // Appliquer à l'ObservableCollection
            Accounts.Clear();
            foreach (var d in displayAccounts)
                Accounts.Add(d);

            // Préserver la sélection Pivot/Receivable au changement de pays
            if (!string.IsNullOrEmpty(previous))
            {
                if (previous.StartsWith("Pivot", StringComparison.OrdinalIgnoreCase))
                {
                    var pivotDisplay = displayAccounts.FirstOrDefault(x => x.StartsWith("Pivot", StringComparison.OrdinalIgnoreCase));
                    if (pivotDisplay != null) { SelectedAccount = pivotDisplay; return; }
                }
                if (previous.StartsWith("Receivable", StringComparison.OrdinalIgnoreCase))
                {
                    var recvDisplay = displayAccounts.FirstOrDefault(x => x.StartsWith("Receivable", StringComparison.OrdinalIgnoreCase));
                    if (recvDisplay != null) { SelectedAccount = recvDisplay; return; }
                }

                // Sinon, si l'ancienne valeur existe encore, la conserver
                if (displayAccounts.Contains(previous))
                {
                    SelectedAccount = previous;
                    return;
                }
            }

            // Valeur par défaut (obligatoire)
            SelectedAccount = displayAccounts.FirstOrDefault();
            if (string.IsNullOrEmpty(SelectedAccount) && Accounts.Any())
                SelectedAccount = Accounts.FirstOrDefault();
        }

        /// <summary>
        /// Modèle simple pour "Saved Views" (nom + WHERE optionnel)
        /// </summary>
        public class UserViewPreset
        {
            public string Name { get; set; }
            public string WhereClause { get; set; }
        }

        /// <summary>
        /// Chargement des Saved Views depuis T_Ref_User_Fields_Preference
        /// </summary>
        private async Task LoadSavedViewsAsync()
        {
            try
            {
                _isLoadingViews = true;
                try
                {
                    var previousName = SelectedSavedView?.Name;

                    SavedViews.Clear();

                    if (_reconciliationService == null)
                        return;

                    var prefs = await _reconciliationService.GetUserFieldsPreferencesAsync();
                    foreach (var p in prefs.OrderBy(x => x.UPF_Name))
                    {
                        SavedViews.Add(new UserViewPreset
                        {
                            Name = p.UPF_Name,
                            WhereClause = p.UPF_SQL
                        });
                    }

                    // Insert a clear option at top
                    if (!SavedViews.Any() || SavedViews.First().Name != "None")
                    {
                        SavedViews.Insert(0, new UserViewPreset { Name = "None", WhereClause = null });
                    }

                    // Restore previous selection if possible
                    if (!string.IsNullOrWhiteSpace(previousName))
                    {
                        SelectedSavedView = SavedViews.FirstOrDefault(v => v.Name == previousName) ?? SavedViews.FirstOrDefault();
                    }
                    else
                    {
                        SelectedSavedView = SavedViews.FirstOrDefault();
                    }
                }
                finally
                {
                    _isLoadingViews = false;
                }
            }
            catch { }
        }

        /// <summary>
        /// Définit le filtre sauvegardé sélectionné pour la prochaine vue uniquement
        /// (ne recharge pas les données sur la page)
        /// </summary>
        private void ApplySavedFilter(UserFilter filter)
        {
            try
            {
                _currentFilter = filter?.UFI_SQL;
                _currentFilterName = filter?.UFI_Name;
            }
            catch (Exception ex)
            {
                ShowError($"Erreur lors de la sélection du filtre: {ex.Message}");
            }
        }

        #endregion

        #region Event Handlers

        private async void ReconciliationPage_Loaded(object sender, RoutedEventArgs e)
        {
            try
            {
                using var _ = BeginWaitCursor();
                // S'assurer que le pays courant est bien défini côté service
                if (_offlineFirstService != null)
                {
                    var svcCountryId = _offlineFirstService.CurrentCountryId ?? _offlineFirstService.CurrentCountry?.CNT_Id;
                    if (!string.IsNullOrEmpty(svcCountryId))
                    {
                        _currentCountryId = svcCountryId;
                    }
                }
                // Démarrer le polling d'état du verrou
                StartLockPolling();
                // Synchroniser au chargement si aucun verrou global actif
                await TrySynchronizeIfSafeAsync();

                await LoadDataAsync();

                // Ne pas ouvrir de vue par défaut. Laisser l'utilisateur en sélectionner/ajouter une.
            }
            catch (Exception ex)
            {
                ShowError($"Erreur lors du chargement initial: {ex.Message}");
            }
        }

        private void ReconciliationPage_Unloaded(object sender, RoutedEventArgs e)
        {
            try { StopLockPolling(); } catch { }
        }

        /// <summary>
        /// Gestion du changement de pays
        /// </summary>
        private async void OnCountryChanged()
        {
            _currentCountryId = _offlineFirstService.CurrentCountryId;
            await LoadDataAsync();

            // Notifier les vues intégrées pour synchroniser l'entête Pivot/Receivable
            try
            {
                var panel = FindName("ViewsPanel") as StackPanel;
                if (panel != null)
                {
                    foreach (var child in panel.Children.OfType<ReconciliationView>())
                    {
                        child.SyncCountryFromService();
                    }
                }
            }
            catch { }
        }

        // Legacy DataGrid editing/selection handlers removed with page-level grid.

        /// <summary>
        /// Changement du filtre sauvegardé sélectionné
        /// </summary>
        private void SavedFiltersComboBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (_isLoadingFilters)
                return;

            if (e.AddedItems.Count > 0)
            {
                var selected = e.AddedItems[0];
                if (selected is UserFilter selectedFilter)
                {
                    try
                    {
                        // Empty or default => clear (do NOT reload page data)
                        if (string.IsNullOrWhiteSpace(selectedFilter?.UFI_SQL))
                        {
                            _currentFilter = null;
                            _currentFilterName = null;
                        }
                        else
                        {
                            // Apply for next added view only (do NOT reload page data)
                            _currentFilter = selectedFilter.UFI_SQL;
                            _currentFilterName = selectedFilter.UFI_Name;
                        }
                    }
                    catch (Exception ex)
                    {
                        ShowError($"Erreur lors de la réinitialisation du filtre: {ex.Message}");
                    }
                }
            }
        }

        /// <summary>
        /// Changement de "Saved View" sélectionnée
        /// </summary>
        private async void SavedViewsComboBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (_isLoadingViews)
                return;
            if (e.AddedItems.Count > 0)
            {
                var selected = e.AddedItems[0];
                try
                {
                    if (selected is UserViewPreset preset)
                    {
                        if (string.IsNullOrWhiteSpace(preset.WhereClause))
                        {
                            // None selected -> clear view filter (do NOT reload page data)
                            _currentFilter = null;
                            _currentFilterName = null;
                        }
                        else
                        {
                            // Set for next added view only (do NOT reload page data)
                            _currentFilter = preset.WhereClause;
                            _currentFilterName = preset.Name;
                        }
                    }
                    // Intentionally no LoadDataAsync here to avoid impacting account/top filters
                }
                catch (Exception ex)
                {
                    ShowError($"Erreur lors de la sélection de la vue: {ex.Message}");
                }
            }
        }

        

        /// <summary>
        /// Ajouter une nouvelle vue
        /// </summary>
        private void AddViewButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                using var _ = BeginWaitCursor();
                // Ouvrir la fenêtre de création de vue personnalisée
                var addViewWindow = new AddViewWindow();
                addViewWindow.Owner = Window.GetWindow(this);
                
                if (addViewWindow.ShowDialog() == true)
                {
                    // Traiter la nouvelle vue
                    Refresh();
                }
            }
            catch (Exception ex)
            {
                ShowError($"Erreur lors de l'ajout de vue: {ex.Message}");
            }
        }

        /// <summary>
        /// Exporter les données
        /// </summary>
        private void ExportButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var saveFileDialog = new Microsoft.Win32.SaveFileDialog
                {
                    Title = "Exporter les données de réconciliation",
                    Filter = "Fichiers Excel (*.xlsx)|*.xlsx|Fichiers CSV (*.csv)|*.csv",
                    DefaultExt = "xlsx"
                };

                if (saveFileDialog.ShowDialog() == true)
                {
                    ExportData(saveFileDialog.FileName);
                }
            }
            catch (Exception ex)
            {
                ShowError($"Erreur lors de l'export: {ex.Message}");
            }
        }

        /// <summary>
        /// Appliquer les règles automatiques
        /// </summary>
        private async void ApplyRulesButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if (string.IsNullOrEmpty(_currentCountryId))
                {
                    ShowWarning("Veuillez sélectionner un pays avant d'appliquer les règles.");
                    return;
                }

                var result = MessageBox.Show(
                    "Êtes-vous sûr de vouloir appliquer les règles automatiques ?\nCela peut modifier les Actions et KPI existants.",
                    "Confirmation",
                    MessageBoxButton.YesNo,
                    MessageBoxImage.Question);

                if (result == MessageBoxResult.Yes)
                {
                    IsLoading = true;
                    await _reconciliationService.ApplyAutomaticRulesAsync(_currentCountryId);
                    await LoadDataAsync();
                    
                    ShowInfo("Règles automatiques appliquées avec succès.");
                }
            }
            catch (Exception ex)
            {
                ShowError($"Erreur lors de l'application des règles: {ex.Message}");
            }
            finally
            {
                IsLoading = false;
            }
        }

        /// <summary>
        /// Effectuer un rapprochement automatique
        /// </summary>
        private async void AutoMatchButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if (string.IsNullOrEmpty(_currentCountryId))
                {
                    ShowWarning("Veuillez sélectionner un pays avant le rapprochement automatique.");
                    return;
                }

                IsLoading = true;
                var matchCount = await _reconciliationService.PerformAutomaticMatchingAsync(_currentCountryId);
                await LoadDataAsync();
                
                ShowInfo($"Rapprochement automatique terminé.\n{matchCount} éléments ont été rapprochés.");
            }
            catch (Exception ex)
            {
                ShowError($"Erreur lors du rapprochement automatique: {ex.Message}");
            }
            finally
            {
                IsLoading = false;
            }
        }

        #endregion

        #region Helper Methods

        private void StartLockPolling()
        {
            try
            {
                if (_onLockStateChanged != null || _onSyncSuggested != null) return;

                // Initialize previous network state
                try { _prevNetworkAvailable = _offlineFirstService?.IsNetworkSyncAvailable == true; } catch { _prevNetworkAvailable = false; }

                var monitor = RecoTool.Services.SyncMonitorService.Instance;
                monitor.Initialize(() => _offlineFirstService);

                _onLockStateChanged = (isActive) =>
                {
                    // Ensure UI thread
                    try
                    {
                        Dispatcher?.InvokeAsync(() => { IsGlobalLockActive = isActive; });
                    }
                    catch { IsGlobalLockActive = isActive; }
                };

                _onSyncSuggested = async (reason) =>
                {
                    // Gate and pre-checks on background thread
                    if (_isAutoSyncRunning) return;
                    if (DateTime.UtcNow - _lastAutoSyncUtc <= AutoSyncCooldown) return;
                    if (_offlineFirstService == null || string.IsNullOrEmpty(_offlineFirstService.CurrentCountryId)) return;
                    if (IsGlobalLockActive) return;

                    _isAutoSyncRunning = true;
                    try
                    {
                        // Ensure UI-affecting ops are dispatched
                        await Dispatcher.InvokeAsync(async () =>
                        {
                            if (string.Equals(reason, "LockReleased", StringComparison.OrdinalIgnoreCase))
                            {
                                // Après fin de verrou global (publication réseau d'un import), effectuer une réconciliation post-publication
                                await ReconcileAfterImportAsync();
                                _lastAutoSyncUtc = DateTime.UtcNow;
                                try { ShowInfo("Réconciliation terminée après publication (fin du verrou global)"); } catch { }
                            }
                            else
                            {
                                // Comportement existant pour les autres raisons (ex: réseau rétabli)
                                await TrySynchronizeIfSafeAsync();
                                await LoadDataAsync();
                                _lastAutoSyncUtc = DateTime.UtcNow;
                                try { ShowInfo("Synchronisation terminée (réseau rétabli)"); } catch { }
                            }
                        });
                    }
                    catch
                    {
                        try
                        {
                            await Dispatcher.InvokeAsync(() =>
                                ShowWarning(string.Equals(reason, "LockReleased", StringComparison.OrdinalIgnoreCase)
                                    ? "Réconciliation échouée après libération du verrou"
                                    : "Synchronisation échouée au rétablissement du réseau"));
                        }
                        catch { }
                    }
                    finally { _isAutoSyncRunning = false; }
                };

                monitor.LockStateChanged += _onLockStateChanged;
                monitor.SyncSuggested += _onSyncSuggested;
                monitor.Start();
            }
            catch { }
        }

        private void StopLockPolling()
        {
            try
            {
                var monitor = RecoTool.Services.SyncMonitorService.Instance;
                if (_onLockStateChanged != null)
                {
                    monitor.LockStateChanged -= _onLockStateChanged;
                    _onLockStateChanged = null;
                }
                if (_onSyncSuggested != null)
                {
                    monitor.SyncSuggested -= _onSyncSuggested;
                    _onSyncSuggested = null;
                }
                // Do not stop the shared monitor here to avoid impacting other pages.
            }
            catch { }
        }

        /// <summary>
        /// Tente de synchroniser avec le serveur si disponible et si aucun verrou global n'est actif.
        /// </summary>
        private async Task TrySynchronizeIfSafeAsync()
        {
            try
            {
                if (_offlineFirstService == null) return;
                if (string.IsNullOrEmpty(_offlineFirstService.CurrentCountryId)) return;

                // Éviter la synchro si un import/d'autres opérations tiennent le verrou global
                var locked = await _offlineFirstService.IsGlobalLockActiveAsync();
                IsGlobalLockActive = locked;
                if (locked) return;

                await _offlineFirstService.SynchronizeData();
            }
            catch
            {
                // Ignorer les erreurs de synchro silencieusement ici; l'utilisateur peut relancer via Refresh
            }
        }

        /// <summary>
        /// Effectue la réconciliation post-publication après un import Ambre: pousse les pending locaux puis recharge la base locale depuis le réseau.
        /// </summary>
        private async Task ReconcileAfterImportAsync()
        {
            if (_offlineFirstService == null || string.IsNullOrEmpty(_offlineFirstService.CurrentCountryId)) return;
            try
            {
                // Empêcher l'interaction pendant l'opération
                IsLoading = true;
                await _offlineFirstService.PostPublishReconcileAsync(_offlineFirstService.CurrentCountryId);
                await LoadDataAsync();
            }
            finally
            {
                IsLoading = false;
            }
        }

        /// <summary>
        /// Handler du bouton "Sync Now" (synchro manuelle si possible)
        /// </summary>
        private async void SyncNowButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if (_offlineFirstService == null || string.IsNullOrEmpty(_offlineFirstService.CurrentCountryId))
                {
                    ShowWarning("Aucun pays n'est sélectionné pour la synchronisation.");
                    return;
                }

                if (IsGlobalLockActive)
                {
                    ShowInfo("Synchronisation ignorée: un verrou d'import global est actif.");
                    return;
                }

                IsLoading = true;
                await TrySynchronizeIfSafeAsync();
                await LoadDataAsync();
                ShowInfo("Synchronisation terminée.");
            }
            catch (Exception ex)
            {
                ShowError($"Erreur lors de la synchronisation: {ex.Message}");
            }
            finally
            {
                IsLoading = false;
            }
        }

        /// <summary>
        /// Recharge uniquement la ComboBox des filtres sauvegardés (sans recharger les données)
        /// </summary>
        public async Task ReloadSavedFiltersOnly()
        {
            await LoadSavedFiltersAsync();
        }

        /// <summary>
        /// Met à jour l'état des boutons d'action selon la sélection
        /// </summary>
        private void UpdateActionButtons(int selectedCount)
        {
            // TODO: Mettre à jour l'état des boutons selon la sélection
        }

        /// <summary>
        /// Exporte les données vers un fichier
        /// </summary>
        private void ExportData(string fileName)
        {
            try
            {
                // TODO: Implémenter l'export Excel/CSV
                ShowInfo($"Export terminé vers {fileName}");
            }
            catch (Exception ex)
            {
                ShowError($"Erreur lors de l'export: {ex.Message}");
            }
        }

        /// <summary>
        /// Affiche un message d'erreur
        /// </summary>
        private void ShowError(string message)
        {
            MessageBox.Show(message, "Erreur", MessageBoxButton.OK, MessageBoxImage.Error);
        }

        /// <summary>
        /// Affiche un message d'avertissement
        /// </summary>
        private void ShowWarning(string message)
        {
            MessageBox.Show(message, "Avertissement", MessageBoxButton.OK, MessageBoxImage.Warning);
        }

        /// <summary>
        /// Affiche un message d'information
        /// </summary>
        private void ShowInfo(string message)
        {
            MessageBox.Show(message, "Information", MessageBoxButton.OK, MessageBoxImage.Information);
        }

        #endregion

        #region INotifyPropertyChanged Implementation

        public event PropertyChangedEventHandler PropertyChanged;

        protected virtual void OnPropertyChanged(string propertyName)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }

        #endregion

        private async void AddView_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                // Le type de vue est déterminé par SelectedViewType
                var popup = string.Equals(SelectedViewType, "Popup", StringComparison.OrdinalIgnoreCase);
                await AddReconciliationView(popup);
            }
            catch (Exception ex)
            {
                ShowError($"Erreur lors de l'ajout de vue: {ex.Message}");
            }
        }

        private async Task AddReconciliationView(bool asPopup = false)
        {
            if (_reconciliationService == null)
            {
                ShowWarning("Le service de réconciliation n'est pas disponible.");
                return;
            }

            // Créer la vue et l'attacher aux services existants
            var view = new ReconciliationView(_reconciliationService, _offlineFirstService)
            {
                Margin = new Thickness(0)
            };
            // Ne pas créer de conteneur si on ouvre en popup (évite l'erreur de parent visuel)

            if (asPopup)
            {
                var wnd = new Window
                {
                    Title = "Reconciliation View",
                    Content = view,
                    Owner = Window.GetWindow(this),
                    Width = 1100,
                    Height = 750,
                    WindowStartupLocation = WindowStartupLocation.CenterOwner
                };
                // Fermer la fenêtre quand la vue demande la fermeture
                view.CloseRequested += (s, e) => { try { wnd.Close(); } catch { } };
                // Aligner le pays avant toute action
                try { view.SyncCountryFromService(); } catch { }
                view.UpdateExternalFilters(SelectedAccount, SelectedStatus);
                if (!string.IsNullOrWhiteSpace(_currentFilter))
                {
                    view.ApplySavedFilterSql(_currentFilter);
                    if (!string.IsNullOrWhiteSpace(_currentFilterName))
                        view.SetViewTitle(_currentFilterName);
                    // Apply saved layout for the selected view asynchronously to avoid blocking UI
                    if (!string.IsNullOrWhiteSpace(_currentFilterName))
                    {
                        _ = System.Threading.Tasks.Task.Run(async () =>
                        {
                            try
                            {
                                var pref = await _reconciliationService.GetUserFieldsPreferenceByNameAsync(_currentFilterName);
                                if (!string.IsNullOrWhiteSpace(pref?.UPF_ColumnWidths))
                                {
                                    await view.Dispatcher.InvokeAsync(() =>
                                    {
                                        try { view.ApplyLayoutJson(pref.UPF_ColumnWidths); } catch { }
                                    });
                                }
                            }
                            catch { }
                        });
                    }
                }
                // Précharger les données sans bloquer l'UI puis injecter dans la vue
                _ = System.Threading.Tasks.Task.Run(async () =>
                {
                    try
                    {
                        var countryId = _offlineFirstService?.CurrentCountryId ?? _offlineFirstService?.CurrentCountry?.CNT_Id;
                        var backendSql = _currentFilter;
                        var list = await _reconciliationService.GetReconciliationViewAsync(countryId, backendSql).ConfigureAwait(false);
                        await view.Dispatcher.InvokeAsync(() =>
                        {
                            try
                            {
                                view.InitializeWithPreloadedData(list, backendSql);
                                view.Refresh();
                            }
                            catch { }
                        });
                    }
                    catch { }
                });
                wnd.Show();
                return;
            }

            var panel = FindName("ViewsPanel") as StackPanel;
            var empty = FindName("EmptyStatePanel") as FrameworkElement;
            var fab = FindName("FloatingAddButton") as FrameworkElement;
            if (panel == null) return;

            // Toujours étirer la vue aux dimensions du parent
            view.HorizontalAlignment = HorizontalAlignment.Stretch;
            view.VerticalAlignment = VerticalAlignment.Stretch;
            view.Width = double.NaN; // Auto width to stretch
            // Créer un conteneur redimensionnable pour la vue (hauteur ajustable depuis le bas)
            var container = CreateResizableContainer(view);
            panel.Children.Add(container);

            // Abonner la fermeture pour retirer la vue intégrée
            view.CloseRequested += (s, e) =>
            {
                try
                {
                    var p = FindName("ViewsPanel") as StackPanel;
                    if (p != null && p.Children.Contains(container))
                    {
                        p.Children.Remove(container);
                        // Basculer visibilité si aucune vue
                        if (p.Children.Count == 0)
                        {
                            p.Visibility = Visibility.Collapsed;
                            var emptyState = FindName("EmptyStatePanel") as UIElement;
                            var floatBtn = FindName("FloatingAddButton") as UIElement;
                            if (emptyState != null) emptyState.Visibility = Visibility.Visible;
                            if (floatBtn != null) floatBtn.Visibility = Visibility.Collapsed;
                        }
                    }
                }
                catch { }
            };

            // Aligner le pays avant toute action
            try { view.SyncCountryFromService(); } catch { }
            // Synchroniser l'affichage des filtres (chargement déclenché après précharge)
            view.UpdateExternalFilters(SelectedAccount, SelectedStatus);
            if (!string.IsNullOrWhiteSpace(_currentFilter))
            {
                view.ApplySavedFilterSql(_currentFilter);
                if (!string.IsNullOrWhiteSpace(_currentFilterName))
                    view.SetViewTitle(_currentFilterName);
                // Apply saved layout for the selected view asynchronously to avoid blocking UI
                if (!string.IsNullOrWhiteSpace(_currentFilterName))
                {
                    _ = System.Threading.Tasks.Task.Run(async () =>
                    {
                        try
                        {
                            var pref = await _reconciliationService.GetUserFieldsPreferenceByNameAsync(_currentFilterName);
                            if (!string.IsNullOrWhiteSpace(pref?.UPF_ColumnWidths))
                            {
                                await view.Dispatcher.InvokeAsync(() =>
                                {
                                    try { view.ApplyLayoutJson(pref.UPF_ColumnWidths); } catch { }
                                });
                            }
                        }
                        catch { }
                    });
                }
            }
            // Précharger les données sans bloquer l'UI puis injecter dans la vue
            _ = System.Threading.Tasks.Task.Run(async () =>
            {
                try
                {
                    var countryId = _offlineFirstService?.CurrentCountryId ?? _offlineFirstService?.CurrentCountry?.CNT_Id;
                    var backendSql = _currentFilter;
                    var list = await _reconciliationService.GetReconciliationViewAsync(countryId, backendSql).ConfigureAwait(false);
                    await view.Dispatcher.InvokeAsync(() =>
                    {
                        try
                        {
                            view.InitializeWithPreloadedData(list, backendSql);
                            view.Refresh();
                        }
                        catch { }
                    });
                }
                catch { }
            });

            // Mettre à jour la visibilité
            panel.Visibility = Visibility.Visible;
            if (empty != null) empty.Visibility = Visibility.Collapsed;
            if (fab != null) fab.Visibility = Visibility.Visible;
        }

        /// <summary>
        /// Crée un conteneur redimensionnable verticalement pour une ReconciliationView, avec poignée de redimensionnement en bas
        /// et icônes agrandir/rétrécir.
        /// </summary>
        private Grid CreateResizableContainer(ReconciliationView view)
        {
            // Grille à 2 lignes: contenu + barre de redimensionnement
            var grid = new Grid
            {
                Margin = new Thickness(15),
                HorizontalAlignment = HorizontalAlignment.Stretch,
                VerticalAlignment = VerticalAlignment.Top,
                Height = 400, // hauteur initiale
                MinHeight = 200,
                MaxHeight = 1200
            };
            grid.RowDefinitions.Add(new RowDefinition { Height = new GridLength(1, GridUnitType.Star) });
            grid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto });

            // Contenu
            Grid.SetRow(view, 0);
            grid.Children.Add(view);

            // Barre de redimensionnement
            var bar = new Border
            {
                Background = new System.Windows.Media.SolidColorBrush(System.Windows.Media.Color.FromRgb(230, 230, 230)),
                BorderBrush = new System.Windows.Media.SolidColorBrush(System.Windows.Media.Color.FromRgb(200, 200, 200)),
                BorderThickness = new Thickness(1, 0, 1, 1),
                Height = 18,
                Padding = new Thickness(6, 0, 6, 0)
            };
            Grid.SetRow(bar, 1);

            var barGrid = new Grid();
            barGrid.ColumnDefinitions.Add(new ColumnDefinition { Width = new GridLength(1, GridUnitType.Star) });
            barGrid.ColumnDefinitions.Add(new ColumnDefinition { Width = GridLength.Auto });
            barGrid.ColumnDefinitions.Add(new ColumnDefinition { Width = GridLength.Auto });

            // Visuel de poignée (3 petites barres)
            var handle = new StackPanel { Orientation = Orientation.Horizontal, HorizontalAlignment = HorizontalAlignment.Left };
            for (int i = 0; i < 3; i++)
            {
                handle.Children.Add(new Border { Width = 18, Height = 3, Margin = new Thickness(2, 7, 2, 6), Background = new System.Windows.Media.SolidColorBrush(System.Windows.Media.Color.FromRgb(160, 160, 160)) });
            }
            Grid.SetColumn(handle, 0);
            barGrid.Children.Add(handle);

            // Poignée de redimensionnement (Thumb) transparente superposée pour capter le drag
            var thumb = new Thumb
            {
                Height = 16,
                Cursor = Cursors.SizeNS,
                HorizontalAlignment = HorizontalAlignment.Stretch,
                VerticalAlignment = VerticalAlignment.Center,
                Background = System.Windows.Media.Brushes.Transparent
            };
            thumb.DragDelta += (s, e) =>
            {
                try
                {
                    // Vers le bas => augmenter la hauteur (VerticalChange > 0 en descendant)
                    var baseH = double.IsNaN(grid.Height) ? grid.ActualHeight : grid.Height;
                    var newH = baseH + e.VerticalChange;
                    newH = Math.Max(grid.MinHeight, Math.Min(grid.MaxHeight, newH));
                    grid.Height = newH;
                    // Ajuster le MaxHeight de la DataGrid si nécessaire
                    AdjustDataGridMaxHeight(grid, view);
                    e.Handled = true;
                }
                catch { }
            };
            Grid.SetColumn(thumb, 0);
            barGrid.Children.Add(thumb);

            // Bouton réduire (moins)
            var btnShrink = new Button
            {
                Content = new TextBlock { Text = "-", FontSize = 16, VerticalAlignment = VerticalAlignment.Center },
                Width = 26,
                Height = 16,
                Margin = new Thickness(6, 0, 0, 0),
                Padding = new Thickness(0),
                ToolTip = "Réduire la hauteur"
            };
            btnShrink.Click += (s, e) =>
            {
                try { grid.Height = Math.Max(200, grid.Height - 150); } catch { }
            };
            Grid.SetColumn(btnShrink, 1);
            barGrid.Children.Add(btnShrink);

            // Bouton agrandir (plus)
            var btnGrow = new Button
            {
                Content = new TextBlock { Text = "+", FontSize = 16, VerticalAlignment = VerticalAlignment.Center },
                Width = 26,
                Height = 16,
                Margin = new Thickness(6, 0, 0, 0),
                Padding = new Thickness(0),
                ToolTip = "Agrandir la hauteur"
            };
            btnGrow.Click += (s, e) =>
            {
                try
                {
                    grid.Height = Math.Min(1200, grid.Height + 150);
                    // Ajuster le MaxHeight de la DataGrid si nécessaire
                    AdjustDataGridMaxHeight(grid, view);
                }
                catch { }
            };
            Grid.SetColumn(btnGrow, 2);
            barGrid.Children.Add(btnGrow);

            bar.Child = barGrid;
            grid.Children.Add(bar);

            return grid;
        }

        /// <summary>
        /// Augmente le MaxHeight de la DataGrid (ResultsDataGrid) si le conteneur devient plus grand que sa limite actuelle.
        ///</summary>
        private void AdjustDataGridMaxHeight(Grid container, ReconciliationView view)
        {
            try
            {
                var dg = view.FindName("ResultsDataGrid") as DataGrid;
                if (dg == null) return;
                var containerH = double.IsNaN(container.Height) ? container.ActualHeight : container.Height;
                // marge approximative pour header/paddings/toolbar/etc.
                double overhead = 140; // ajuste si besoin
                double target = Math.Max(200, containerH - overhead);
                if (target > dg.MaxHeight)
                {
                    dg.MaxHeight = target;
                }
            }
            catch { }
        }

        private void ApplyEmbeddedOrientation()
        {
            try
            {
                var panel = FindName("ViewsPanel") as StackPanel;
                if (panel == null) return;
                // Forcer l'empilement vertical uniquement
                panel.Orientation = Orientation.Vertical;
            }
            catch { }
        }
    }

    #region Helper Classes

    /// <summary>
    /// Fenêtre pour ajouter une nouvelle vue personnalisée
    /// </summary>
    public partial class AddViewWindow : Window
    {
        public AddViewWindow()
        {
            // TODO: Implémenter la fenêtre de création de vue
            InitializeComponent();
            Title = "Ajouter une vue";
            Width = 500;
            Height = 400;
            WindowStartupLocation = WindowStartupLocation.CenterOwner;
        }

        private void InitializeComponent()
        {
            // Interface basique temporaire
            var grid = new Grid();
            var textBlock = new TextBlock
            {
                Text = "Fonctionnalité à implémenter",
                HorizontalAlignment = HorizontalAlignment.Center,
                VerticalAlignment = VerticalAlignment.Center
            };
            grid.Children.Add(textBlock);
            Content = grid;
        }
    }

    #endregion
}
