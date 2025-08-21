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
        private ObservableCollection<ReconciliationViewData> _reconciliationData;
        private ObservableCollection<UserFilter> _savedFilters;
        private CollectionViewSource _viewSource;
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

        public ObservableCollection<ReconciliationViewData> ReconciliationData
        {
            get => _reconciliationData;
            set
            {
                _reconciliationData = value;
                OnPropertyChanged(nameof(ReconciliationData));
            }
        }

        

        /// <summary>
        /// Résout l'ID de compte réel à partir d'un libellé d'affichage (ex: "Pivot (ID)")
        /// </summary>
        private string ResolveSelectedAccountIdForFilter(string display)
        {
            if (string.IsNullOrWhiteSpace(display)) return display;

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
                _viewSource?.View?.Refresh();
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
                _viewSource?.View?.Refresh();
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

        #region Initialization

        /// <summary>
        /// Initialise les données par défaut
        /// </summary>
        private void InitializeData()
        {
            ReconciliationData = new ObservableCollection<ReconciliationViewData>();
            SavedFilters = new ObservableCollection<UserFilter>();
            SavedViews = new ObservableCollection<UserViewPreset>();
            ViewTypes = new ObservableCollection<string>(new[] { "Embedded (Vertical)", "Popup" });
            SelectedViewType = ViewTypes.FirstOrDefault();
            Accounts = new ObservableCollection<string>();
            Statuses = new ObservableCollection<string>(new[] { "All", "Active", "Deleted" });
            SelectedStatus = Statuses.FirstOrDefault();
            
            
            // Configuration de la vue avec tri et filtrage
            _viewSource = new CollectionViewSource { Source = ReconciliationData };
            _viewSource.View.Filter = FilterReconciliationItems;
            
            // Configuration du DataGrid si présent
            SetupDataGrid();
        }

        /// <summary>
        /// Configure le DataGrid principal
        /// </summary>
        private void SetupDataGrid()
        {
            try
            {
                var dataGrid = FindName("ReconciliationDataGrid") as DataGrid;
                if (dataGrid != null)
                {
                    dataGrid.ItemsSource = _viewSource.View;
                    dataGrid.AutoGenerateColumns = false;
                    dataGrid.CanUserAddRows = false;
                    dataGrid.CanUserDeleteRows = false;
                    dataGrid.SelectionMode = DataGridSelectionMode.Extended;
                    
                    // Événements
                    dataGrid.SelectionChanged += DataGrid_SelectionChanged;
                    dataGrid.CellEditEnding += DataGrid_CellEditEnding;
                }
            }
            catch (Exception ex)
            {
                ShowError($"Erreur lors de la configuration du DataGrid: {ex.Message}");
            }
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
                IsLoading = true;
                // Toujours charger/rafraîchir les filtres/vues sauf si la recharge est déclenchée par une sélection
                if (!_skipReloadSavedLists)
                {
                    await LoadSavedFiltersAsync();
                    await LoadSavedViewsAsync();
                }

                // Si les données ne peuvent pas être chargées (pas de pays ou pas de service), s'arrêter après les filtres
                if (_reconciliationService == null || string.IsNullOrEmpty(_currentCountryId))
                    return;

                // Chargement des données de réconciliation (dépend du pays)
                var data = await _reconciliationService.GetReconciliationViewAsync(_currentCountryId, _currentFilter);

                ReconciliationData.Clear();
                foreach (var item in data)
                {
                    ReconciliationData.Add(item);
                }

                // Mettre à jour les filtres de haut de page à partir des données
                UpdateTopFiltersFromData();
                
                // Rafraîchir la vue
                _viewSource?.View?.Refresh();
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

        /// <summary>
        /// Filtre les éléments de réconciliation
        /// </summary>
        private bool FilterReconciliationItems(object item)
        {
            if (item is ReconciliationViewData row)
            {
                // Filtre compte (sélection obligatoire, pas de "All")
                if (!string.IsNullOrEmpty(SelectedAccount))
                {
                    var selectedId = ResolveSelectedAccountIdForFilter(SelectedAccount);
                    if (!string.Equals(row.Account_ID, selectedId, StringComparison.OrdinalIgnoreCase))
                        return false;
                }

                // Filtre statut (Active/Deleted) basé sur IsDeleted
                if (!string.IsNullOrEmpty(SelectedStatus) && !string.Equals(SelectedStatus, "All", StringComparison.OrdinalIgnoreCase))
                {
                    if (string.Equals(SelectedStatus, "Active", StringComparison.OrdinalIgnoreCase) && row.IsDeleted)
                        return false;
                    if (string.Equals(SelectedStatus, "Deleted", StringComparison.OrdinalIgnoreCase) && !row.IsDeleted)
                        return false;
                }

                return true;
            }
            return false;
        }

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

        /// <summary>
        /// Gestion de la sélection dans le DataGrid
        /// </summary>
        private void DataGrid_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            try
            {
                var dataGrid = sender as DataGrid;
                var selectedItems = dataGrid?.SelectedItems?.Cast<ReconciliationViewData>().ToList();
                
                // Mettre à jour les boutons/actions selon la sélection
                UpdateActionButtons(selectedItems?.Count ?? 0);
            }
            catch (Exception ex)
            {
                ShowError($"Erreur lors de la gestion de la sélection: {ex.Message}");
            }
        }

        /// <summary>
        /// Gestion de la fin d'édition de cellule
        /// </summary>
        private async void DataGrid_CellEditEnding(object sender, DataGridCellEditEndingEventArgs e)
        {
            try
            {
                if (e.EditAction == DataGridEditAction.Commit && e.Row.Item is ReconciliationViewData item)
                {
                    // Sauvegarder les modifications
                    var reconciliation = new Reconciliation
                    {
                        ID = item.ID, // legacy compatibility
                        DWINGS_GuaranteeID = item.DWINGS_GuaranteeID,
                        DWINGS_InvoiceID = item.DWINGS_InvoiceID,
                        DWINGS_CommissionID = item.DWINGS_CommissionID,
                        Action = item.Action,
                        Comments = item.Comments,
                        KPI = item.KPI,
                        // ... autres propriétés
                    };

                    await _reconciliationService.SaveReconciliationAsync(reconciliation);
                }
            }
            catch (Exception ex)
            {
                ShowError($"Erreur lors de la sauvegarde: {ex.Message}");
            }
        }

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
                            await TrySynchronizeIfSafeAsync();
                            await LoadDataAsync();
                            _lastAutoSyncUtc = DateTime.UtcNow;
                            try { ShowInfo(reason == "LockReleased" ? "Synchronisation terminée (fin du verrou global)" : "Synchronisation terminée (réseau rétabli)"); } catch { }
                        });
                    }
                    catch
                    {
                        try { await Dispatcher.InvokeAsync(() => ShowWarning(reason == "LockReleased" ? "Synchronisation échouée après libération du verrou" : "Synchronisation échouée au rétablissement du réseau")); } catch { }
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
                Margin = new Thickness(15)
            };
            // Abonner la fermeture pour retirer la vue intégrée
            view.CloseRequested += (s, e) =>
            {
                try
                {
                    var panel = FindName("ViewsPanel") as StackPanel;
                    if (panel != null && panel.Children.Contains(view))
                    {
                        panel.Children.Remove(view);
                        // Basculer visibilité si aucune vue
                        if (panel.Children.Count == 0)
                        {
                            panel.Visibility = Visibility.Collapsed;
                            var empty = FindName("EmptyStatePanel") as UIElement;
                            var floatBtn = FindName("FloatingAddButton") as UIElement;
                            if (empty != null) empty.Visibility = Visibility.Visible;
                            if (floatBtn != null) floatBtn.Visibility = Visibility.Collapsed;
                        }
                    }
                }
                catch { }
            };

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
                view.Refresh();
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
            panel.Children.Add(view);

            // Synchroniser l'affichage des filtres et déclencher le chargement
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
            view.Refresh();

            // Mettre à jour la visibilité
            panel.Visibility = Visibility.Visible;
            if (empty != null) empty.Visibility = Visibility.Collapsed;
            if (fab != null) fab.Visibility = Visibility.Visible;
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
