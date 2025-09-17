using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using RecoTool.Domain.Repositories;
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
using System.Threading;

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
        private int _autoSyncRunningFlag; // 0 = idle, 1 = running (atomic)
        private bool _prevNetworkAvailable;
        private DateTime _lastAutoSyncUtc = DateTime.MinValue;
        private static readonly TimeSpan AutoSyncCooldown = TimeSpan.FromMinutes(1);
        private Action<bool> _onLockStateChanged;
        private Action<string> _onSyncSuggested;
        private bool _isLoadingViews; // Guard for SavedViews repopulation
        private bool _skipReloadSavedLists; // Prevent reloading saved filters/views during data refresh
        private CancellationTokenSource _pageCts; // Cancellation for page-level long operations
        private int _syncInFlightFlag; // 0 = idle, 1 = syncing
        private InvoiceFinderWindow _invoiceFinderWindow; // modeless invoice finder

        /// <summary>
        /// Effectue une réconciliation après une publication d'import (fin de verrou global):
        /// synchronise si possible puis recharge les données et KPI.
        /// </summary>
        private async Task ReconcileAfterImportAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                using var _ = BeginWaitCursor();
                IsLoading = true;
                // Sync restricted: do not synchronize here
                await LoadDataAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // cancellation is user-driven; keep silent
            }
            catch (Exception ex)
            {
                ShowWarning($"Post-import reconciliation ignored: {ex.Message}");
            }
            finally
            {
                IsLoading = false;
            }
        }

        private void OpenInvoiceFinder_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if (_invoiceFinderWindow == null || !_invoiceFinderWindow.IsVisible)
                {
                    _invoiceFinderWindow = new InvoiceFinderWindow();
                    try { _invoiceFinderWindow.Owner = Window.GetWindow(this); } catch { }
                    _invoiceFinderWindow.Show();
                }
                else
                {
                    _invoiceFinderWindow.Activate();
                }
            }
            catch (Exception ex)
            {
                ShowError($"Cannot open Invoice Finder: {ex.Message}");
            }
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
            // Safety net: if this control is constructed outside DI, resolve services now
            try
            {
                if (_offlineFirstService == null)
                {
                    _offlineFirstService = RecoTool.App.ServiceProvider?.GetService(typeof(RecoTool.Services.OfflineFirstService)) as RecoTool.Services.OfflineFirstService;
                }
            }
            catch { }
            try
            {
                if (_reconciliationService == null)
                {
                    _reconciliationService = RecoTool.App.ServiceProvider?.GetService(typeof(RecoTool.Services.ReconciliationService)) as RecoTool.Services.ReconciliationService;
                }
            }
            catch { }
            try
            {
                if (_recoRepository == null)
                {
                    _recoRepository = RecoTool.App.ServiceProvider?.GetService(typeof(RecoTool.Domain.Repositories.IReconciliationRepository)) as RecoTool.Domain.Repositories.IReconciliationRepository;
                }
            }
            catch { }
            InitializeData();
            Loaded += ReconciliationPage_Loaded;
            Unloaded += ReconciliationPage_Unloaded;
        }

        public ReconciliationPage(ReconciliationService reconciliationService, OfflineFirstService offlineFirstService, IReconciliationRepository recoRepository) : this()
        {
            _reconciliationService = reconciliationService;
            _offlineFirstService = offlineFirstService;
            _recoRepository = recoRepository;
            
            // Synchroniser avec la country courante du service
            _currentCountryId = _offlineFirstService?.CurrentCountry?.CNT_Id;
            LoadAvailableCountries();
            // Le chargement initial est confirmé dans l'événement Loaded pour garantir que le pays a été initialisé
        }

        // Lock first 4 columns (N, U, M, Account) from being moved
        private void ResultsDataGrid_ColumnReordering(object sender, DataGridColumnReorderingEventArgs e)
        {
            try
            {
                var dg = sender as DataGrid;
                if (dg == null) return;
                int protectedCount = 4;
                int currentIndex = e.Column.DisplayIndex;
                if (currentIndex < protectedCount)
                    e.Cancel = true; // disallow moving protected columns
            }
            catch { }
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
            Statuses = new ObservableCollection<string>(new[] { "Live", "Archived" });
            SelectedStatus = "Live";
            
            
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
                ShowError($"Error loading countries: {ex.Message}");
            }
        }

        /// <summary>
        /// Charge les données de réconciliation
        /// </summary>
        public async Task LoadDataAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                using var _ = BeginWaitCursor();
                IsLoading = true;
                // Toujours charger/rafraîchir les filtres/vues sauf si la recharge est déclenchée par une sélection
                if (!_skipReloadSavedLists)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    await LoadSavedFiltersAsync(cancellationToken).ConfigureAwait(false);
                    cancellationToken.ThrowIfCancellationRequested();
                    await LoadSavedViewsAsync(cancellationToken).ConfigureAwait(false);
                }
                // Mettre à jour les filtres de haut de page à partir du référentiel pays
                cancellationToken.ThrowIfCancellationRequested();
                UpdateTopFiltersFromData();

                // Preload reconciliation view data for current country/filter to warm service cache
                try
                {
                    var countryId = _offlineFirstService?.CurrentCountryId ?? _offlineFirstService?.CurrentCountry?.CNT_Id;
                    if (!string.IsNullOrWhiteSpace(countryId) && _reconciliationService != null)
                    {
                        var backendSql = _currentFilter; // may be null/empty
                        System.Threading.Tasks.Task.Run(async () =>
                        {
                            try { await _reconciliationService.GetReconciliationViewAsync(countryId, backendSql).ConfigureAwait(false); } catch { }
                        }, cancellationToken);
                    }
                }
                catch { }
            }
            catch (Exception ex)
            {
                if (ex is OperationCanceledException)
                    ShowWarning("Load cancelled.");
                else
                    ShowError($"Error loading data: {ex.Message}");
            }
            finally
            {
                IsLoading = false;
            }
        }

        /// <summary>
        /// Charge les filtres sauvegardés
        /// </summary>
        private async Task LoadSavedFiltersAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                if (_offlineFirstService == null)
                    return;

                // Récupérer les filtres depuis les référentiels
                // Optionnel: filtrer par utilisateur courant (si nécessaire, remplacer par une propriété UserName)
                cancellationToken.ThrowIfCancellationRequested();
                var filters = await _offlineFirstService.GetUserFilters();
                // Sanitize filters to remove Account/Status predicates (those are managed by top combos)
                try
                {
                    if (filters != null)
                    {
                        filters = filters
                            .Select(f => new UserFilter
                            {
                                UFI_Name = f.UFI_Name,
                                UFI_SQL = RecoTool.Services.UserFilterService.SanitizeWhereClause(f.UFI_SQL)
                            })
                            .ToList();
                    }
                }
                catch { }
                if (filters == null || filters.Count == 0)
                {
                    try
                    {
                        var refPath = _offlineFirstService?.ReferentialConnectionString;
                        var curUser = _offlineFirstService?.CurrentUser;
                        if (!string.IsNullOrWhiteSpace(refPath))
                        {
                            var ufs = new RecoTool.Services.UserFilterService(refPath, curUser);
                            var names = ufs.ListUserFilterNames();
                            var rebuilt = new List<UserFilter>();
                            foreach (var n in names)
                            {
                                var where = ufs.LoadUserFilterWhere(n);
                                try { where = RecoTool.Services.UserFilterService.SanitizeWhereClause(where); } catch { }
                                rebuilt.Add(new UserFilter { UFI_Name = n, UFI_SQL = where });
                            }
                            filters = rebuilt;
                        }
                    }
                    catch { }
                }

                _isLoadingFilters = true;
                try
                {
                    // Mémoriser la sélection précédente (par nom) si disponible, en respectant le thread UI
                    string previousName = null;
                    try
                    {
                        if (Dispatcher != null && !Dispatcher.CheckAccess())
                        {
                            previousName = await Dispatcher.InvokeAsync(() =>
                            {
                                try
                                {
                                    var comboPrevUi = FindName("SavedFiltersComboBox") as ComboBox;
                                    if (comboPrevUi != null && comboPrevUi.SelectedItem is UserFilter ufPrevUi)
                                        return ufPrevUi.UFI_Name;
                                }
                                catch { }
                                // fallback sur la valeur mémo si contrôle non accessible
                                return _currentFilterName;
                            });
                        }
                        else
                        {
                            var comboPrev = FindName("SavedFiltersComboBox") as ComboBox;
                            if (comboPrev != null && comboPrev.SelectedItem is UserFilter ufPrev)
                                previousName = ufPrev.UFI_Name;
                            else
                                previousName = _currentFilterName;
                        }
                    }
                    catch { }

                    var ordered = filters.OrderBy(x => x.UFI_Name).ToList();
                    // Marshal mutations to UI thread
                    if (Dispatcher != null && !Dispatcher.CheckAccess())
                    {
                        await Dispatcher.InvokeAsync(() =>
                        {
                            SavedFilters.Clear();
                            foreach (var f in ordered)
                                SavedFilters.Add(f);

                            if (!SavedFilters.Any(sf => string.IsNullOrWhiteSpace(sf.UFI_SQL)))
                                SavedFilters.Insert(0, new UserFilter { UFI_Name = "Tous", UFI_SQL = string.Empty });

                            var combo = FindName("SavedFiltersComboBox") as ComboBox;
                            if (combo != null && SavedFilters.Any())
                            {
                                // Restaurer la sélection si possible, sinon garder l'actuelle, sinon 0
                                if (!string.IsNullOrWhiteSpace(previousName))
                                {
                                    var target = SavedFilters.FirstOrDefault(sf => string.Equals(sf.UFI_Name, previousName, StringComparison.OrdinalIgnoreCase));
                                    if (target != null) combo.SelectedItem = target;
                                    else if (combo.SelectedIndex < 0) combo.SelectedIndex = 0;
                                }
                                else if (combo.SelectedIndex < 0)
                                {
                                    combo.SelectedIndex = 0;
                                }
                            }
                        });
                    }
                    else
                    {
                        SavedFilters.Clear();
                        foreach (var f in ordered)
                            SavedFilters.Add(f);

                        if (!SavedFilters.Any(sf => string.IsNullOrWhiteSpace(sf.UFI_SQL)))
                            SavedFilters.Insert(0, new UserFilter { UFI_Name = "Tous", UFI_SQL = string.Empty });

                        var combo = FindName("SavedFiltersComboBox") as ComboBox;
                        if (combo != null && SavedFilters.Any())
                        {
                            if (!string.IsNullOrWhiteSpace(previousName))
                            {
                                var target = SavedFilters.FirstOrDefault(sf => string.Equals(sf.UFI_Name, previousName, StringComparison.OrdinalIgnoreCase));
                                if (target != null) combo.SelectedItem = target;
                                else if (combo.SelectedIndex < 0) combo.SelectedIndex = 0;
                            }
                            else if (combo.SelectedIndex < 0)
                            {
                                combo.SelectedIndex = 0;
                            }
                        }
                    }
                }
                finally
                {
                    _isLoadingFilters = false;
                }
            }
            catch { }
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
                _pageCts?.Dispose();
                _pageCts = new CancellationTokenSource();
                var token = _pageCts.Token;
                // Sync restricted: do not synchronize here
                await LoadDataAsync(token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // silent cancel
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
            // Ensure UI thread for ObservableCollection and UI element access
            if (Dispatcher != null && !Dispatcher.CheckAccess())
            {
                try { Dispatcher.Invoke(UpdateTopFiltersFromData); } catch { }
                return;
            }
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
        private async Task LoadSavedViewsAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                _isLoadingViews = true;
                try
                {
                    var previousName = SelectedSavedView?.Name;

                    if (_reconciliationService == null)
                        return;

                    cancellationToken.ThrowIfCancellationRequested();
                    var refCs = _offlineFirstService?.ReferentialConnectionString ?? RecoTool.Properties.Settings.Default.ReferentialDB;
                    var curUser = _offlineFirstService?.CurrentUser ?? Environment.UserName;
                    var viewSvc = new UserViewPreferenceService(refCs, curUser);
                    var prefs = await viewSvc.GetAllAsync();
                    var ordered = prefs.OrderBy(x => x.UPF_Name).ToList();

                    // Marshal mutations to UI thread
                    if (Dispatcher != null && !Dispatcher.CheckAccess())
                    {
                        await Dispatcher.InvokeAsync(() =>
                        {
                            SavedViews.Clear();
                            foreach (var p in ordered)
                            {
                                // Views are layout-only: ignore UPF_SQL.
                                SavedViews.Add(new UserViewPreset
                                {
                                    Name = p.UPF_Name,
                                    WhereClause = null
                                });
                            }

                            if (!SavedViews.Any() || SavedViews.First().Name != "None")
                                SavedViews.Insert(0, new UserViewPreset { Name = "None", WhereClause = null });

                            if (!string.IsNullOrWhiteSpace(previousName))
                                SelectedSavedView = SavedViews.FirstOrDefault(v => v.Name == previousName) ?? SavedViews.FirstOrDefault();
                            else
                                SelectedSavedView = SavedViews.FirstOrDefault();
                        });
                    }
                    else
                    {
                        SavedViews.Clear();
                        foreach (var p in ordered)
                        {
                            // Views are layout-only: ignore UPF_SQL.
                            SavedViews.Add(new UserViewPreset
                            {
                                Name = p.UPF_Name,
                                WhereClause = null
                            });
                        }

                        if (!SavedViews.Any() || SavedViews.First().Name != "None")
                            SavedViews.Insert(0, new UserViewPreset { Name = "None", WhereClause = null });

                        if (!string.IsNullOrWhiteSpace(previousName))
                            SelectedSavedView = SavedViews.FirstOrDefault(v => v.Name == previousName) ?? SavedViews.FirstOrDefault();
                        else
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
            catch (Exception ex) { }
        }

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
                // IMPORTANT: peupler immédiatement la liste Account (Pivot/Receivable) à partir du référentiel pays
                // avant les chargements asynchrones potentiellement lourds (filtres/vues, warm-up service)
                try { UpdateTopFiltersFromData(); } catch { }
                // Charger les listes (filtres/vues) et initialiser les combos de haut de page
                _pageCts?.Dispose();
                _pageCts = new CancellationTokenSource();
                var token = _pageCts.Token;
                await LoadDataAsync(token).ConfigureAwait(false);

                // Ne pas ouvrir de vue par défaut. Laisser l'utilisateur en sélectionner/ajouter une.
            }
            catch (Exception ex)
            {
                if (ex is OperationCanceledException)
                    ShowWarning("Initial load cancelled.");
                else
                    ShowError($"Error during initial load: {ex.Message}");
            }
        }

        private async void ReconciliationPage_Unloaded(object sender, RoutedEventArgs e)
        {
            try { StopLockPolling(); } catch { }
            // Do not sync on page unload: navigation to Reconciliation replaces the page and fires Unloaded,
            // which was causing an unintended synchronization on click. We keep sync only on explicit events
            // (country change and app/window close handled elsewhere).
        }

        /// <summary>
        /// Gestion du changement de pays
        /// </summary>
        private async void OnCountryChanged()
        {
            _currentCountryId = _offlineFirstService.CurrentCountryId;
            _pageCts?.Dispose();
            _pageCts = new CancellationTokenSource();
            var token = _pageCts.Token;
            // Reset current page-level filter context on country change
            _currentFilter = null;
            _currentFilterName = null;

            // Assurer la fin d'une synchro éventuelle avant de recharger les listes/combos
            try
            {
                var cid = _offlineFirstService?.CurrentCountryId ?? _offlineFirstService?.CurrentCountry?.CNT_Id;
                if (!string.IsNullOrWhiteSpace(cid))
                    await _offlineFirstService.WaitForSynchronizationAsync(cid, token).ConfigureAwait(false);
                // Recharger les référentiels (inclut UserFilters) pour refléter le nouveau contexte pays
                try { await _offlineFirstService.RefreshConfigurationAsync().ConfigureAwait(false); } catch { }
            }
            catch { }

            // Recharger explicitement les listes de filtres/vues et MAJ des comptes (Pivot/Receivable)
            try
            {
                await LoadSavedFiltersAsync(token).ConfigureAwait(false);
                await LoadSavedViewsAsync(token).ConfigureAwait(false);
                if (Dispatcher != null)
                    await Dispatcher.InvokeAsync(UpdateTopFiltersFromData);
                else
                    UpdateTopFiltersFromData();
            }
            catch { }

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

            // Trigger synchronization only on country selection change
            try { await TrySynchronizeIfSafeAsync(token).ConfigureAwait(false); } catch { }
            try { await LoadDataAsync(token).ConfigureAwait(false); } catch { }
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
                            // IMPORTANT: strip Account/Status predicates from the saved filter; page combos handle them.
                            try { _currentFilter = RecoTool.Services.UserFilterService.SanitizeWhereClause(selectedFilter.UFI_SQL); }
                            catch { _currentFilter = selectedFilter.UFI_SQL; }
                            _currentFilterName = selectedFilter.UFI_Name;
                        }
                    }
                    catch (Exception ex)
                    {
                        ShowError($"Error resetting filter: {ex.Message}");
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
                        SelectedSavedView = preset;

                        // Views are layout-only: do not change backend filter. Optionally apply layout/title to open views.
                        try
                        {
                            var panel = FindName("ViewsPanel") as StackPanel;
                            if (panel != null)
                            {
                                foreach (var child in panel.Children.OfType<ReconciliationView>())
                                {
                                    child.SetViewTitle(preset?.Name);
                                    try
                                    {
                                        var refCs = _offlineFirstService?.ReferentialConnectionString ?? RecoTool.Properties.Settings.Default.ReferentialDB;
                                        var curUser = _offlineFirstService?.CurrentUser ?? Environment.UserName;
                                        var viewSvc = new UserViewPreferenceService(refCs, curUser);
                                        var pref = await viewSvc.GetByNameAsync(preset?.Name);
                                        if (!string.IsNullOrWhiteSpace(pref?.UPF_ColumnWidths))
                                        {
                                            await child.Dispatcher.InvokeAsync(() =>
                                            {
                                                try { child.ApplyLayoutJson(pref.UPF_ColumnWidths); } catch { }
                                            });
                                        }
                                    }
                                    catch { }
                                }
                            }
                        }
                        catch { }
                    }
                }
                catch (Exception ex)
                {
                    ShowError($"Error selecting view: {ex.Message}");
                }
            }
        }

        /// <summary>
        /// Ajouter une nouvelle vue
        /// </summary>
        private async void AddViewButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                using var _ = BeginWaitCursor();
                // Hard guard: if sync or page load in progress, block and inform user
                try
                {
                    var countryId = _offlineFirstService?.CurrentCountryId ?? _offlineFirstService?.CurrentCountry?.CNT_Id;
                    if (IsLoading || (!string.IsNullOrWhiteSpace(countryId) && _offlineFirstService != null && _offlineFirstService.IsSynchronizationInProgress(countryId)))
                    {
                        ShowWarning("Synchronization in progress. Please wait until it finishes before adding a view.");
                        return;
                    }
                }
                catch { }
                await AwaitSafeToOpenViewAsync();
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
                ShowError($"Error adding view: {ex.Message}");
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
                    Title = "Export reconciliation data",
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
                ShowError($"Error during export: {ex.Message}");
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
                    ShowWarning("Please select a country before applying rules.");
                    return;
                }

                var result = MessageBox.Show(
                    "Are you sure you want to apply automatic rules?\nThis may modify existing Actions and KPIs.",
                    "Confirmation",
                    MessageBoxButton.YesNo,
                    MessageBoxImage.Question);

                if (result == MessageBoxResult.Yes)
                {
                    IsLoading = true;
                    _pageCts?.Dispose();
                    _pageCts = new CancellationTokenSource();
                    var token = _pageCts.Token;
                    await _reconciliationService.ApplyAutomaticRulesAsync(_currentCountryId);
                    await LoadDataAsync(token).ConfigureAwait(false);
                    
                    ShowInfo("Automatic rules applied successfully.");
                }
            }
            catch (Exception ex)
            {
                ShowError($"Error applying rules: {ex.Message}");
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
                    ShowWarning("Please select a country before automatic matching.");
                    return;
                }

                IsLoading = true;
                _pageCts?.Dispose();
                _pageCts = new CancellationTokenSource();
                var token = _pageCts.Token;
                var matchCount = await _reconciliationService.PerformAutomaticMatchingAsync(_currentCountryId);
                await LoadDataAsync(token).ConfigureAwait(false);
                
                ShowInfo($"Automatic matching completed.\n{matchCount} items matched.");
            }
            catch (Exception ex)
            {
                ShowError($"Error during automatic matching: {ex.Message}");
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
                    // Sync restricted: ignore auto-sync suggestions for now
                    await Task.CompletedTask;
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
        private async Task TrySynchronizeIfSafeAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                // Do nothing if no OfflineFirstService available
                if (_offlineFirstService == null) return;

                // Skip when global lock is active
                if (IsGlobalLockActive) return;

                // Skip when network is not available
                bool networkOk = false;
                try { networkOk = _offlineFirstService.IsNetworkSyncAvailable; } catch { networkOk = false; }
                if (!networkOk) return;

                // Cooldown gate (avoid frequent sync)
                if (DateTime.UtcNow - _lastAutoSyncUtc <= AutoSyncCooldown) return;

                // UI in-flight gate: prevent overlapping sync triggers
                if (System.Threading.Interlocked.CompareExchange(ref _syncInFlightFlag, 1, 0) != 0)
                    return;

                // Perform sync
                try { if (Dispatcher != null) await Dispatcher.InvokeAsync(() => IsLoading = true); else IsLoading = true; } catch { IsLoading = true; }
                cancellationToken.ThrowIfCancellationRequested();
                await _offlineFirstService.SynchronizeAsync(_currentCountryId, cancellationToken);
                _lastAutoSyncUtc = DateTime.UtcNow;
            }
            catch (Exception ex)
            {
                // Non-fatal
                try { ShowWarning($"Synchronization skipped: {ex.Message}"); } catch { }
            }
            finally
            {
                try { if (Dispatcher != null) await Dispatcher.InvokeAsync(() => IsLoading = false); else IsLoading = false; } catch { IsLoading = false; }
                System.Threading.Interlocked.Exchange(ref _syncInFlightFlag, 0);
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
                ShowInfo($"Export completed to {fileName}");
            }
            catch (Exception ex)
            {
                ShowError($"Error during export: {ex.Message}");
            }
        }

        /// <summary>
        /// Affiche un message d'erreur
        /// </summary>
        private void ShowError(string message)
        {
            MessageBox.Show(message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }

        /// <summary>
        /// Affiche un message d'avertissement
        /// </summary>
        private void ShowWarning(string message)
        {
            MessageBox.Show(message, "Warning", MessageBoxButton.OK, MessageBoxImage.Warning);
        }

        /// <summary>
        /// Affiche un message d'information
        /// </summary>
        private void ShowInfo(string message)
        {
            Console.WriteLine(message);
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
                // Le type de vue est déterminé par SelectedViewType (rendre robuste: trim et fallback sur ComboBox SelectedItem)
                var popup = string.Equals(SelectedViewType?.Trim(), "Popup", StringComparison.OrdinalIgnoreCase);
                try
                {
                    if (!popup)
                    {
                        var combo = FindName("ViewTypeComboBox") as ComboBox;
                        var txt = combo?.SelectedItem as string;
                        if (!string.IsNullOrWhiteSpace(txt))
                            popup = string.Equals(txt.Trim(), "Popup", StringComparison.OrdinalIgnoreCase);
                    }
                }
                catch { }
                // Hard guard: if sync or page load in progress, block and inform user
                try
                {
                    var countryId = _offlineFirstService?.CurrentCountryId ?? _offlineFirstService?.CurrentCountry?.CNT_Id;
                    if (IsLoading || (!string.IsNullOrWhiteSpace(countryId) && _offlineFirstService != null && _offlineFirstService.IsSynchronizationInProgress(countryId)))
                    {
                        ShowWarning("Synchronization in progress. Please wait until it finishes before adding a view.");
                        return;
                    }
                }
                catch { }
                await AwaitSafeToOpenViewAsync();
                await AddReconciliationView(popup);
            }
            catch (Exception ex)
            {
                ShowError($"Error adding view: {ex.Message}");
            }
        }

        private async Task AddReconciliationView(bool asPopup = false)
        {
            // Try late resolution if the field was not injected (e.g., instantiated via default ctor)
            var recoSvc = _reconciliationService ?? (RecoTool.App.ServiceProvider?.GetService(typeof(RecoTool.Services.ReconciliationService)) as RecoTool.Services.ReconciliationService);
            var repo = _recoRepository; // may be null

            if (recoSvc == null && repo == null)
            {
                ShowWarning("Reconciliation service is not available.");
                return;
            }

            // Créer la vue et l'attacher aux services existants
            var view = new ReconciliationView(recoSvc, _offlineFirstService)
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
                await ConfigureAndPreloadView(view);
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

            await ConfigureAndPreloadView(view);

            // Mettre à jour la visibilité
            panel.Visibility = Visibility.Visible;
            if (empty != null) empty.Visibility = Visibility.Collapsed;
            if (fab != null) fab.Visibility = Visibility.Visible;
        }

        /// <summary>
        /// Configure la vue (pays, filtres, layout) et précharge les données à partir du cache ou du service.
        /// Utilisée pour les ouvertures Popup et intégrées afin d'éviter la duplication et garantir le même comportement.
        /// </summary>
        private readonly IReconciliationRepository _recoRepository;

        private async Task ConfigureAndPreloadView(ReconciliationView view)
        {
            // Aligner le pays avant toute action (sans déclencher un Refresh prématuré)
            try { view.SyncCountryFromService(false); } catch { }
            // Synchroniser l'affichage des filtres d'en-tête
            view.UpdateExternalFilters(SelectedAccount, SelectedStatus);

            // Appliquer filtre courant (si présent) et titre
            if (!string.IsNullOrWhiteSpace(_currentFilter))
            {
                try { view.ApplySavedFilterSql(_currentFilter); } catch { }
                if (!string.IsNullOrWhiteSpace(_currentFilterName))
                {
                    try { view.SetViewTitle(_currentFilterName); } catch { }
                    _ = System.Threading.Tasks.Task.Run(async () =>
                    {
                        try
                        {
                            var refCs = _offlineFirstService?.ReferentialConnectionString ?? RecoTool.Properties.Settings.Default.ReferentialDB;
                            var curUser = _offlineFirstService?.CurrentUser ?? Environment.UserName;
                            var viewSvc = new UserViewPreferenceService(refCs, curUser);
                            var pref = await viewSvc.GetByNameAsync(_currentFilterName);
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

            // Appliquer layout/titre issu de la Saved View sélectionnée (layout-only)
            if (SelectedSavedView != null && !string.IsNullOrWhiteSpace(SelectedSavedView.Name))
            {
                try { view.SetViewTitle(SelectedSavedView.Name); } catch { }
                _ = System.Threading.Tasks.Task.Run(async () =>
                {
                    try
                    {
                        var refCs = _offlineFirstService?.ReferentialDatabasePath ?? RecoTool.Properties.Settings.Default.ReferentialDB;
                        var curUser = _offlineFirstService?.CurrentUser ?? Environment.UserName;
                        var viewSvc = new UserViewPreferenceService(refCs, curUser);
                        var pref = await viewSvc.GetByNameAsync(SelectedSavedView.Name);
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

            // No synchronization when opening a view: preload from service, then initialize and refresh
            var countryId = _offlineFirstService?.CurrentCountryId ?? _offlineFirstService?.CurrentCountry?.CNT_Id;
            var backendSql = _currentFilter;
            // Freeze local references for lambda capture (resolve from DI as fallback)
            var localRepo = _recoRepository ?? (RecoTool.App.ServiceProvider?.GetService(typeof(RecoTool.Domain.Repositories.IReconciliationRepository)) as RecoTool.Domain.Repositories.IReconciliationRepository);
            var localSvc = _reconciliationService ?? (RecoTool.App.ServiceProvider?.GetService(typeof(RecoTool.Services.ReconciliationService)) as RecoTool.Services.ReconciliationService);
            _ = System.Threading.Tasks.Task.Run(async () =>
            {
                try
                {
                    var list = localRepo != null
                        ? await localRepo.GetReconciliationViewAsync(countryId, backendSql).ConfigureAwait(false)
                        : await localSvc.GetReconciliationViewAsync(countryId, backendSql).ConfigureAwait(false);
                    await view.Dispatcher.InvokeAsync(() =>
                    {
                        try { view.InitializeWithPreloadedData(list, backendSql); } catch { }
                        try { view.Refresh(); } catch { }
                    });
                }
                catch { }
            });
        }

        /// <summary>
        /// Attend de manière fiable la fin d'une synchro et de tout chargement de page avant d'ouvrir une vue,
        /// puis rafraîchit les filtres de haut de page pour garantir l'application des filtres obligatoires.
        /// </summary>
        private async Task AwaitSafeToOpenViewAsync(CancellationToken cancellationToken = default)
        {
            // 1) Attendre la synchro de OfflineFirstService (si disponible)
            try
            {
                var countryId = _offlineFirstService?.CurrentCountryId ?? _offlineFirstService?.CurrentCountry?.CNT_Id;
                if (!string.IsNullOrWhiteSpace(countryId) && _offlineFirstService != null)
                {
                    await _offlineFirstService.WaitForSynchronizationAsync(countryId, cancellationToken).ConfigureAwait(false);
                }
            }
            catch { }

            // 2) Attendre que la page ait fini son propre état de chargement (ex: TrySynchronizeIfSafeAsync en cours)
            try
            {
                var waited = 0;
                while (IsLoading && waited < 30000) // max 30s safety
                {
                    await Task.Delay(150, cancellationToken).ConfigureAwait(false);
                    waited += 150;
                }
            }
            catch { }

            // 3) Rafraîchir seulement les filtres de haut de page (comptes) sans recharger Saved Filters/Views
            try
            {
                if (Dispatcher != null && !Dispatcher.CheckAccess())
                    await Dispatcher.InvokeAsync(UpdateTopFiltersFromData);
                else
                    UpdateTopFiltersFromData();
            }
            catch { }
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
                ToolTip = "Reduce height"
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
                Text = "Feature to implement",
                HorizontalAlignment = HorizontalAlignment.Center,
                VerticalAlignment = VerticalAlignment.Center
            };
            grid.Children.Add(textBlock);
            Content = grid;
        }
    }

    #endregion
}
#endregion