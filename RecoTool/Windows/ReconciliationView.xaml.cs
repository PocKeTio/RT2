using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Input;
using System.Windows.Threading;
using System.Diagnostics;
using Microsoft.VisualBasic;
using RecoTool.Models;
using RecoTool.Properties;
using RecoTool.Services;
using OfflineFirstAccess.Models;
using RecoTool.Domain.Filters;
using RecoTool.UI.ViewModels;
using RecoTool.UI.Models;
using RecoTool.Infrastructure.Logging;
using System.Windows.Media;
using System.Windows.Controls.Primitives;
using RecoTool.Services.DTOs;
using RecoTool.UI.Helpers;
using RecoTool.Helpers;

namespace RecoTool.Windows
{
    /// <summary>
    /// Logique d'interaction pour ReconciliationView.xaml
    /// Vue de réconciliation avec filtres et données réelles
    /// </summary>
    public partial class ReconciliationView : UserControl, INotifyPropertyChanged
    {
        #region Fields and Properties

        private readonly ReconciliationService _reconciliationService;
        private readonly OfflineFirstService _offlineFirstService;
        // MVVM bridge: lightweight ViewModel holder to gradually migrate bindings
        public ReconciliationViewViewModel VM { get; } = new ReconciliationViewViewModel();
        private string _currentCountryId;
        private string _currentView = "Default View";
        private bool _isLoading;
        private bool _canRefresh = true;
        private bool _initialLoaded;
        private DispatcherTimer _filterDebounceTimer;
        // Transient highlight clear timer
        private DispatcherTimer _highlightClearTimer;
        private bool _isSyncRefreshInProgress;
        private const int HighlightDurationMs = 4000;
        private bool _syncEventsHooked;
        private bool _hasLoadedOnce; // set after first RefreshCompleted to avoid double-load on startup
        // Debounce timer for background push (avoid immediate sync on rapid edits)
        private DispatcherTimer _pushDebounceTimer;

        // Collections pour l'affichage (vue combinée)
        private ObservableCollection<ReconciliationViewData> _viewData;
        private List<ReconciliationViewData> _allViewData; // Toutes les données pour le filtrage
        // Paging / incremental loading
        private const int InitialPageSize = 500;
        private List<ReconciliationViewData> _filteredData; // Données filtrées complètes (pour totaux/scroll)
        private int _loadedCount; // Nombre actuellement affiché dans ViewData
        private bool _isLoadingMore; // Garde-fou
        private bool _scrollHooked; // Pour éviter double-hook
        private ScrollViewer _resultsScrollViewer;
        private Button _loadMoreFooterButton; // cache footer button to avoid repeated FindName on scroll
        // Filtre backend transmis au service (défini par la page au moment de l'ajout de vue)
        private string _backendFilterSql;

        // Données préchargées par la page parente (si présentes, on évite un fetch service)
        private IReadOnlyList<ReconciliationViewData> _preloadedAllData;

        // Perf: throttled logging for scroll handling (avoid log spam)
        private DateTime _lastScrollPerfLog = DateTime.MinValue;
        private const int ScrollLogThrottleMs = 500;

        // Propriétés de filtrage (legacy display-only field kept)
        private string _filterCountry;

        // (see further below for filter properties; using ScheduleApplyFiltersDebounced)

        public event PropertyChangedEventHandler PropertyChanged;
        public event EventHandler CloseRequested;

        private void OnPropertyChanged(string propertyName = null)
        {
            try
            {
                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
            }
            catch { }
        }

        // Lock first 4 columns (N, U, M, Account) from being moved
        private void ResultsDataGrid_ColumnReordering(object sender, DataGridColumnReorderingEventArgs e)
        {
            try
            {
                int protectedCount = 3; // Protect first three indicator columns: N, U, M
                int currentIndex = e.Column.DisplayIndex;
                if (currentIndex < protectedCount)
                {
                    e.Cancel = true;
                }
            }
            catch { }
        }

        // Auto-open DatePicker calendar on edit start for faster date selection
        private void DatePicker_OpenOnLoad(object sender, RoutedEventArgs e)
        {
            try
            {
                var dp = sender as DatePicker;
                if (dp == null) return;
                // Ensure French culture visual formatting if needed
                try { dp.Language = System.Windows.Markup.XmlLanguage.GetLanguage("fr-FR"); } catch { }
                // Open the popup calendar immediately
                try { dp.IsDropDownOpen = true; } catch { }
            }
            catch { }
        }

        

        

        private void ResultsDataGrid_Loaded(object sender, RoutedEventArgs e)
        {
            try
            {
                if (sender is DataGrid dg)
                {
                    TryHookResultsGridScroll(dg);
                }
            }
            catch { }
        }

        // Open a popup view showing rows that share the same DWINGS_InvoiceID (BGI) or, if absent, the same InternalInvoiceReference
        private void MatchedIndicator_MouseLeftButtonUp(object sender, MouseButtonEventArgs e)
        {
            try
            {
                var tb = sender as TextBlock;
                var rowData = tb?.DataContext as ReconciliationViewData;
                OpenMatchedPopup(rowData);
            }
            catch { }
        }

        #endregion

        // Single-click to edit cells
        private void DataGridCell_PreviewMouseLeftButtonDown(object sender, MouseButtonEventArgs e)
        {
            try
            {
                var cell = sender as DataGridCell;
                if (cell == null) return;
                if (cell.IsEditing || cell.IsReadOnly) return;

                if (!cell.IsFocused)
                    cell.Focus();

                var dataGrid = VisualTreeHelpers.FindParent<DataGrid>(cell);
                if (dataGrid == null) return;
                // Ensure single selection by selecting the owning row item directly
                var row = VisualTreeHelpers.FindParent<DataGridRow>(cell);
                if (row != null)
                {
                    dataGrid.SelectedItem = row.Item;
                }

                // If the cell hosts a CheckBox (e.g., Risky Item), let the CheckBox handle the click
                // and do not force BeginEdit which can cause sticky edit mode.
                var hasCheckBox = cell.Content is CheckBox || VisualTreeHelpers.FindDescendant<CheckBox>(cell) != null;
                if (!hasCheckBox)
                {
                    dataGrid.BeginEdit(e);
                    e.Handled = true;
                }
            }
            catch { }
        }



        // Ensure inner DataGrid scroll consumes the mouse wheel instead of the container page
        private void ResultsDataGrid_PreviewMouseWheel(object sender, MouseWheelEventArgs e)
        {
            try
            {
                var dg = sender as DataGrid;
                if (dg == null) return;
                if (_resultsScrollViewer == null)
                {
                    _resultsScrollViewer = VisualTreeHelpers.FindDescendant<ScrollViewer>(dg);
                }
                if (_resultsScrollViewer != null)
                {
                    // Route wheel to inner ScrollViewer and prevent bubbling
                    e.Handled = true;
                    int steps = Math.Max(1, Math.Abs(e.Delta) / 120);
                    for (int i = 0; i < steps; i++)
                    {
                        if (e.Delta > 0) _resultsScrollViewer.LineUp(); else _resultsScrollViewer.LineDown();
                    }
                }
            }
            catch { }
        }

        // New: populate via DataGridRow ContextMenuOpening to avoid XAML column parsing issues
        private void DataGridRow_ContextMenuOpening(object sender, ContextMenuEventArgs e)
        {
            try
            {
                var row = sender as DataGridRow;
                if (row == null) return;
                var rowData = row.DataContext as ReconciliationViewData;
                if (rowData == null) return;

                var cm = row.ContextMenu;
                if (cm == null) return;

                // Resolve the root submenus
                MenuItem actionRoot = cm.Items.OfType<MenuItem>().FirstOrDefault(mi => (mi.Tag as string) == "Action");
                MenuItem kpiRoot = cm.Items.OfType<MenuItem>().FirstOrDefault(mi => (mi.Tag as string) == "KPI");
                MenuItem incRoot = cm.Items.OfType<MenuItem>().FirstOrDefault(mi => (mi.Tag as string) == "Incident Type");

                void Populate(MenuItem root, string category)
                {
                    if (root == null) return;
                    root.Items.Clear();

                    var options = GetUserFieldOptionsForRow(category, rowData).ToList();

                    var clearItem = new MenuItem { Header = $"Clear {category}", Tag = category, CommandParameter = null, DataContext = rowData };
                    clearItem.Click += QuickSetUserFieldMenuItem_Click;
                    bool hasValue = (string.Equals(category, "Action", StringComparison.OrdinalIgnoreCase) && rowData.Action.HasValue)
                                     || (string.Equals(category, "KPI", StringComparison.OrdinalIgnoreCase) && rowData.KPI.HasValue)
                                     || (string.Equals(category, "Incident Type", StringComparison.OrdinalIgnoreCase) && rowData.IncidentType.HasValue);
                    clearItem.IsEnabled = hasValue;
                    root.Items.Add(clearItem);

                    foreach (var opt in options)
                    {
                        var mi = new MenuItem
                        {
                            Header = opt.USR_FieldName,
                            Tag = category,
                            CommandParameter = opt.USR_ID,
                            DataContext = rowData,
                            IsCheckable = true,
                            IsChecked = (string.Equals(category, "Action", StringComparison.OrdinalIgnoreCase) && rowData.Action == opt.USR_ID)
                                        || (string.Equals(category, "KPI", StringComparison.OrdinalIgnoreCase) && rowData.KPI == opt.USR_ID)
                                        || (string.Equals(category, "Incident Type", StringComparison.OrdinalIgnoreCase) && rowData.IncidentType == opt.USR_ID)
                        };
                        mi.Click += QuickSetUserFieldMenuItem_Click;
                        root.Items.Add(mi);
                    }

                    root.IsEnabled = options.Any() || hasValue;
                }

                Populate(actionRoot, "Action");
                Populate(kpiRoot, "KPI");
                Populate(incRoot, "Incident Type");

                // Add Set Comment action applicable to multi-selection
                try
                {
                    // Avoid duplicates: remove previous injected items
                    var existing = cm.Items.OfType<MenuItem>().FirstOrDefault(m => (m.Tag as string) == "__SetComment__");
                    if (existing != null) cm.Items.Remove(existing);
                    var existingTake = cm.Items.OfType<MenuItem>().FirstOrDefault(m => (m.Tag as string) == "__Take__");
                    if (existingTake != null) cm.Items.Remove(existingTake);
                    var existingRem = cm.Items.OfType<MenuItem>().FirstOrDefault(m => (m.Tag as string) == "__SetReminder__");
                    if (existingRem != null) cm.Items.Remove(existingRem);
                    var existingDone = cm.Items.OfType<MenuItem>().FirstOrDefault(m => (m.Tag as string) == "__MarkActionDone__");
                    if (existingDone != null) cm.Items.Remove(existingDone);
                    var sep = cm.Items.OfType<Separator>().FirstOrDefault(s => (s.Tag as string) == "__InjectedSep__");
                    if (sep != null) cm.Items.Remove(sep);

                    cm.Items.Add(new Separator { Tag = "__InjectedSep__" });
                    var takeItem = new MenuItem { Header = "Take (Assign to me)", Tag = "__Take__", DataContext = rowData };
                    takeItem.Click += QuickTakeMenuItem_Click;
                    cm.Items.Add(takeItem);
                    var reminderItem = new MenuItem { Header = "Set Reminder Date…", Tag = "__SetReminder__", DataContext = rowData };
                    reminderItem.Click += QuickSetReminderMenuItem_Click;
                    cm.Items.Add(reminderItem);
                    var doneItem = new MenuItem { Header = "Set Action as DONE", Tag = "__MarkActionDone__", DataContext = rowData };
                    doneItem.Click += QuickMarkActionDoneMenuItem_Click;
                    cm.Items.Add(doneItem);
                    var commentItem = new MenuItem { Header = "Set Comment…", Tag = "__SetComment__" };
                    commentItem.Click += QuickSetCommentMenuItem_Click;
                    cm.Items.Add(commentItem);
                }
                catch { }
            }
            catch { }
        }

        

        

        /// <summary>
        /// Reçoit un SQL de filtre sauvegardé depuis la page parente.
        /// S'il contient un snapshot JSON, restaure les champs UI et isole le WHERE pur
        /// pour le backend. Sinon, transmet tel quel au backend. N'effectue pas de chargement ici.
        /// </summary>
        public void ApplySavedFilterSql(string sql)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(sql))
                {
                    _backendFilterSql = null;
                    // Any change in backend filter invalidates previously preloaded data
                    _preloadedAllData = null;
                    return;
                }

                if (TryExtractPresetFromSql(sql, out var preset, out var pureWhere))
                {
                    // Restaurer l'UI de la vue selon le snapshot
                    ApplyFilterPreset(preset);
                    // Recalculer une WHERE clause propre basée sur l'état courant (sans compte du preset)
                    _backendFilterSql = GenerateWhereClause();
                }
                else
                {
                    // Aucun snapshot: transmettre au backend en retirant le filtre compte s'il est présent
                    _backendFilterSql = StripAccountFromWhere(sql);
                }
                // Backend filter changed: drop preloaded data to force a fresh load
                _preloadedAllData = null;
            }
            catch
            {
                // En cas d'erreur de parsing, fallback sur le SQL brut
                _backendFilterSql = StripAccountFromWhere(sql);
                _preloadedAllData = null;
            }
        }

        /* moved to partial: SyncAndTimers.cs (InitializeFilterDebounce) */

        

        // Ensure the push debounce timer exists
        /* moved to partial: SyncAndTimers.cs (EnsurePushDebounceTimer) */

        // Timer handlers (named so we can unsubscribe on Unloaded)
        /* moved to partial: SyncAndTimers.cs (FilterDebounceTimer_Tick) */

        /* moved to partial: SyncAndTimers.cs (PushDebounceTimer_Tick) */

        // Public entry to schedule a debounced background push
        /* moved to partial: SyncAndTimers.cs (ScheduleBulkPushDebounced) */

        /* moved to partial: SyncAndTimers.cs (SubscribeToSyncEvents) */

        /* moved to partial: SyncAndTimers.cs (ReconciliationView_Unloaded) */

        /* moved to partial: SyncAndTimers.cs (OnSyncStateChanged) */

        // When any VM Filter* property changes, debounce ApplyFilters
        /* moved to partial: SyncAndTimers.cs (VM_PropertyChanged) */

        /* moved to partial: SyncAndTimers.cs (HandleSyncStateChangedAsync) */

        /* moved to partial: SyncAndTimers.cs (HasMeaningfulUpdate) */

        /* moved to partial: SyncAndTimers.cs (StringEquals/NullableEquals) */

        /* moved to partial: SyncAndTimers.cs (StartHighlightClearTimer) */

        /* moved to partial: SyncAndTimers.cs (HighlightClearTimer_Tick) */

        /* moved to partial: SyncAndTimers.cs (ScheduleApplyFiltersDebounced) */

        /* moved to partial: SyncAndTimers.cs (QueueBulkPush) */

        // ---- Generic saved filter snapshot support ----

        /* moved to partial: FilterSqlBridge.cs (GetCurrentFilterPreset) */

        /* moved to partial: FilterSqlBridge.cs (ApplyFilterPreset) */

        /// <summary>
        /// Removes any Account_ID = '...' predicate from a WHERE or full SQL fragment.
        /// Preserves other predicates and keeps/strips the WHERE keyword appropriately.
        /// </summary>
        /* moved to partial: FilterSqlBridge.cs (StripAccountFromWhere) */

        /* moved to partial: FilterSqlBridge.cs (BuildSqlWithJsonComment) */

        /* moved to partial: FilterSqlBridge.cs (TryExtractPresetFromSql) */

        /// <summary>
        /// Constructeur par défaut
        /// </summary>
        public ReconciliationView()
        {
            InitializeComponent();
            InitializeData();
            DataContext = this;
            Loaded += ReconciliationView_Loaded;
            Unloaded += ReconciliationView_Unloaded;
            InitializeFilterDebounce();
            SubscribeToSyncEvents();
            RefreshCompleted += (s, e) => _hasLoadedOnce = true;
            try { VM.PropertyChanged += VM_PropertyChanged; } catch { }
        }

        /// <summary>
        /// Constructeur avec services
        /// </summary>
        public ReconciliationView(ReconciliationService reconciliationService, OfflineFirstService offlineFirstService) : this()
        {
            _reconciliationService = reconciliationService;
            _offlineFirstService = offlineFirstService;

            // Synchroniser avec la country courante du service
            _currentCountryId = _offlineFirstService?.CurrentCountry?.CNT_Id;
            // Notifier que les référentiels sont disponibles pour les liaisons XAML
            OnPropertyChanged(nameof(AllUserFields));
            OnPropertyChanged(nameof(CurrentCountryObject));

            InitializeFromServices();

            // Enable header context menu for column visibility
            this.Loaded += (s, e) =>
            {
                try
                {
                    var dg = this.FindName("ResultsDataGrid") as DataGrid;
                    if (dg != null)
                    {
                        try { dg.GotFocus -= ResultsDataGrid_GotFocus; } catch { }
                        try { dg.GotFocus += ResultsDataGrid_GotFocus; } catch { }
                        dg.PreviewMouseRightButtonUp -= ResultsDataGrid_PreviewMouseRightButtonUp;
                        dg.PreviewMouseRightButtonUp += ResultsDataGrid_PreviewMouseRightButtonUp;
                        dg.CanUserSortColumns = true; // allow sorting on all columns (template ones have SortMemberPath in XAML)
                        TryHookResultsGridScroll(dg);
                    }
                }
                catch { }
            };

            try { this.GotFocus -= ReconciliationView_GotFocus; } catch { }
            try { this.GotFocus += ReconciliationView_GotFocus; } catch { }
        }

        private void ReconciliationView_GotFocus(object sender, RoutedEventArgs e)
        {
            try { ReconciliationViewFocusTracker.SetLastFocused(this); } catch { }
        }

        private void ResultsDataGrid_GotFocus(object sender, RoutedEventArgs e)
        {
            try { ReconciliationViewFocusTracker.SetLastFocused(this); } catch { }
        }

        public void FlashLinkProposalHighlight()
        {
            try
            {
                var b = this.FindName("HeaderBorder") as Border;
                if (b == null) return;
                var prevBrush = b.BorderBrush;
                var prevThickness = b.BorderThickness;
                b.BorderBrush = Brushes.Red;
                b.BorderThickness = new Thickness(3);
                var t = new DispatcherTimer { Interval = TimeSpan.FromSeconds(3) };
                t.Tick += (s, e) =>
                {
                    try
                    {
                        b.BorderBrush = prevBrush;
                        b.BorderThickness = prevThickness;
                    }
                    catch { }
                    finally { (s as DispatcherTimer)?.Stop(); }
                };
                t.Start();
            }
            catch { }
        }

        /// <summary>
        /// Initialise les données par défaut
        /// </summary>
        /* moved to partial: DataLoading.cs */

        /// <summary>
        /// Initialise les données depuis les services
        /// </summary>
        /* moved to partial: DataLoading.cs */

        /* moved to partial: DataLoading.cs (ReconciliationView_Loaded) */

        public ObservableCollection<ReconciliationViewData> ViewData
        {
            get => _viewData;
            set
            {
                _viewData = value;
                OnPropertyChanged(nameof(ViewData));
                // Keep VM collection in sync so the DataGrid (bound to VM.ViewData) updates
                try { VM.ViewData = value; } catch { }
            }
        }

        // Expose referentials for XAML bindings (ComboBox items/label resolution)
        public IReadOnlyList<UserField> AllUserFields => _offlineFirstService?.UserFields;
        public Country CurrentCountryObject => _offlineFirstService?.CurrentCountry;

        // Options moved to ReconciliationView.Options.cs

        public string CurrentView
        {
            get => _currentView;
            set
            {
                _currentView = value;
                OnPropertyChanged(nameof(CurrentView));
                UpdateViewTitle();
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
            }
        }

        #region Row ContextMenu (Quick Set: Action/KPI/Incident)
        // Moved to ReconciliationView.RowActions.cs
        #endregion

        #region IRefreshable Implementation

        public bool CanRefresh => _canRefresh && !string.IsNullOrEmpty(_currentCountryId);

        public event EventHandler RefreshStarted;
        public event EventHandler RefreshCompleted;

        /* moved to partial: DataLoading.cs (Refresh) */

        /* moved to partial: DataLoading.cs (RefreshAsync) */

        #endregion

        #region Data Loading

        /* moved to partial: DataLoading.cs (LoadInitialData) */

        /* moved to partial: DataLoading.cs (LoadReconciliationDataAsync) */

        /* moved to partial: DataLoading.cs (InitializeWithPreloadedData) */

        /* moved to partial: DataLoading.cs (UpdateExternalFilters) */

        /* moved to partial: DataLoading.cs (UpdateCountryPivotReceivableInfo) */

        /* moved to partial: DataLoading.cs (ResolveAccountIdForFilter) */

        /* moved to partial: DataLoading.cs (SyncCountryFromService) */

        
        
        #endregion
        

        // Moved to partial: Events.cs

        #region Bound Filter Properties

        public string FilterAccountId { get => VM.FilterAccountId; set { VM.FilterAccountId = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterAccountId)); ScheduleApplyFiltersDebounced(); } }
        public string FilterCurrency { get => VM.FilterCurrency; set { VM.FilterCurrency = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterCurrency)); ScheduleApplyFiltersDebounced(); } }

        public string FilterCountry { get => _filterCountry; set { _filterCountry = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterCountry)); ScheduleApplyFiltersDebounced(); } }
        public DateTime? FilterFromDate { get => VM.FilterFromDate; set { VM.FilterFromDate = value; OnPropertyChanged(nameof(FilterFromDate)); ScheduleApplyFiltersDebounced(); } }
        public DateTime? FilterToDate { get => VM.FilterToDate; set { VM.FilterToDate = value; OnPropertyChanged(nameof(FilterToDate)); ScheduleApplyFiltersDebounced(); } }
        public DateTime? FilterDeletedDate { get => VM.FilterDeletedDate; set { VM.FilterDeletedDate = value; OnPropertyChanged(nameof(FilterDeletedDate)); ScheduleApplyFiltersDebounced(); } }
        public decimal? FilterMinAmount { get => VM.FilterMinAmount; set { VM.FilterMinAmount = value; OnPropertyChanged(nameof(FilterMinAmount)); ScheduleApplyFiltersDebounced(); } }
        public decimal? FilterMaxAmount { get => VM.FilterMaxAmount; set { VM.FilterMaxAmount = value; OnPropertyChanged(nameof(FilterMaxAmount)); ScheduleApplyFiltersDebounced(); } }
        public string FilterReconciliationNum { get => VM.FilterReconciliationNum; set { VM.FilterReconciliationNum = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterReconciliationNum)); ScheduleApplyFiltersDebounced(); } }
        public string FilterRawLabel { get => VM.FilterRawLabel; set { VM.FilterRawLabel = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterRawLabel)); ScheduleApplyFiltersDebounced(); } }
        public string FilterEventNum { get => VM.FilterEventNum; set { VM.FilterEventNum = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterEventNum)); ScheduleApplyFiltersDebounced(); } }
        public string FilterComments { get => VM.FilterComments; set { VM.FilterComments = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterComments)); ScheduleApplyFiltersDebounced(); } }
        public string FilterDwGuaranteeId { get => VM.FilterDwGuaranteeId; set { VM.FilterDwGuaranteeId = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterDwGuaranteeId)); ScheduleApplyFiltersDebounced(); } }
        public string FilterDwCommissionId { get => VM.FilterDwCommissionId; set { VM.FilterDwCommissionId = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterDwCommissionId)); ScheduleApplyFiltersDebounced(); } }
        public string FilterStatus { get => VM.FilterStatus; set { VM.FilterStatus = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterStatus)); ScheduleApplyFiltersDebounced(); } }

        // New string-backed ComboBox filters
        public string FilterGuaranteeType { get => VM.FilterGuaranteeType; set { VM.FilterGuaranteeType = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterGuaranteeType)); ScheduleApplyFiltersDebounced(); } }

        public string FilterTransactionType { get => VM.CurrentFilter.TransactionType; set { VM.CurrentFilter.TransactionType = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterTransactionType)); ScheduleApplyFiltersDebounced(); } }

        // New: Transaction Type filter by enum id (matches DataAmbre.Category int)
        public int? FilterTransactionTypeId
        {
            get => VM.FilterTransactionTypeId;
            set
            {
                // Treat negative sentinel values (e.g., -1 for 'All') as null
                var coerced = (value.HasValue && value.Value < 0) ? (int?)null : value;
                VM.FilterTransactionTypeId = coerced;
                OnPropertyChanged(nameof(FilterTransactionTypeId));
                ScheduleApplyFiltersDebounced();
            }
        }

        // ID-backed referential filter wrappers
        public int? FilterActionId { get => VM.FilterActionId; set { VM.FilterActionId = value; OnPropertyChanged(nameof(FilterActionId)); ScheduleApplyFiltersDebounced(); } }
        public int? FilterKpiId { get => VM.FilterKpiId; set { VM.FilterKpiId = value; OnPropertyChanged(nameof(FilterKpiId)); ScheduleApplyFiltersDebounced(); } }
        public int? FilterIncidentTypeId { get => VM.FilterIncidentTypeId; set { VM.FilterIncidentTypeId = value; OnPropertyChanged(nameof(FilterIncidentTypeId)); ScheduleApplyFiltersDebounced(); } }

        public string FilterGuaranteeStatus { get => VM.FilterGuaranteeStatus; set { VM.FilterGuaranteeStatus = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterGuaranteeStatus)); ScheduleApplyFiltersDebounced(); } }

        // New: Assignee filter (user id string)
        public string FilterAssigneeId
        {
            get => VM.FilterAssigneeId;
            set { VM.FilterAssigneeId = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterAssigneeId)); ScheduleApplyFiltersDebounced(); }
        }

        // New: Potential Duplicates filter (checkbox)
        public bool FilterPotentialDuplicates
        {
            get => VM.FilterPotentialDuplicates;
            set { VM.FilterPotentialDuplicates = value; OnPropertyChanged(nameof(FilterPotentialDuplicates)); ScheduleApplyFiltersDebounced(); }
        }

        // New: Unmatched and NewLines toggles
        public bool FilterUnmatched
        {
            get => VM.FilterUnmatched;
            set { VM.FilterUnmatched = value; OnPropertyChanged(nameof(FilterUnmatched)); ScheduleApplyFiltersDebounced(); }
        }

        public bool FilterNewLines
        {
            get => VM.FilterNewLines;
            set { VM.FilterNewLines = value; OnPropertyChanged(nameof(FilterNewLines)); ScheduleApplyFiltersDebounced(); }
        }

        // New filters: Action Done and Action Date range
        public bool? FilterActionDone
        {
            get => VM.FilterActionDone;
            set { VM.FilterActionDone = value; OnPropertyChanged(nameof(FilterActionDone)); ScheduleApplyFiltersDebounced(); }
        }

        public DateTime? FilterActionDateFrom
        {
            get => VM.FilterActionDateFrom;
            set { VM.FilterActionDateFrom = value; OnPropertyChanged(nameof(FilterActionDateFrom)); ScheduleApplyFiltersDebounced(); }
        }

        public DateTime? FilterActionDateTo
        {
            get => VM.FilterActionDateTo;
            set { VM.FilterActionDateTo = value; OnPropertyChanged(nameof(FilterActionDateTo)); ScheduleApplyFiltersDebounced(); }
        }

        #endregion

        // Options moved to ReconciliationView.Options.cs

        // --- Dynamic top filter options loading ---
        // Options moved to ReconciliationView.Options.cs

        

        #region Editing Handlers (persist user field changes)
        /* moved to partial: Editing.cs */
        #endregion

        #region Filtering

        /* moved to partial: Filtering.cs */

        

        

        

        

        /* moved to partial: Filtering.cs */

        /* moved to partial: Filtering.cs */

        #endregion

        #region Event Handlers

        /// <summary>
        /// Basculer l'affichage des filtres
        /// </summary>
        /* moved to partial: Events.cs */
        /* moved to partial: Events.cs */

        /// <summary>
        /// Sélection changée dans la grille
        /// </summary>
        /* moved to partial: Events.cs */
        /* moved to partial: Events.cs */

        /// <summary>
        /// Double-clic sur une ligne de la grille
        /// </summary>
        /* moved to partial: Events.cs */
        /* moved to partial: Events.cs */

        /// <summary>
        /// Efface les filtres (événement du bouton)
        /// </summary>
        /* moved to partial: Events.cs */
        /* moved to partial: Events.cs */

        #endregion

        #region Helper Methods

        // Removed unused ReadFiltersFromUI (legacy direct control scraping)

        /// <summary>
        /// Gestion du double-clic sur une ligne Ambre (code-behind pur)
        /// </summary>
        private void OnAmbreItemDoubleClick(DataAmbre item)
        {
            try
            {
                // Open detail of an Ambre line
                MessageBox.Show($"Ambre Detail - ID: {item.ID}\nAccount: {item.Account_ID}\nAmount: {item.SignedAmount:N2}\nCurrency: {item.CCY}\nDate: {item.Operation_Date:d}",
                               "Ambre Detail", MessageBoxButton.OK, MessageBoxImage.Information);
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Error opening detail: {ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        /* moved to partial: Title.cs */
        /* moved to partial: Title.cs */

        /* moved to partial: Title.cs */
        /* moved to partial: Title.cs */

        

        /// <summary>
        /// Obtient la valeur d'une TextBox
        /// </summary>
        private string GetTextBoxValue(string name)
        {
            var textBox = FindName(name) as TextBox;
            return textBox?.Text?.Trim();
        }

        /// <summary>
        /// Obtient une valeur décimale d'une TextBox
        /// </summary>
        private decimal? GetDecimalFromTextBox(string name)
        {
            var value = GetTextBoxValue(name);
            return decimal.TryParse(value, out decimal result) ? result : (decimal?)null;
        }

        /// <summary>
        /// Obtient la valeur d'un DatePicker
        /// </summary>
        private DateTime? GetDatePickerValue(string name)
        {
            var datePicker = FindName(name) as DatePicker;
            return datePicker?.SelectedDate;
        }

        /// <summary>
        /// Obtient la valeur entière d'un ComboBox
        /// </summary>
        private int? GetComboBoxIntValue(string name)
        {
            var comboBox = FindName(name) as ComboBox;
            if (comboBox?.SelectedValue != null && int.TryParse(comboBox.SelectedValue.ToString(), out int result))
                return result;
            return null;
        }

        /// <summary>
        /// Efface une TextBox
        /// </summary>
        private void ClearTextBox(string name)
        {
            var textBox = FindName(name) as TextBox;
            if (textBox != null) textBox.Text = string.Empty;
        }

        /// <summary>
        /// Efface un DatePicker
        /// </summary>
        private void ClearDatePicker(string name)
        {
            var datePicker = FindName(name) as DatePicker;
            if (datePicker != null) datePicker.SelectedDate = null;
        }

        /// <summary>
        /// Efface un ComboBox
        /// </summary>
        private void ClearComboBox(string name)
        {
            var comboBox = FindName(name) as ComboBox;
            if (comboBox != null) comboBox.SelectedIndex = -1;
        }

        

        

        /// <summary>
        /// Displays an error message
        /// </summary>
        /* moved to partial: Logging.cs */

        /* moved to partial: Logging.cs */

        /* moved to partial: Logging.cs */

        /* moved to partial: FilterSqlBridge.cs (GenerateWhereClause) */

        /* moved to partial: FilterSqlBridge.cs (ApplyWhereClause) */

        

        

        

        

        

        

        /* moved to partial: Events.cs */
    }
}
#endregion