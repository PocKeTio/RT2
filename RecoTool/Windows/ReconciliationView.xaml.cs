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
using System.Windows.Media;
using System.Windows.Controls.Primitives;

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
        private string _currentCountryId;
        private string _currentView = "Default View";
        private bool _isLoading;
        private bool _canRefresh = true;
        private bool _initialLoaded;
        private DispatcherTimer _filterDebounceTimer;

        // Collections pour l'affichage (vue combinée)
        private ObservableCollection<ReconciliationViewData> _viewData;
        private List<ReconciliationViewData> _allViewData; // Toutes les données pour le filtrage
        // Paging / incremental loading
        private const int InitialPageSize = 100;
        private List<ReconciliationViewData> _filteredData; // Données filtrées complètes (pour totaux/scroll)
        private int _loadedCount; // Nombre actuellement affiché dans ViewData
        private bool _isLoadingMore; // Garde-fou
        private bool _scrollHooked; // Pour éviter double-hook
        private ScrollViewer _resultsScrollViewer;
        // Filtre backend transmis au service (défini par la page au moment de l'ajout de vue)
        private string _backendFilterSql;

        // Données préchargées par la page parente (si présentes, on évite un fetch service)
        private IReadOnlyList<ReconciliationViewData> _preloadedAllData;

        // Perf: throttled logging for scroll handling (avoid log spam)
        private DateTime _lastScrollPerfLog = DateTime.MinValue;
        private const int ScrollLogThrottleMs = 250;

        // Propriétés de filtrage
        private string _filterAccountId;
        private string _filterCurrency;
        private string _filterCountry;
        private decimal? _filterMinAmount;
        private decimal? _filterMaxAmount;
        private DateTime? _filterFromDate;
        private DateTime? _filterToDate;
        private int? _filterAction;
        private int? _filterKPI;
        private string _filterStatus; // All, Matched, Unmatched
        private string _filterReconciliationNum;
        private string _filterRawLabel;
        private string _filterEventNum;
        private string _filterDwGuaranteeId;
        private string _filterDwCommissionId;
        // New: Selected IDs for referential ComboBox filters
        private int? _filterActionId;
        private int? _filterKpiId;
        private int? _filterIncidentTypeId;

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

        private void LoadMoreButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                LoadMorePage();
            }
            catch { }
        }

        private void PopulateReferentialOptions()
        {
            try
            {
                ActionOptions.Clear();
                KpiOptions.Clear();
                IncidentTypeOptions.Clear();

                var all = AllUserFields ?? Array.Empty<UserField>();

                foreach (var uf in all.Where(u => string.Equals(u.USR_Category, "Action", StringComparison.OrdinalIgnoreCase))
                                       .OrderBy(u => u.USR_FieldName))
                {
                    ActionOptions.Add(new OptionItem { Id = uf.USR_ID, Name = uf.USR_FieldName });
                }
                foreach (var uf in all.Where(u => string.Equals(u.USR_Category, "KPI", StringComparison.OrdinalIgnoreCase))
                                       .OrderBy(u => u.USR_FieldName))
                {
                    KpiOptions.Add(new OptionItem { Id = uf.USR_ID, Name = uf.USR_FieldName });
                }
                foreach (var uf in all.Where(u =>
                                                string.Equals(u.USR_Category, "Incident Type", StringComparison.OrdinalIgnoreCase)
                                                || string.Equals(u.USR_Category, "INC", StringComparison.OrdinalIgnoreCase))
                                       .OrderBy(u => u.USR_FieldName))
                {
                    IncidentTypeOptions.Add(new OptionItem { Id = uf.USR_ID, Name = uf.USR_FieldName });
                }
            }
            catch { }
        }

        /// <summary>
        /// Public helper to apply a saved grid layout from its JSON representation.
        /// This is used by the host page when instantiating a new view from a saved preset.
        /// </summary>
        public void ApplyLayoutJson(string layoutJson)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(layoutJson)) return;
                var layout = JsonSerializer.Deserialize<GridLayout>(layoutJson);
                ApplyGridLayout(layout);
            }
            catch { /* ignore invalid layout JSON */ }
        }

        #region Column Layout Capture/Apply and Header Menu

        private class ColumnSetting
        {
            public string Header { get; set; }
            public string SortMemberPath { get; set; }
            public int DisplayIndex { get; set; }
            public double? WidthValue { get; set; } // store as pixel width when possible
            public string WidthType { get; set; } // Auto, SizeToCells, SizeToHeader, Pixel
            public bool Visible { get; set; }
        }

        private class GridLayout
        {
            public List<ColumnSetting> Columns { get; set; } = new List<ColumnSetting>();
            public List<SortDescriptor> Sorts { get; set; } = new List<SortDescriptor>();
        }

        private class SortDescriptor
        {
            public string Member { get; set; }
            public ListSortDirection Direction { get; set; }
        }

        private GridLayout CaptureGridLayout()
        {
            var layout = new GridLayout();
            try
            {
                var dg = this.FindName("ResultsDataGrid") as DataGrid;
                if (dg == null) return layout;

                foreach (var col in dg.Columns)
                {
                    var st = new ColumnSetting
                    {
                        Header = Convert.ToString(col.Header),
                        SortMemberPath = col.SortMemberPath,
                        DisplayIndex = col.DisplayIndex,
                        Visible = col.Visibility == Visibility.Visible
                    };
                    if (col.Width.IsAbsolute)
                    {
                        st.WidthType = "Pixel";
                        st.WidthValue = col.Width.Value;
                    }
                    else if (col.Width.IsAuto)
                    {
                        st.WidthType = "Auto";
                    }
                    else if (col.Width.UnitType == DataGridLengthUnitType.SizeToCells)
                    {
                        st.WidthType = "SizeToCells";
                    }
                    else if (col.Width.UnitType == DataGridLengthUnitType.SizeToHeader)
                    {
                        st.WidthType = "SizeToHeader";
                    }
                    layout.Columns.Add(st);
                }

                var view = CollectionViewSource.GetDefaultView((this as UserControl).DataContext == this ? ViewData : dg.ItemsSource) as ICollectionView;
                if (view != null)
                {
                    foreach (var sd in view.SortDescriptions)
                    {
                        layout.Sorts.Add(new SortDescriptor { Member = sd.PropertyName, Direction = sd.Direction });
                    }
                }
            }
            catch { }
            return layout;
        }

        private void ApplyGridLayout(GridLayout layout)
        {
            try
            {
                if (layout == null) return;
                var dg = this.FindName("ResultsDataGrid") as DataGrid;
                if (dg == null) return;

                // Map by header text
                foreach (var setting in layout.Columns)
                {
                    var col = dg.Columns.FirstOrDefault(c => string.Equals(Convert.ToString(c.Header), setting.Header, StringComparison.OrdinalIgnoreCase));
                    if (col == null) continue;
                    try { col.DisplayIndex = Math.Max(0, Math.Min(setting.DisplayIndex, dg.Columns.Count - 1)); } catch { }
                    try { col.Visibility = setting.Visible ? Visibility.Visible : Visibility.Collapsed; } catch { }
                    try
                    {
                        switch (setting.WidthType)
                        {
                            case "Pixel":
                                if (setting.WidthValue.HasValue && setting.WidthValue.Value > 0)
                                    col.Width = new DataGridLength(setting.WidthValue.Value);
                                break;
                            case "Auto":
                                col.Width = DataGridLength.Auto;
                                break;
                            case "SizeToCells":
                                col.Width = new DataGridLength(1, DataGridLengthUnitType.SizeToCells);
                                break;
                            case "SizeToHeader":
                                col.Width = new DataGridLength(1, DataGridLengthUnitType.SizeToHeader);
                                break;
                        }
                    }
                    catch { }
                }

                // Apply sorting
                var view = CollectionViewSource.GetDefaultView(dg.ItemsSource);
                if (view != null)
                {
                    using (view.DeferRefresh())
                    {
                        view.SortDescriptions.Clear();
                        foreach (var s in layout.Sorts)
                        {
                            if (!string.IsNullOrWhiteSpace(s.Member))
                                view.SortDescriptions.Add(new SortDescription(s.Member, s.Direction));
                        }
                    }
                }
            }
            catch { }
        }

        private void ResultsDataGrid_PreviewMouseRightButtonUp(object sender, MouseButtonEventArgs e)
        {
            try
            {
                var dg = sender as DataGrid;
                if (dg == null) return;
                var dep = e.OriginalSource as DependencyObject;
                while (dep != null && !(dep is DataGridColumnHeader))
                {
                    dep = VisualTreeHelper.GetParent(dep);
                }
                if (dep is DataGridColumnHeader header)
                {
                    e.Handled = true;
                    var cm = new ContextMenu();
                    foreach (var col in dg.Columns)
                    {
                        var mi = new MenuItem { Header = Convert.ToString(col.Header), IsCheckable = true, IsChecked = col.Visibility == Visibility.Visible };
                        mi.Click += (s, ev) =>
                        {
                            try
                            {
                                col.Visibility = mi.IsChecked ? Visibility.Visible : Visibility.Collapsed;
                            }
                            catch { }
                        };
                        cm.Items.Add(mi);
                    }
                    cm.IsOpen = true;
                }
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

                var dataGrid = FindParent<DataGrid>(cell);
                if (dataGrid == null) return;
                // Ensure single selection by selecting the owning row item directly
                var row = FindParent<DataGridRow>(cell);
                if (row != null)
                {
                    dataGrid.SelectedItem = row.Item;
                }

                dataGrid.BeginEdit(e);
                e.Handled = true;
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
                    _resultsScrollViewer = FindDescendant<ScrollViewer>(dg);
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

        #region Save/Load View Handlers

        private async void SaveView_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if (_reconciliationService == null)
                {
                    ShowError("Service not available");
                    return;
                }

                var name = Interaction.InputBox("View name to save:", "Save View", _currentView ?? "My View");
                if (string.IsNullOrWhiteSpace(name)) return;

                // Build SQL snapshot with JSON preset comment
                var preset = GetCurrentFilterPreset();
                var wherePart = _backendFilterSql; // already pure WHERE or null
                var sqlWithJson = BuildSqlWithJsonComment(preset, wherePart);

                // Capture grid layout
                var layout = CaptureGridLayout();
                var layoutJson = JsonSerializer.Serialize(layout);

                var id = await _reconciliationService.UpsertUserFieldsPreferenceAsync(name, sqlWithJson, layoutJson);
                if (id > 0)
                {
                    CurrentView = name;
                    UpdateStatusInfo($"View '{name}' saved.");
                }
                else
                {
                    ShowError("Failed to save view.");
                }
            }
            catch (Exception ex)
            {
                ShowError($"Error saving view: {ex.Message}");
            }
        }

        #endregion

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

                    var clearItem = new MenuItem { Header = "Clear", Tag = category, CommandParameter = null };
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
            }
            catch
            {
                // En cas d'erreur de parsing, fallback sur le SQL brut
                _backendFilterSql = StripAccountFromWhere(sql);
            }
        }

        private void InitializeFilterDebounce()
        {
            _filterDebounceTimer = new DispatcherTimer
            {
                Interval = TimeSpan.FromMilliseconds(300)
            };
            _filterDebounceTimer.Tick += (s, e) =>
            {
                _filterDebounceTimer.Stop();
                try { ApplyFilters(); } catch { }
            };
        }

        private void ScheduleApplyFiltersDebounced()
        {
            try
            {
                _filterDebounceTimer.Stop();
                _filterDebounceTimer.Start();
            }
            catch { }
        }

        // ---- Generic saved filter snapshot support ----
        private class FilterPreset
        {
            public string AccountId { get; set; }
            public string Currency { get; set; }
            public string Country { get; set; }
            public decimal? MinAmount { get; set; }
            public decimal? MaxAmount { get; set; }
            public DateTime? FromDate { get; set; }
            public DateTime? ToDate { get; set; }
            public int? Action { get; set; }
            public int? KPI { get; set; }
            public int? IncidentType { get; set; }
            public string Status { get; set; }
            public string ReconciliationNum { get; set; }
            public string RawLabel { get; set; }
            public string EventNum { get; set; }
            public string DwGuaranteeId { get; set; }
            public string DwCommissionId { get; set; }
        }

        private FilterPreset GetCurrentFilterPreset()
        {
            return new FilterPreset
            {
                AccountId = _filterAccountId,
                Currency = _filterCurrency,
                Country = _filterCountry,
                MinAmount = _filterMinAmount,
                MaxAmount = _filterMaxAmount,
                FromDate = _filterFromDate,
                ToDate = _filterToDate,
                Action = _filterActionId ?? _filterAction,
                KPI = _filterKpiId ?? _filterKPI,
                IncidentType = _filterIncidentTypeId,
                Status = _filterStatus,
                ReconciliationNum = _filterReconciliationNum,
                RawLabel = _filterRawLabel,
                EventNum = _filterEventNum,
                DwGuaranteeId = _filterDwGuaranteeId,
                DwCommissionId = _filterDwCommissionId
            };
        }

        private void ApplyFilterPreset(FilterPreset p)
        {
            if (p == null) return;
            try
            {
                // Ne pas appliquer le compte depuis un preset de vue/filtre (compte géré en dehors)
                // FilterAccountId = p.AccountId;
                FilterCurrency = p.Currency;
                _filterCountry = p.Country; // informational
                FilterMinAmount = p.MinAmount;
                FilterMaxAmount = p.MaxAmount;
                FilterFromDate = p.FromDate;
                FilterToDate = p.ToDate;
                // Prefer ID-based restore
                FilterActionId = p.Action;
                FilterKpiId = p.KPI;
                FilterIncidentTypeId = p.IncidentType;
                // Backward-compatibility: also set string fields for legacy UI pieces if any
                FilterAction = p.Action?.ToString();
                FilterKPI = p.KPI?.ToString();
                FilterStatus = p.Status;
                FilterReconciliationNum = p.ReconciliationNum;
                FilterRawLabel = p.RawLabel;
                FilterEventNum = p.EventNum;
                FilterDwGuaranteeId = p.DwGuaranteeId;
                FilterDwCommissionId = p.DwCommissionId;
            }
            catch { }
        }

        /// <summary>
        /// Removes any Account_ID = '...' predicate from a WHERE or full SQL fragment.
        /// Preserves other predicates and keeps/strips the WHERE keyword appropriately.
        /// </summary>
        private string StripAccountFromWhere(string whereOrSql)
        {
            if (string.IsNullOrWhiteSpace(whereOrSql)) return whereOrSql;
            try
            {
                var s = whereOrSql.Trim();
                var hasWhere = s.StartsWith("WHERE ", StringComparison.OrdinalIgnoreCase);
                if (hasWhere) s = s.Substring(6).Trim();

                // Regex for Account_ID with optional alias/brackets: [Alias.]?[Account_ID] = '...'
                var pred = new System.Text.RegularExpressions.Regex(@"(?i)(\b[\w\[\]]+\.)?\[?Account_ID\]?\s*=\s*'[^']*'");

                string RemovePredicate(string input)
                {
                    if (string.IsNullOrWhiteSpace(input)) return input;
                    var text = input;

                    // 1) Predicate at start followed by AND/OR
                    text = System.Text.RegularExpressions.Regex.Replace(text, @"^(\s*\(*\s*)" + pred + @"(\s*\)*\s*(AND|OR)\s*)", string.Empty, System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                    // 2) Predicate at end preceded by AND/OR
                    text = System.Text.RegularExpressions.Regex.Replace(text, @"(\s*(AND|OR)\s*\(*\s*)" + pred + @"(\s*\)*\s*)$", string.Empty, System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                    // 3) Predicate in the middle with AND neighbors -> collapse to single AND
                    text = System.Text.RegularExpressions.Regex.Replace(text, @"(\s*(AND|OR)\s*)" + pred + @"(\s*(AND|OR)\s*)", m => m.Groups[1].Value, System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                    // 4) Bare predicate alone
                    text = System.Text.RegularExpressions.Regex.Replace(text, @"^\s*" + pred + @"\s*$", string.Empty, System.Text.RegularExpressions.RegexOptions.IgnoreCase);

                    // Cleanup doubled spaces and trim parentheses
                    text = System.Text.RegularExpressions.Regex.Replace(text, @"\s{2,}", " ").Trim();
                    text = System.Text.RegularExpressions.Regex.Replace(text, @"^\((.*)\)$", "$1");
                    text = text.Trim();
                    return text;
                }

                var cleaned = RemovePredicate(s);
                if (string.IsNullOrWhiteSpace(cleaned)) return string.Empty;
                return hasWhere ? ("WHERE " + cleaned) : cleaned;
            }
            catch { return whereOrSql; }
        }

        private string BuildSqlWithJsonComment(FilterPreset preset, string whereClause)
        {
            try
            {
                var json = JsonSerializer.Serialize(preset, new JsonSerializerOptions { WriteIndented = false });
                // Keep compatibility: prepend JSON in a comment, followed by the WHERE text consumed by backend
                return $"/*JSON:{json}*/ " + (whereClause ?? string.Empty);
            }
            catch
            {
                return whereClause ?? string.Empty;
            }
        }

        private bool TryExtractPresetFromSql(string sql, out FilterPreset preset, out string pureWhere)
        {
            preset = null;
            pureWhere = sql ?? string.Empty;
            if (string.IsNullOrWhiteSpace(sql)) return false;

            try
            {
                var m = Regex.Match(sql, @"^/\*JSON:(.*?)\*/\s*(.*)$", RegexOptions.Singleline);
                if (m.Success)
                {
                    var json = m.Groups[1].Value;
                    pureWhere = m.Groups[2].Value?.Trim();
                    preset = JsonSerializer.Deserialize<FilterPreset>(json);
                    return preset != null;
                }
            }
            catch { }
            return false;
        }

        /// <summary>
        /// Constructeur par défaut
        /// </summary>
        public ReconciliationView()
        {
            InitializeComponent();
            InitializeData();
            DataContext = this;
            Loaded += ReconciliationView_Loaded;
            InitializeFilterDebounce();
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
                        dg.PreviewMouseRightButtonUp -= ResultsDataGrid_PreviewMouseRightButtonUp;
                        dg.PreviewMouseRightButtonUp += ResultsDataGrid_PreviewMouseRightButtonUp;
                        dg.CanUserSortColumns = true; // allow sorting on all columns (template ones have SortMemberPath in XAML)
                        TryHookResultsGridScroll(dg);
                    }
                }
                catch { }
            };
        }

        /// <summary>
        /// Initialise les données par défaut
        /// </summary>
        private void InitializeData()
        {
            _viewData = new ObservableCollection<ReconciliationViewData>();
            _allViewData = new List<ReconciliationViewData>();
        }

        /// <summary>
        /// Initialise les données depuis les services
        /// </summary>
        private async void InitializeFromServices()
        {
            try
            {
                if (_offlineFirstService != null)
                {
                    // Synchroniser avec la country courante
                    var currentCountry = _offlineFirstService.CurrentCountry;
                    if (currentCountry != null)
                    {
                        _currentCountryId = currentCountry.CNT_Id;
                        _filterCountry = currentCountry.CNT_Name;
                        // Mettre à jour l'entête Pivot/Receivable selon le référentiel pays
                        UpdateCountryPivotReceivableInfo();
                    }
                    // Référentiels: prévenir l'UI que UserFields/Country sont prêts
                    OnPropertyChanged(nameof(AllUserFields));
                    OnPropertyChanged(nameof(CurrentCountryObject));

                    // Peupler les options pour les ComboBox référentielles (Action/KPI/Incident Type)
                    PopulateReferentialOptions();
                }
                // Ne pas effectuer de chargement automatique ici; la page parente appliquera
                // les filtres et la mise en page, puis appellera explicitement Refresh().
            }
            catch (Exception ex)
            {
                // Log l'erreur si nécessaire
                System.Diagnostics.Debug.WriteLine($"Erreur lors de l'initialisation des services: {ex.Message}");
            }
        }

        private void ReconciliationView_Loaded(object sender, RoutedEventArgs e)
        {
            if (_initialLoaded) return;
            if (!string.IsNullOrEmpty(_currentCountryId))
            {
                // Rafraîchir l'affichage Pivot/Receivable à l'ouverture
                UpdateCountryPivotReceivableInfo();
                // Marquer comme initialisé pour éviter les chargements implicites multiples.
                // La page hôte déclenchera Refresh() après avoir appliqué les filtres/présélections.
                _initialLoaded = true;
            }
        }

        public ObservableCollection<ReconciliationViewData> ViewData
        {
            get => _viewData;
            set
            {
                _viewData = value;
                OnPropertyChanged(nameof(ViewData));
            }
        }

        // Expose referentials for XAML bindings (ComboBox items/label resolution)
        public IReadOnlyList<UserField> AllUserFields => _offlineFirstService?.UserFields;
        public Country CurrentCountryObject => _offlineFirstService?.CurrentCountry;

        // Options for referential ComboBoxes
        public class OptionItem
        {
            public int Id { get; set; }
            public string Name { get; set; }
        }
        private ObservableCollection<OptionItem> _actionOptions = new ObservableCollection<OptionItem>();
        private ObservableCollection<OptionItem> _kpiOptions = new ObservableCollection<OptionItem>();
        private ObservableCollection<OptionItem> _incidentTypeOptions = new ObservableCollection<OptionItem>();
        public ObservableCollection<OptionItem> ActionOptions { get => _actionOptions; private set { _actionOptions = value; OnPropertyChanged(nameof(ActionOptions)); } }
        public ObservableCollection<OptionItem> KpiOptions { get => _kpiOptions; private set { _kpiOptions = value; OnPropertyChanged(nameof(KpiOptions)); } }
        public ObservableCollection<OptionItem> IncidentTypeOptions { get => _incidentTypeOptions; private set { _incidentTypeOptions = value; OnPropertyChanged(nameof(IncidentTypeOptions)); } }

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

        #endregion

        #region Row ContextMenu (Quick Set: Action/KPI/Incident)

        private IEnumerable<UserField> GetUserFieldOptionsForRow(string category, ReconciliationViewData row)
        {
            try
            {
                var all = AllUserFields ?? Array.Empty<UserField>();
                var country = CurrentCountryObject;
                if (country == null) return Array.Empty<UserField>();

                bool isPivot = string.Equals(row?.Account_ID?.Trim(), country.CNT_AmbrePivot?.Trim(), StringComparison.OrdinalIgnoreCase);
                bool isReceivable = string.Equals(row?.Account_ID?.Trim(), country.CNT_AmbreReceivable?.Trim(), StringComparison.OrdinalIgnoreCase);

                // Category mapping: handle synonyms (e.g., Incident Type vs INC)
                bool incident = string.Equals(category, "Incident Type", StringComparison.OrdinalIgnoreCase)
                                 || string.Equals(category, "INC", StringComparison.OrdinalIgnoreCase);
                IEnumerable<UserField> query = incident
                    ? all.Where(u => string.Equals(u.USR_Category, "Incident Type", StringComparison.OrdinalIgnoreCase)
                                     || string.Equals(u.USR_Category, "INC", StringComparison.OrdinalIgnoreCase))
                    : all.Where(u => string.Equals(u.USR_Category, category, StringComparison.OrdinalIgnoreCase));
                if (isPivot)
                    query = query.Where(u => u.USR_Pivot);
                else if (isReceivable)
                    query = query.Where(u => u.USR_Receivable);
                else
                    return Array.Empty<UserField>();

                return query.OrderBy(u => u.USR_FieldName).ToList();
            }
            catch { return Array.Empty<UserField>(); }
        }

        // Populate the context menu items at open time to ensure correct DataContext
        private void RowContextMenu_Opened(object sender, RoutedEventArgs e)
        {
            try
            {
                var cm = sender as ContextMenu;
                if (cm == null) return;
                var fe = cm.PlacementTarget as FrameworkElement;
                var rowData = fe?.DataContext as ReconciliationViewData;
                if (rowData == null) return;

                // Resolve the root submenus
                MenuItem actionRoot = cm.Items.OfType<MenuItem>().FirstOrDefault(mi => (mi.Tag as string) == "Action");
                MenuItem kpiRoot = cm.Items.OfType<MenuItem>().FirstOrDefault(mi => (mi.Tag as string) == "KPI");
                MenuItem incRoot = cm.Items.OfType<MenuItem>().FirstOrDefault(mi => (mi.Tag as string) == "Incident Type");

                void Populate(MenuItem root, string category)
                {
                    if (root == null) return;
                    root.Items.Clear();

                    var options = GetUserFieldOptionsForRow(category, rowData).ToList();

                    // Clear option (always present)
                    var clearItem = new MenuItem { Header = "Clear", Tag = category, CommandParameter = null };
                    clearItem.Click += QuickSetUserFieldMenuItem_Click;
                    // Disable Clear if already empty
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
                            IsCheckable = true,
                            IsChecked = (string.Equals(category, "Action", StringComparison.OrdinalIgnoreCase) && rowData.Action == opt.USR_ID)
                                        || (string.Equals(category, "KPI", StringComparison.OrdinalIgnoreCase) && rowData.KPI == opt.USR_ID)
                                        || (string.Equals(category, "Incident Type", StringComparison.OrdinalIgnoreCase) && rowData.IncidentType == opt.USR_ID)
                        };
                        mi.Click += QuickSetUserFieldMenuItem_Click;
                        root.Items.Add(mi);
                    }

                    // Disable the root if there are no applicable options and value is empty
                    root.IsEnabled = options.Any() || hasValue; // keep enabled if Clear is relevant
                }

                Populate(actionRoot, "Action");
                Populate(kpiRoot, "KPI");
                Populate(incRoot, "Incident Type");
            }
            catch { }
        }

        private async void QuickSetUserFieldMenuItem_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var mi = sender as MenuItem;
                if (mi == null) return;

                // Find the owning ContextMenu and its row
                var cm = FindParent<ContextMenu>(mi);
                var fe = cm?.PlacementTarget as FrameworkElement;
                var row = fe?.DataContext as ReconciliationViewData;
                if (row == null) return;

                var category = mi.Tag as string;
                int? newId = null;
                if (mi.CommandParameter != null)
                {
                    if (mi.CommandParameter is int id)
                        newId = id;
                    else if (int.TryParse(mi.CommandParameter.ToString(), out var parsed))
                        newId = parsed;
                }

                if (_reconciliationService == null) return;
                var reco = await _reconciliationService.GetOrCreateReconciliationAsync(row.ID);

                if (string.Equals(category, "Action", StringComparison.OrdinalIgnoreCase))
                {
                    // Confirm clear
                    if (newId == null && row.Action.HasValue)
                    {
                        if (MessageBox.Show("Clear Action?", "Confirm", MessageBoxButton.YesNo, MessageBoxImage.Question) != MessageBoxResult.Yes) return;
                    }
                    row.Action = newId; reco.Action = newId;
                }
                else if (string.Equals(category, "KPI", StringComparison.OrdinalIgnoreCase))
                {
                    if (newId == null && row.KPI.HasValue)
                    {
                        if (MessageBox.Show("Clear KPI?", "Confirm", MessageBoxButton.YesNo, MessageBoxImage.Question) != MessageBoxResult.Yes) return;
                    }
                    row.KPI = newId; reco.KPI = newId;
                }
                else if (string.Equals(category, "Incident Type", StringComparison.OrdinalIgnoreCase) || string.Equals(category, "IncidentType", StringComparison.OrdinalIgnoreCase))
                {
                    if (newId == null && row.IncidentType.HasValue)
                    {
                        if (MessageBox.Show("Clear Incident Type?", "Confirm", MessageBoxButton.YesNo, MessageBoxImage.Question) != MessageBoxResult.Yes) return;
                    }
                    row.IncidentType = newId; reco.IncidentType = newId;
                }
                else return;

                await _reconciliationService.SaveReconciliationAsync(reco);

                // background sync
                try
                {
                    if (_offlineFirstService != null && _offlineFirstService.IsInitialized && _offlineFirstService.IsNetworkSyncAvailable)
                    {
                        _ = Task.Run(async () => { try { await _offlineFirstService.SynchronizeData(); } catch { } });
                    }
                }
                catch { }
            }
            catch (Exception ex)
            {
                ShowError($"Save error: {ex.Message}");
            }
        }

        private static T FindParent<T>(DependencyObject child) where T : DependencyObject
        {
            DependencyObject parentObject = child is FrameworkElement fe ? fe.Parent ?? fe.TemplatedParent : null;
            if (parentObject == null && child is FrameworkElement fe2)
                parentObject = System.Windows.Media.VisualTreeHelper.GetParent(fe2);
            while (parentObject != null && !(parentObject is T))
            {
                parentObject = System.Windows.Media.VisualTreeHelper.GetParent(parentObject);
            }
            return parentObject as T;
        }

        #endregion

        #region IRefreshable Implementation

        public bool CanRefresh => _canRefresh && !string.IsNullOrEmpty(_currentCountryId);

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
                // Toujours rafraîchir l'info Pivot/Receivable avant chargement
                UpdateCountryPivotReceivableInfo();
                RefreshStarted?.Invoke(this, EventArgs.Empty);
                var sw = Stopwatch.StartNew();
                await LoadReconciliationDataAsync();
                sw.Stop();
                try { LogPerf("RefreshAsync", $"country={_currentCountryId} | totalMs={sw.ElapsedMilliseconds}"); } catch { }
            }
            finally
            {
                RefreshCompleted?.Invoke(this, EventArgs.Empty);
            }
        }

        #endregion

        #region Data Loading

        /// <summary>
        /// Charge les données initiales
        /// </summary>
        private async void LoadInitialData()
        {
            try
            {
                // Charger les données de démonstration ou les dernières données disponibles
                await LoadReconciliationDataAsync();
            }
            catch (Exception ex)
            {
                ShowError($"Erreur lors du chargement initial: {ex.Message}");
            }
        }

        /// <summary>
        /// Charge les données de réconciliation depuis le service
        /// </summary>
        private async Task LoadReconciliationDataAsync()
        {
            try
            {
                IsLoading = true;
                UpdateStatusInfo("Chargement des données...");
                var swTotal = Stopwatch.StartNew();
                var swDb = Stopwatch.StartNew();
                List<ReconciliationViewData> viewList;
                bool usedPreloaded = false;
                if (_preloadedAllData != null)
                {
                    // Utiliser les données préchargées (ne pas toucher au service)
                    viewList = _preloadedAllData.ToList();
                    usedPreloaded = true;
                    swDb.Stop();
                }
                else
                {
                    // Charger la vue combinée avec filtre backend éventuel
                    viewList = await _reconciliationService.GetReconciliationViewAsync(_currentCountryId, _backendFilterSql);
                    swDb.Stop();
                }
                int totalRows = viewList?.Count ?? 0;

                // Stocker toutes les données pour le filtrage
                _allViewData = viewList ?? new List<ReconciliationViewData>();

                // Appliquer les filtres courants (ex: compte/Status) si déjà définis par la page parente
                var swFilter = Stopwatch.StartNew();
                ApplyFilters();
                swFilter.Stop();

                swTotal.Stop();
                UpdateStatusInfo($"{ViewData?.Count ?? 0} lignes chargées");
                try
                {
                    LogPerf(
                        "LoadReconciliationData",
                        $"country={_currentCountryId} | backendFilterLen={( _backendFilterSql?.Length ?? 0)} | source={(usedPreloaded ? "preloaded" : "service")} | dbMs={swDb.ElapsedMilliseconds} | filterMs={swFilter.ElapsedMilliseconds} | totalMs={swTotal.ElapsedMilliseconds} | totalRows={totalRows} | displayed={ViewData?.Count ?? 0}"
                    );
                }
                catch { }
            }
            catch (Exception ex)
            {
                ShowError($"Erreur lors du chargement des données: {ex.Message}");
                UpdateStatusInfo("Erreur de chargement");
            }
            finally
            {
                IsLoading = false;
            }
        }

        /// <summary>
        /// Fournit des données préchargées par la page. Bypass le fetch service.
        /// </summary>
        public void InitializeWithPreloadedData(IReadOnlyList<ReconciliationViewData> allData, string backendFilterSql)
        {
            try
            {
                _preloadedAllData = allData ?? Array.Empty<ReconciliationViewData>();
                _backendFilterSql = backendFilterSql; // garder la trace du filtre appliqué côté service
            }
            catch { }
        }

        /// <summary>
        /// Met à jour l'affichage des filtres externes provenant de ReconciliationPage
        /// </summary>
        public void UpdateExternalFilters(string account, string status)
        {
            try
            {
                var acc = string.IsNullOrWhiteSpace(account) ? "All" : account;
                var stat = string.IsNullOrWhiteSpace(status) ? "All" : status; // Expected: All/Active/Deleted
                var tb = this.FindName("AccountInfoText") as TextBlock;
                if (tb != null)
                {
                    // Avoid displaying internal account IDs in the header (e.g., "Pivot (12345)")
                    var friendlyAcc = acc;
                    var idx = acc?.LastIndexOf('(') ?? -1;
                    if (idx > 0)
                    {
                        friendlyAcc = acc.Substring(0, idx).Trim();
                    }
                    tb.Text = $"Account: {friendlyAcc} | Status: {stat}";
                }

                // Appliquer sur les filtres internes pour la vue
                _filterAccountId = string.Equals(acc, "All", StringComparison.OrdinalIgnoreCase) ? null : ResolveAccountIdForFilter(acc);
                // Status = All/Active/Deleted (use IsDeleted)
                _filterStatus = stat;
                ApplyFilters();
                // Mettre à jour le titre pour refléter le nouvel état
                UpdateViewTitle();
            }
            catch { /* best effort UI update */ }
        }

        /// <summary>
        /// Met à jour le sous-titre affichant Pivot/Receivable depuis le référentiel du pays sélectionné
        /// </summary>
        private void UpdateCountryPivotReceivableInfo()
        {
            try
            {
                var txt = this.FindName("CountryPivotReceivableText") as TextBlock;
                var cc = _offlineFirstService?.CurrentCountry;
                if (txt == null) return;

                if (cc == null)
                {
                    txt.Text = string.Empty;
                    return;
                }

                var pivot = string.IsNullOrWhiteSpace(cc.CNT_AmbrePivot) ? "-" : cc.CNT_AmbrePivot;
                var recv = string.IsNullOrWhiteSpace(cc.CNT_AmbreReceivable) ? "-" : cc.CNT_AmbreReceivable;
                txt.Text = $"Pivot: {pivot} | Receivable: {recv}";
            }
            catch { }
        }

        /// <summary>
        /// Résout l'ID de compte réel à partir d'un libellé d'affichage (ex: "Pivot (ID)" ou "Pivot")
        /// </summary>
        private string ResolveAccountIdForFilter(string display)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(display)) return display;

                // If format "Label (ID)", extract inner ID
                var open = display.LastIndexOf('(');
                var close = display.LastIndexOf(')');
                if (open >= 0 && close > open)
                {
                    var inner = display.Substring(open + 1, close - open - 1).Trim();
                    if (!string.IsNullOrWhiteSpace(inner)) return inner;
                }

                // Map bare Pivot/Receivable to repository values
                var country = _offlineFirstService?.CurrentCountry;
                if (country != null)
                {
                    if (string.Equals(display, "Pivot", StringComparison.OrdinalIgnoreCase))
                        return country.CNT_AmbrePivot;
                    if (string.Equals(display, "Receivable", StringComparison.OrdinalIgnoreCase))
                        return country.CNT_AmbreReceivable;
                }

                // Fallback to raw
                return display;
            }
            catch { return display; }
        }

        /// <summary>
        /// Synchronise le pays courant depuis le service et rafraîchit la vue et l'entête.
        /// Appelé par la page lorsque la sélection de pays change.
        /// </summary>
        public void SyncCountryFromService()
        {
            try
            {
                var cc = _offlineFirstService?.CurrentCountry;
                _currentCountryId = cc?.CNT_Id;
                _filterCountry = cc?.CNT_Name;
                UpdateCountryPivotReceivableInfo();
                Refresh();
            }
            catch { }
        }
        
        #endregion
        

        private void CloseViewButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                // Si hébergé dans une fenêtre popup, fermer la fenêtre propriétaire
                var wnd = Window.GetWindow(this);
                if (wnd != null && wnd.Owner != null)
                {
                    wnd.Close();
                    return;
                }

                // Sinon, demander à la page parente de supprimer cette vue
                CloseRequested?.Invoke(this, EventArgs.Empty);
            }
            catch { }
        }

        private async void BulkEditButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                // Commit any pending cell/row edits before bulk applying
                try
                {
                    ResultsDataGrid?.CommitEdit(DataGridEditingUnit.Cell, true);
                    ResultsDataGrid?.CommitEdit(DataGridEditingUnit.Row, true);
                }
                catch { }

                var selected = ResultsDataGrid?.SelectedItems?.OfType<ReconciliationViewData>()?.ToList() ?? new List<ReconciliationViewData>();
                if (selected.Count == 0)
                {
                    ShowError("No rows selected for bulk edit.");
                    return;
                }

                // Open bulk edit dialog with referential options
                var dlg = new BulkEditWindow(ActionOptions, KpiOptions, IncidentTypeOptions);
                var owner = Window.GetWindow(this);
                if (owner != null) dlg.Owner = owner;
                var result = dlg.ShowDialog();
                if (result != true) return;

                var vm = dlg.ViewModel;
                if (vm == null) return;

                // Confirm bulk clears if any field is applied with null value
                if (vm.ApplyAction && vm.SelectedActionId == null)
                {
                    if (MessageBox.Show($"Clear Action for {selected.Count} selected rows?", "Confirm bulk clear", MessageBoxButton.YesNo, MessageBoxImage.Question) != MessageBoxResult.Yes)
                        return;
                }
                if (vm.ApplyKpi && vm.SelectedKpiId == null)
                {
                    if (MessageBox.Show($"Clear KPI for {selected.Count} selected rows?", "Confirm bulk clear", MessageBoxButton.YesNo, MessageBoxImage.Question) != MessageBoxResult.Yes)
                        return;
                }
                if (vm.ApplyIncidentType && vm.SelectedIncidentTypeId == null)
                {
                    if (MessageBox.Show($"Clear Incident Type for {selected.Count} selected rows?", "Confirm bulk clear", MessageBoxButton.YesNo, MessageBoxImage.Question) != MessageBoxResult.Yes)
                        return;
                }

                if (_reconciliationService == null) return;

                foreach (var row in selected)
                {
                    // Load and update only the selected user fields
                    var reco = await _reconciliationService.GetOrCreateReconciliationAsync(row.ID);

                    if (vm.ApplyAction)
                    {
                        row.Action = vm.SelectedActionId;
                        reco.Action = vm.SelectedActionId;
                    }
                    if (vm.ApplyKpi)
                    {
                        row.KPI = vm.SelectedKpiId;
                        reco.KPI = vm.SelectedKpiId;
                    }
                    if (vm.ApplyIncidentType)
                    {
                        row.IncidentType = vm.SelectedIncidentTypeId;
                        reco.IncidentType = vm.SelectedIncidentTypeId;
                    }

                    await _reconciliationService.SaveReconciliationAsync(reco);
                }

                // Rafraîchir la vue pour refléter immédiatement les modifications du bulk edit
                await RefreshAsync();

                // Fire-and-forget background sync similar to other save handlers
                try
                {
                    if (_offlineFirstService != null && _offlineFirstService.IsInitialized && _offlineFirstService.IsNetworkSyncAvailable)
                    {
                        _ = Task.Run(async () =>
                        {
                            try { await _offlineFirstService.SynchronizeData(); }
                            catch { /* ignore background sync errors */ }
                        });
                    }
                }
                catch { }
            }
            catch (Exception ex)
            {
                ShowError($"Bulk edit error: {ex.Message}");
            }
        }

        private void Export_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var dlg = new Microsoft.Win32.SaveFileDialog
                {
                    Title = "Export",
                    Filter = "CSV Files (*.csv)|*.csv|Excel Workbook (*.xlsx)|*.xlsx",
                    FileName = $"reconciliation_export_{DateTime.Now:yyyyMMdd_HHmmss}"
                };
                if (dlg.ShowDialog() != true) return;

                // Export all filtered rows (not only currently loaded page)
                // Coalesce list sources first to avoid mixed-type '??' between List<> and ObservableCollection<>
                var items = (_filteredData ?? _allViewData)?.ToList()
                           ?? ViewData?.ToList()
                           ?? new List<ReconciliationViewData>();
                if (items.Count == 0)
                {
                    ShowError("Aucune ligne à exporter.");
                    return;
                }

                // Build headers and value accessors from DataGrid columns (visible order)
                var columns = ResultsDataGrid?.Columns?.Where(c => c.Visibility == Visibility.Visible).ToList() ?? new List<DataGridColumn>();
                if (columns.Count == 0)
                {
                    ShowError("Aucune colonne visible à exporter.");
                    return;
                }

                var headers = columns.Select(c => (c.Header ?? string.Empty).ToString()).ToList();

                string path = dlg.FileName;
                string ext = System.IO.Path.GetExtension(path)?.ToLowerInvariant();
                if (string.IsNullOrEmpty(ext))
                {
                    // Infer from selected filter index (0=csv,1=xlsx)
                    ext = dlg.FilterIndex == 2 ? ".xlsx" : ".csv";
                    path += ext;
                }

                if (ext == ".xlsx")
                {
                    ExportToExcel(path, headers, columns, items);
                }
                else
                {
                    ExportToCsv(path, headers, columns, items);
                }

                UpdateStatusInfo($"Exported {items.Count} rows to {path}");
                LogAction("Export", $"{items.Count} rows to {path}");
            }
            catch (Exception ex)
            {
                ShowError($"Erreur export: {ex.Message}");
            }
        }

        private void ResetFilter_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                // Reset all bound filter properties via their setters to raise PropertyChanged and debounce
                FilterAccountId = null;
                FilterCurrency = null;
                FilterCountry = null;
                FilterFromDate = null;
                FilterToDate = null;
                FilterDeletedDate = null;
                FilterMinAmount = null;
                FilterMaxAmount = null;
                FilterReconciliationNum = null;
                FilterRawLabel = null;
                FilterEventNum = null;
                FilterDwGuaranteeId = null;
                FilterDwCommissionId = null;
                FilterStatus = null;

                // String-backed combo filters
                FilterGuaranteeType = null;
                FilterTransactionType = null;
                FilterGuaranteeStatus = null;
                FilterCategory = null;
                FilterAction = null;
                FilterKPI = null;
                FilterIncidentType = null;

                // ID-backed selections
                FilterActionId = null;
                FilterKpiId = null;
                FilterIncidentTypeId = null;

                // Apply immediately and update the title/status
                ApplyFilters();
                UpdateViewTitle();
            }
            catch { }
        }

        private void ExportToCsv(string filePath, List<string> headers, List<DataGridColumn> columns, List<ReconciliationViewData> items)
        {
            using (var writer = new System.IO.StreamWriter(filePath, false, Encoding.UTF8))
            {
                string Escape(object v)
                {
                    if (v == null) return string.Empty;
                    var s = v.ToString();
                    return "\"" + s.Replace("\"", "\"\"") + "\"";
                }

                // headers
                writer.WriteLine(string.Join(",", headers.Select(h => Escape(h))));

                foreach (var item in items)
                {
                    var values = columns.Select(col => Escape(GetColumnValue(col, item)));
                    writer.WriteLine(string.Join(",", values));
                }
            }
        }

        private void ExportToExcel(string filePath, List<string> headers, List<DataGridColumn> columns, List<ReconciliationViewData> items)
        {
            Microsoft.Office.Interop.Excel.Application app = null;
            Microsoft.Office.Interop.Excel.Workbook wb = null;
            Microsoft.Office.Interop.Excel.Worksheet ws = null;
            try
            {
                app = new Microsoft.Office.Interop.Excel.Application { Visible = false, DisplayAlerts = false };
                wb = app.Workbooks.Add();
                ws = wb.ActiveSheet as Microsoft.Office.Interop.Excel.Worksheet;

                // headers
                for (int c = 0; c < headers.Count; c++)
                {
                    ws.Cells[1, c + 1] = headers[c];
                }

                // rows
                for (int r = 0; r < items.Count; r++)
                {
                    var item = items[r];
                    for (int c = 0; c < columns.Count; c++)
                    {
                        var val = GetColumnValue(columns[c], item);
                        ws.Cells[r + 2, c + 1] = val;
                    }
                }

                // Autofit
                ws.Columns.AutoFit();
                wb.SaveAs(filePath, Microsoft.Office.Interop.Excel.XlFileFormat.xlOpenXMLWorkbook);
            }
            finally
            {
                if (wb != null)
                {
                    wb.Close(false);
                    System.Runtime.InteropServices.Marshal.ReleaseComObject(wb);
                    wb = null;
                }
                if (ws != null)
                {
                    System.Runtime.InteropServices.Marshal.ReleaseComObject(ws);
                    ws = null;
                }
                if (app != null)
                {
                    app.Quit();
                    System.Runtime.InteropServices.Marshal.ReleaseComObject(app);
                    app = null;
                }
                System.GC.Collect();
                System.GC.WaitForPendingFinalizers();
            }
        }

        private string GetColumnValue(DataGridColumn column, ReconciliationViewData item)
        {
            try
            {
                // Support DataGridTextColumn and DataGridCheckBoxColumn bound to a property
                string bindingPath = null;
                if (column is DataGridBoundColumn)
                {
                    var b = ((DataGridBoundColumn)column).Binding as System.Windows.Data.Binding;
                    bindingPath = b?.Path?.Path;
                }
                else if (column is DataGridCheckBoxColumn)
                {
                    var b = ((DataGridCheckBoxColumn)column).Binding as System.Windows.Data.Binding;
                    bindingPath = b?.Path?.Path;
                }

                if (string.IsNullOrWhiteSpace(bindingPath))
                {
                    return string.Empty; // unbound or template column not supported
                }

                var prop = item.GetType().GetProperty(bindingPath);
                if (prop == null) return string.Empty;
                var raw = prop.GetValue(item, null);
                if (raw == null) return string.Empty;

                // Basic formatting similar to grid
                if (raw is DateTime dt)
                    return dt.ToString("yyyy-MM-dd");
                if (raw is bool bval)
                    return bval ? "True" : "False";
                if (raw is decimal dec)
                    return dec.ToString("N2");
                if (raw is double dbl)
                    return dbl.ToString("N2");

                return raw.ToString();
            }
            catch
            {
                return string.Empty;
            }
        }

        #region Bound Filter Properties

        public string FilterAccountId { get => _filterAccountId; set { _filterAccountId = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterAccountId)); ScheduleApplyFiltersDebounced(); } }
        public string FilterCurrency { get => _filterCurrency; set { _filterCurrency = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterCurrency)); ScheduleApplyFiltersDebounced(); } }
        public string FilterCountry { get => _filterCountry; set { _filterCountry = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterCountry)); ScheduleApplyFiltersDebounced(); } }
        public DateTime? FilterFromDate { get => _filterFromDate; set { _filterFromDate = value; OnPropertyChanged(nameof(FilterFromDate)); ScheduleApplyFiltersDebounced(); } }
        public DateTime? FilterToDate { get => _filterToDate; set { _filterToDate = value; OnPropertyChanged(nameof(FilterToDate)); ScheduleApplyFiltersDebounced(); } }
        private DateTime? _filterDeletedDate;
        public DateTime? FilterDeletedDate { get => _filterDeletedDate; set { _filterDeletedDate = value; OnPropertyChanged(nameof(FilterDeletedDate)); ScheduleApplyFiltersDebounced(); } }
        public decimal? FilterMinAmount { get => _filterMinAmount; set { _filterMinAmount = value; OnPropertyChanged(nameof(FilterMinAmount)); ScheduleApplyFiltersDebounced(); } }
        public decimal? FilterMaxAmount { get => _filterMaxAmount; set { _filterMaxAmount = value; OnPropertyChanged(nameof(FilterMaxAmount)); ScheduleApplyFiltersDebounced(); } }
        public string FilterReconciliationNum { get => _filterReconciliationNum; set { _filterReconciliationNum = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterReconciliationNum)); ScheduleApplyFiltersDebounced(); } }
        public string FilterRawLabel { get => _filterRawLabel; set { _filterRawLabel = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterRawLabel)); ScheduleApplyFiltersDebounced(); } }
        public string FilterEventNum { get => _filterEventNum; set { _filterEventNum = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterEventNum)); ScheduleApplyFiltersDebounced(); } }
        public string FilterDwGuaranteeId { get => _filterDwGuaranteeId; set { _filterDwGuaranteeId = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterDwGuaranteeId)); ScheduleApplyFiltersDebounced(); } }
        public string FilterDwCommissionId { get => _filterDwCommissionId; set { _filterDwCommissionId = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterDwCommissionId)); ScheduleApplyFiltersDebounced(); } }
        public string FilterStatus { get => _filterStatus; set { _filterStatus = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterStatus)); ScheduleApplyFiltersDebounced(); } }

        // New string-backed ComboBox filters
        private string _filterGuaranteeType;
        public string FilterGuaranteeType { get => _filterGuaranteeType; set { _filterGuaranteeType = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterGuaranteeType)); ScheduleApplyFiltersDebounced(); } }

        private string _filterTransactionType;
        public string FilterTransactionType { get => _filterTransactionType; set { _filterTransactionType = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterTransactionType)); ScheduleApplyFiltersDebounced(); } }

        private string _filterGuaranteeStatus;
        public string FilterGuaranteeStatus { get => _filterGuaranteeStatus; set { _filterGuaranteeStatus = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterGuaranteeStatus)); ScheduleApplyFiltersDebounced(); } }

        private string _filterCategory;
        public string FilterCategory { get => _filterCategory; set { _filterCategory = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterCategory)); ScheduleApplyFiltersDebounced(); } }

        private string _filterActionString;
        public string FilterAction { get => _filterActionString; set { _filterActionString = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterAction)); ScheduleApplyFiltersDebounced(); } }

        private string _filterKpiString;
        public string FilterKPI { get => _filterKpiString; set { _filterKpiString = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterKPI)); ScheduleApplyFiltersDebounced(); } }

        private string _filterIncidentType;
        public string FilterIncidentType { get => _filterIncidentType; set { _filterIncidentType = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterIncidentType)); ScheduleApplyFiltersDebounced(); } }

        // New: Selected IDs bound to ComboBoxes
        public int? FilterActionId { get => _filterActionId; set { _filterActionId = value; OnPropertyChanged(nameof(FilterActionId)); ScheduleApplyFiltersDebounced(); } }
        public int? FilterKpiId { get => _filterKpiId; set { _filterKpiId = value; OnPropertyChanged(nameof(FilterKpiId)); ScheduleApplyFiltersDebounced(); } }
        public int? FilterIncidentTypeId { get => _filterIncidentTypeId; set { _filterIncidentTypeId = value; OnPropertyChanged(nameof(FilterIncidentTypeId)); ScheduleApplyFiltersDebounced(); } }

        #endregion

        #region Editing Handlers (persist user field changes)

        private async void UserFieldComboBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            try
            {
                var cb = sender as ComboBox;
                if (cb == null) return;
                if (cb.SelectedValue == null) return;
                var row = cb.DataContext as ReconciliationViewData;
                if (row == null) return;

                // Determine which field changed via Tag
                var tag = cb.Tag as string;
                int? newId = null;
                try { newId = cb.SelectedValue as int?; } catch { }
                if (newId == null)
                {
                    if (int.TryParse(cb.SelectedValue?.ToString(), out var parsed)) newId = parsed; else return;
                }

                // Load current reconciliation from DB to avoid overwriting unrelated fields
                if (_reconciliationService == null) return;
                var reco = await _reconciliationService.GetOrCreateReconciliationAsync(row.ID);

                if (string.Equals(tag, "Action", StringComparison.OrdinalIgnoreCase))
                {
                    row.Action = newId; // update UI model
                    reco.Action = newId;
                }
                else if (string.Equals(tag, "KPI", StringComparison.OrdinalIgnoreCase))
                {
                    row.KPI = newId;
                    reco.KPI = newId;
                }
                else if (string.Equals(tag, "Incident Type", StringComparison.OrdinalIgnoreCase) || string.Equals(tag, "IncidentType", StringComparison.OrdinalIgnoreCase))
                {
                    row.IncidentType = newId;
                    reco.IncidentType = newId;
                }
                else
                {
                    return;
                }

                await _reconciliationService.SaveReconciliationAsync(reco);

                // Fire-and-forget background sync to network DB to reduce sync debt
                try
                {
                    if (_offlineFirstService != null && _offlineFirstService.IsInitialized && _offlineFirstService.IsNetworkSyncAvailable)
                    {
                        _ = Task.Run(async () =>
                        {
                            try { await _offlineFirstService.SynchronizeData(); }
                            catch { /* best-effort: ignore background sync errors */ }
                        });
                    }
                }
                catch { /* ignore any scheduling errors */ }
            }
            catch (Exception ex)
            {
                // Enrichir le message avec des infos de diagnostic de connexion
                try
                {
                    string country = _offlineFirstService?.CurrentCountryId ?? "<null>";
                    bool isInit = _offlineFirstService?.IsInitialized ?? false;
                    string cs = null;
                    try { cs = _offlineFirstService?.GetCurrentLocalConnectionString(); } catch (Exception csex) { cs = $"<error: {csex.Message}>"; }
                    string dw = null;
                    try { dw = _offlineFirstService?.GetLocalDWDatabasePath(); } catch { }
                    ShowError($"Erreur de sauvegarde: {ex.Message}\nPays: {country} | Init: {isInit}\nCS: {cs}\nDW: {dw}");
                }
                catch
                {
                    ShowError($"Erreur de sauvegarde: {ex.Message}");
                }
            }
        }

        // Persist text/checkbox/date edits as soon as a cell commit occurs
        private async void ResultsDataGrid_CellEditEnding(object sender, DataGridCellEditEndingEventArgs e)
        {
            try
            {
                if (e.EditAction != DataGridEditAction.Commit) return;
                var rowData = e.Row?.Item as ReconciliationViewData;
                if (rowData == null) return;

                // Skip here for ComboBox-based columns handled by UserFieldComboBox_SelectionChanged
                var headerText = Convert.ToString(e.Column?.Header) ?? string.Empty;
                if (string.Equals(headerText, "Action", StringComparison.OrdinalIgnoreCase)
                    || string.Equals(headerText, "KPI", StringComparison.OrdinalIgnoreCase)
                    || string.Equals(headerText, "Incident Type", StringComparison.OrdinalIgnoreCase))
                {
                    return;
                }

                // Ensure the editing element pushes its value to the binding source before we save
                if (e.EditingElement is TextBox tb)
                {
                    try { tb.GetBindingExpression(TextBox.TextProperty)?.UpdateSource(); } catch { }
                }
                else if (e.EditingElement is CheckBox cbx)
                {
                    try { cbx.GetBindingExpression(ToggleButton.IsCheckedProperty)?.UpdateSource(); } catch { }
                }
                else if (e.EditingElement is DatePicker dp)
                {
                    try { dp.GetBindingExpression(DatePicker.SelectedDateProperty)?.UpdateSource(); } catch { }
                }

                await SaveEditedRowAsync(rowData);
            }
            catch (Exception ex)
            {
                ShowError($"Erreur de sauvegarde (cell): {ex.Message}");
            }
        }

        // Extra safety to commit any pending cell/row edits when the current cell changes (e.g., checkboxes)
        private void ResultsDataGrid_CurrentCellChanged(object sender, EventArgs e)
        {
            try
            {
                var dg = sender as DataGrid;
                if (dg == null) return;
                dg.CommitEdit(DataGridEditingUnit.Cell, true);
                dg.CommitEdit(DataGridEditingUnit.Row, true);
            }
            catch { }
        }

        // Loads existing reconciliation and maps editable fields from the view row, then saves
        private async Task SaveEditedRowAsync(ReconciliationViewData row)
        {
            if (_reconciliationService == null || row == null) return;
            var reco = await _reconciliationService.GetOrCreateReconciliationAsync(row.ID);

            // Map user-editable fields
            reco.Action = row.Action;
            reco.KPI = row.KPI;
            reco.IncidentType = row.IncidentType;
            reco.Comments = row.Comments;
            reco.InternalInvoiceReference = row.InternalInvoiceReference;
            reco.FirstClaimDate = row.FirstClaimDate;
            reco.LastClaimDate = row.LastClaimDate;
            reco.ToRemind = row.ToRemind;
            reco.ToRemindDate = row.ToRemindDate;
            reco.ACK = row.ACK;
            reco.SwiftCode = row.SwiftCode;
            reco.PaymentReference = row.PaymentReference;
            reco.RiskyItem = row.RiskyItem;
            reco.ReasonNonRisky = row.ReasonNonRisky;

            await _reconciliationService.SaveReconciliationAsync(reco);

            // Best-effort background sync
            try
            {
                if (_offlineFirstService != null && _offlineFirstService.IsInitialized && _offlineFirstService.IsNetworkSyncAvailable)
                {
                    _ = Task.Run(async () =>
                    {
                        try { await _offlineFirstService.SynchronizeData(); }
                        catch { }
                    });
                }
            }
            catch { }
        }

        #endregion

        #region Filtering

        /// <summary>
        /// Applique les filtres aux données
        /// </summary>
        private void ApplyFilters()
        {
            if (_allViewData == null) return;
            var sw = Stopwatch.StartNew();
            var filtered = _allViewData.AsEnumerable();

            // Appliquer les filtres sur les données Ambre
            if (!string.IsNullOrEmpty(_filterAccountId))
            {
                var id = _filterAccountId?.Trim();
                filtered = filtered.Where(x => string.Equals(x.Account_ID?.Trim(), id, StringComparison.OrdinalIgnoreCase));
            }

            if (!string.IsNullOrEmpty(_filterCurrency))
                filtered = filtered.Where(x => x.CCY?.IndexOf(_filterCurrency, StringComparison.OrdinalIgnoreCase) >= 0);

            if (_filterMinAmount.HasValue)
                filtered = filtered.Where(x => x.SignedAmount >= _filterMinAmount.Value);

            if (_filterMaxAmount.HasValue)
                filtered = filtered.Where(x => x.SignedAmount <= _filterMaxAmount.Value);

            if (_filterFromDate.HasValue)
                filtered = filtered.Where(x => x.Operation_Date >= _filterFromDate.Value);

            if (_filterToDate.HasValue)
                filtered = filtered.Where(x => x.Operation_Date <= _filterToDate.Value);

            if (!string.IsNullOrEmpty(_filterReconciliationNum))
                filtered = filtered.Where(x => x.Reconciliation_Num?.IndexOf(_filterReconciliationNum, StringComparison.OrdinalIgnoreCase) >= 0);

            if (!string.IsNullOrEmpty(_filterRawLabel))
                filtered = filtered.Where(x => x.RawLabel?.IndexOf(_filterRawLabel, StringComparison.OrdinalIgnoreCase) >= 0);

            if (!string.IsNullOrEmpty(_filterEventNum))
                filtered = filtered.Where(x => x.Event_Num?.IndexOf(_filterEventNum, StringComparison.OrdinalIgnoreCase) >= 0);

            // Filter by Transaction Type (match against known Ambre label fields)
            if (!string.IsNullOrWhiteSpace(_filterTransactionType))
            {
                var term = _filterTransactionType.Trim();
                filtered = filtered.Where(x =>
                    (x.Pivot_TransactionCodesFromLabel?.IndexOf(term, StringComparison.OrdinalIgnoreCase) ?? -1) >= 0
                    || (x.Pivot_TRNFromLabel?.IndexOf(term, StringComparison.OrdinalIgnoreCase) ?? -1) >= 0);
            }

            // Filter by Guarantee Status (from DWINGS Guarantee table)
            if (!string.IsNullOrWhiteSpace(_filterGuaranteeStatus))
            {
                var gs = _filterGuaranteeStatus.Trim();
                filtered = filtered.Where(x => (x.GUARANTEE_STATUS?.IndexOf(gs, StringComparison.OrdinalIgnoreCase) ?? -1) >= 0);
            }

            if (!string.IsNullOrEmpty(_filterDwGuaranteeId))
                filtered = filtered.Where(x => x.DWINGS_GuaranteeID?.IndexOf(_filterDwGuaranteeId, StringComparison.OrdinalIgnoreCase) >= 0
                                            || x.GUARANTEE_ID?.IndexOf(_filterDwGuaranteeId, StringComparison.OrdinalIgnoreCase) >= 0);

            if (!string.IsNullOrEmpty(_filterDwCommissionId))
                filtered = filtered.Where(x => x.DWINGS_CommissionID?.IndexOf(_filterDwCommissionId, StringComparison.OrdinalIgnoreCase) >= 0
                                            || x.COMMISSION_ID?.IndexOf(_filterDwCommissionId, StringComparison.OrdinalIgnoreCase) >= 0);

            // Filter by DeletedDate exact day if provided (Archived date)
            if (FilterDeletedDate.HasValue)
            {
                var day = FilterDeletedDate.Value.Date;
                var next = day.AddDays(1);
                filtered = filtered.Where(a => a.DeleteDate.HasValue && a.DeleteDate.Value >= day && a.DeleteDate.Value < next);
            }

            // Status Live/Archived based on DeleteDate presence
            if (!string.IsNullOrEmpty(_filterStatus) && !string.Equals(_filterStatus, "All", StringComparison.OrdinalIgnoreCase))
            {
                if (string.Equals(_filterStatus, "Live", StringComparison.OrdinalIgnoreCase))
                {
                    filtered = filtered.Where(a => !a.DeleteDate.HasValue);
                }
                else if (string.Equals(_filterStatus, "Archived", StringComparison.OrdinalIgnoreCase))
                {
                    filtered = filtered.Where(a => a.DeleteDate.HasValue);
                }
            }

            // Apply referential ComboBox filters if set
            if (_filterActionId.HasValue)
                filtered = filtered.Where(x => x.Action == _filterActionId);
            if (_filterKpiId.HasValue)
                filtered = filtered.Where(x => x.KPI == _filterKpiId);
            if (_filterIncidentTypeId.HasValue)
                filtered = filtered.Where(x => x.IncidentType == _filterIncidentTypeId);

            // Mettre à jour l'affichage avec pagination (100 premières lignes), mais totaux sur tout le jeu filtré
            var filteredList = filtered.ToList();
            _filteredData = filteredList;
            _loadedCount = Math.Min(InitialPageSize, _filteredData.Count);
            ViewData = new ObservableCollection<ReconciliationViewData>(_filteredData.Take(_loadedCount));
            UpdateKpis(_filteredData); // totaux sur l'ensemble
            UpdateStatusInfo($"{ViewData.Count} / {_filteredData.Count} lignes affichées");
            LogAction("ApplyFilters", $"{ViewData.Count} / {_filteredData.Count} displayed | Account={_filterAccountId ?? "All"} | Status={_filterStatus ?? "All"}");
            sw.Stop();
            try { LogPerf("ApplyFilters", $"source={_allViewData.Count} | displayed={ViewData.Count} | ms={sw.ElapsedMilliseconds}"); } catch { }
        }

        // Raccorde le ScrollViewer du DataGrid pour le chargement incrémental
        private void TryHookResultsGridScroll(DataGrid dg)
        {
            try
            {
                if (_scrollHooked || dg == null) return;
                _resultsScrollViewer = FindDescendant<ScrollViewer>(dg);
                if (_resultsScrollViewer != null)
                {
                    _resultsScrollViewer.ScrollChanged -= ResultsScrollViewer_ScrollChanged;
                    _resultsScrollViewer.ScrollChanged += ResultsScrollViewer_ScrollChanged;
                    _scrollHooked = true;
                }
            }
            catch { }
        }

        private void ResultsScrollViewer_ScrollChanged(object sender, ScrollChangedEventArgs e)
        {
            try
            {
                var sw = Stopwatch.StartNew();
                if (_filteredData == null || _filteredData.Count == 0) return;
                var sv = sender as ScrollViewer;
                if (sv == null) return;
                // Show footer button when user reaches bottom (android-like behavior)
                bool atBottom = sv.ScrollableHeight > 0 && sv.VerticalOffset >= (sv.ScrollableHeight * 0.9);
                int remaining = Math.Max(0, _filteredData.Count - _loadedCount);
                var btn = this.FindName("LoadMoreFooterButton") as Button;
                if (btn != null)
                {
                    btn.Visibility = (atBottom && remaining > 0) ? Visibility.Visible : Visibility.Collapsed;
                }

                sw.Stop();
                // Throttle perf log to once every ScrollLogThrottleMs
                var now = DateTime.Now;
                if ((now - _lastScrollPerfLog).TotalMilliseconds >= ScrollLogThrottleMs)
                {
                    try
                    {
                        LogPerf("GridScroll",
                            $"offset={sv.VerticalOffset:0.0}/{sv.ScrollableHeight:0.0} | deltaV={e.VerticalChange:0.0} | viewport={sv.ViewportHeight:0.0} | itemsLoaded={_loadedCount}/{_filteredData.Count} | ms={sw.ElapsedMilliseconds}");
                    }
                    catch { }
                    _lastScrollPerfLog = now;
                }
            }
            catch { }
        }

        private void LoadMorePage()
        {
            if (_isLoadingMore) return;
            _isLoadingMore = true;
            try
            {
                if (_filteredData == null) return;
                int remaining = _filteredData.Count - _loadedCount;
                if (remaining <= 0) return;
                int take = Math.Min(InitialPageSize, remaining);
                foreach (var item in _filteredData.Skip(_loadedCount).Take(take))
                {
                    ViewData.Add(item);
                }
                _loadedCount += take;
                UpdateStatusInfo($"{ViewData.Count} / {_filteredData.Count} lignes affichées");
                // After load, hide footer if no more data, otherwise keep visible when still at bottom
                var btn = this.FindName("LoadMoreFooterButton") as Button;
                if (btn != null)
                {
                    int newRemaining = _filteredData.Count - _loadedCount;
                    if (newRemaining <= 0)
                        btn.Visibility = Visibility.Collapsed;
                }
            }
            catch { }
            finally
            {
                _isLoadingMore = false;
            }
        }

        private void UpdateKpis(IEnumerable<ReconciliationViewData> data)
        {
            try
            {
                var list = data?.ToList() ?? new List<ReconciliationViewData>();
                int total = list.Count;
                int matched = list.Count(a => !string.IsNullOrWhiteSpace(a.DWINGS_GuaranteeID)
                                           || !string.IsNullOrWhiteSpace(a.DWINGS_InvoiceID)
                                           || !string.IsNullOrWhiteSpace(a.DWINGS_CommissionID));
                int unmatched = total - matched;

                decimal totalAmt = list.Sum(a => a.SignedAmount);
                decimal matchedAmt = list.Where(a => !string.IsNullOrWhiteSpace(a.DWINGS_GuaranteeID)
                                                   || !string.IsNullOrWhiteSpace(a.DWINGS_InvoiceID)
                                                   || !string.IsNullOrWhiteSpace(a.DWINGS_CommissionID))
                                         .Sum(a => a.SignedAmount);
                decimal unmatchedAmt = totalAmt - matchedAmt;

                if (KpiTotalCountText != null) KpiTotalCountText.Text = total.ToString();
                if (KpiMatchedCountText != null) KpiMatchedCountText.Text = matched.ToString();
                if (KpiUnmatchedCountText != null) KpiUnmatchedCountText.Text = unmatched.ToString();
                if (KpiTotalAmountText != null) KpiTotalAmountText.Text = totalAmt.ToString("N2");
                if (KpiMatchedAmountText != null) KpiMatchedAmountText.Text = matchedAmt.ToString("N2");
                if (KpiUnmatchedAmountText != null) KpiUnmatchedAmountText.Text = unmatchedAmt.ToString("N2");
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Erreur KPI: {ex.Message}");
            }
        }

        /// <summary>
        /// Réinitialise tous les filtres
        /// </summary>
        private void ClearFilters()
        {
            _filterAccountId = null;
            _filterCurrency = null;
            _filterCountry = null;
            _filterMinAmount = null;
            _filterMaxAmount = null;
            _filterFromDate = null;
            _filterToDate = null;
            _filterAction = null;
            _filterKPI = null;
            _filterGuaranteeType = null;
            _filterTransactionType = null;
            _filterGuaranteeStatus = null;
            _filterCategory = null;
            _filterActionString = null;
            _filterKpiString = null;
            _filterIncidentType = null;
            _filterActionId = null;
            _filterKpiId = null;
            _filterIncidentTypeId = null;

            // Réinitialiser les contrôles UI
            ClearFilterControls();
            ApplyFilters();
        }

        /// <summary>
        /// Efface les contrôles de filtre dans l'UI
        /// </summary>
        private void ClearFilterControls()
        {
            try
            {
                // Effacer les TextBox de filtres (noms basés sur le XAML)
                ClearTextBox("AccountIdFilterTextBox");
                ClearTextBox("CurrencyFilterTextBox");
                ClearTextBox("CountryFilterTextBox");
                ClearTextBox("MinAmountFilterTextBox");
                ClearTextBox("MaxAmountFilterTextBox");
                ClearDatePicker("FromDatePicker");
                ClearDatePicker("ToDatePicker");
                ClearComboBox("ActionComboBox");
                ClearComboBox("KPIComboBox");
                ClearComboBox("IncidentTypeComboBox");
                // New ComboBoxes in Ambre Filters
                ClearComboBox("TypeComboBox");
                ClearComboBox("TransactionTypeComboBox");
                ClearComboBox("GuaranteeStatusComboBox");
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Erreur lors de l'effacement des filtres: {ex.Message}");
            }
        }

        #endregion

        #region Event Handlers

        /// <summary>
        /// Basculer l'affichage des filtres
        /// </summary>
        private void ToggleFilters_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                FiltersExpander.IsExpanded = !FiltersExpander.IsExpanded;
            }
            catch (Exception ex)
            {
                ShowError($"Erreur lors du basculement des filtres: {ex.Message}");
            }
        }

        /// <summary>
        /// Sélection changée dans la grille
        /// </summary>
        private void ResultsDataGrid_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            try
            {
                var selectedItem = ResultsDataGrid.SelectedItem as ReconciliationViewData;
                if (selectedItem != null)
                {
                    // Mettre à jour les infos de sélection si nécessaire
                    UpdateStatusInfo($"Ligne sélectionnée: {selectedItem.ID}");
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Erreur sélection: {ex.Message}");
            }
        }

        /// <summary>
        /// Double-clic sur une ligne de la grille
        /// </summary>
        private async void ResultsDataGrid_MouseDoubleClick(object sender, MouseButtonEventArgs e)
        {
            try
            {
                var selectedItem = ResultsDataGrid.SelectedItem as ReconciliationViewData;
                if (selectedItem != null)
                {
                    var win = new RecoTool.UI.Views.Windows.ReconciliationDetailWindow(selectedItem, _allViewData);
                    win.Owner = Window.GetWindow(this);
                    var result = win.ShowDialog();
                    if (result == true)
                    {
                        await RefreshAsync();
                    }
                }
            }
            catch (Exception ex)
            {
                ShowError($"Erreur lors de l'ouverture du détail: {ex.Message}");
            }
        }

        /// <summary>
        /// Efface les filtres (événement du bouton)
        /// </summary>
        private void ClearFilters_Click(object sender, RoutedEventArgs e)
        {
            ClearFilters();
        }

        #endregion

        #region Helper Methods

        /// <summary>
        /// Lit les filtres depuis l'interface utilisateur
        /// </summary>
        private void ReadFiltersFromUI()
        {
            try
            {
                _filterAccountId = GetTextBoxValue("AccountIdFilterTextBox");
                _filterCurrency = GetTextBoxValue("CurrencyFilterTextBox");
                _filterCountry = GetTextBoxValue("CountryFilterTextBox");

                _filterMinAmount = GetDecimalFromTextBox("MinAmountFilterTextBox");
                _filterMaxAmount = GetDecimalFromTextBox("MaxAmountFilterTextBox");

                _filterFromDate = GetDatePickerValue("FromDatePicker");
                _filterToDate = GetDatePickerValue("ToDatePicker");

                _filterAction = GetComboBoxIntValue("ActionFilterComboBox");
                _filterKPI = GetComboBoxIntValue("KPIFilterComboBox");
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Erreur lecture filtres: {ex.Message}");
            }
        }

        /// <summary>
        /// Gestion du double-clic sur une ligne Ambre (code-behind pur)
        /// </summary>
        private void OnAmbreItemDoubleClick(DataAmbre item)
        {
            try
            {
                // Ouvrir le détail d'une ligne Ambre
                MessageBox.Show($"Détail Ambre - ID: {item.ID}\nCompte: {item.Account_ID}\nMontant: {item.SignedAmount:N2}\nDevise: {item.CCY}\nDate: {item.Operation_Date:d}",
                               "Détail Ambre", MessageBoxButton.OK, MessageBoxImage.Information);
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Erreur lors de l'ouverture du détail: {ex.Message}", "Erreur", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        /// <summary>
        /// Met à jour le titre de la vue
        /// </summary>
        private void UpdateViewTitle()
        {
            try
            {
                if (TitleText != null)
                {
                    // Construire un titre convivial selon l'état courant
                    // 1) Nom du filtre (si défini)
                    var hasNamedFilter = !string.IsNullOrWhiteSpace(_currentView) && !string.Equals(_currentView, "Default View", StringComparison.OrdinalIgnoreCase);

                    // 2) Account (friendly, sans ID éventuel entre parenthèses)
                    string friendlyAcc = null;
                    if (!string.IsNullOrWhiteSpace(_filterAccountId))
                    {
                        var idx = _filterAccountId.LastIndexOf('(');
                        friendlyAcc = idx > 0 ? _filterAccountId.Substring(0, idx).Trim() : _filterAccountId;
                    }
                    var accPart = string.IsNullOrWhiteSpace(friendlyAcc) ? "All" : friendlyAcc;

                    // 3) Status
                    var statPart = string.IsNullOrWhiteSpace(_filterStatus) ? "All" : _filterStatus;

                    // 4) Construire le titre final
                    // Format: "Account: {acc} | Status: {status}" et si filtre nommé: " - {filterName}"
                    var baseTitle = $"Account: {accPart} | Status: {statPart}";
                    var finalTitle = hasNamedFilter ? baseTitle + $" - {_currentView}" : baseTitle;

                    TitleText.Text = finalTitle;
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Erreur mise à jour titre: {ex.Message}");
            }
        }

        /// <summary>
        /// Définit le titre de la vue (ex: nom du filtre sélectionné) et met à jour l'UI
        /// </summary>
        public void SetViewTitle(string title)
        {
            _currentView = string.IsNullOrWhiteSpace(title) ? _currentView : title;
            UpdateViewTitle();
        }

        /// <summary>
        /// Met à jour les informations de statut
        /// </summary>
        private void UpdateStatusInfo(string status)
        {
            try
            {
                if (AccountInfoText != null)
                {
                    AccountInfoText.Text = $"Pays: {_currentCountryId ?? "N/A"} | {status}";
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Erreur mise à jour statut: {ex.Message}");
            }
        }

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

        // Helper: find a descendant of a given type in the visual tree
        private static T FindDescendant<T>(DependencyObject root) where T : DependencyObject
        {
            if (root == null) return null;
            int count = VisualTreeHelper.GetChildrenCount(root);
            for (int i = 0; i < count; i++)
            {
                var child = VisualTreeHelper.GetChild(root, i);
                if (child is T t) return t;
                var found = FindDescendant<T>(child);
                if (found != null) return found;
            }
            return null;
        }

        /// <summary>
        /// Affiche un message d'erreur
        /// </summary>
        private void ShowError(string message)
        {
            MessageBox.Show(message, "Erreur", MessageBoxButton.OK, MessageBoxImage.Error);
        }

        private void LogAction(string action, string details)
        {
            try
            {
                var user = Environment.UserName;
                var dir = System.IO.Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "RecoTool");
                if (!Directory.Exists(dir)) Directory.CreateDirectory(dir);
                var path = System.IO.Path.Combine(dir, "actions.log");
                var line = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss}\t{user}\t{action}\t{details}";
                System.IO.File.AppendAllLines(path, new[] { line }, Encoding.UTF8);
            }
            catch
            {
                // ignore logging failures
            }
        }

        // Append performance diagnostics to %APPDATA%/RecoTool/perf.log
        private void LogPerf(string area, string details)
        {
            try
            {
                var dir = System.IO.Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "RecoTool");
                if (!Directory.Exists(dir)) Directory.CreateDirectory(dir);
                var path = System.IO.Path.Combine(dir, "perf.log");
                var line = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss}\t{area}\t{details}";
                System.IO.File.AppendAllLines(path, new[] { line }, Encoding.UTF8);
            }
            catch { }
        }

        // Build an Access SQL WHERE clause from current bound filters
        private string GenerateWhereClause()
        {
            string Esc(string s) => string.IsNullOrEmpty(s) ? s : s.Replace("'", "''");
            string DateLit(DateTime d) => "#" + d.ToString("yyyy-MM-dd") + "#"; // Access date literal

            var parts = new List<string>();
            if (!string.IsNullOrWhiteSpace(FilterAccountId)) parts.Add($"Account_ID = '{Esc(FilterAccountId)}'");
            if (!string.IsNullOrWhiteSpace(FilterCurrency)) parts.Add($"CCY = '{Esc(FilterCurrency)}'");
            if (FilterMinAmount.HasValue) parts.Add($"SignedAmount >= {FilterMinAmount.Value.ToString(System.Globalization.CultureInfo.InvariantCulture)}");
            if (FilterMaxAmount.HasValue) parts.Add($"SignedAmount <= {FilterMaxAmount.Value.ToString(System.Globalization.CultureInfo.InvariantCulture)}");
            if (FilterFromDate.HasValue) parts.Add($"Operation_Date >= {DateLit(FilterFromDate.Value)}");
            if (FilterToDate.HasValue) parts.Add($"Operation_Date <= {DateLit(FilterToDate.Value)}");
            if (FilterDeletedDate.HasValue)
            {
                var d = FilterDeletedDate.Value.Date;
                var next = d.AddDays(1);
                parts.Add($"a.DeleteDate >= {DateLit(d)} AND a.DeleteDate < {DateLit(next)}");
            }
            if (!string.IsNullOrWhiteSpace(FilterReconciliationNum)) parts.Add($"Reconciliation_Num LIKE '%{Esc(FilterReconciliationNum)}%'");
            if (!string.IsNullOrWhiteSpace(FilterRawLabel)) parts.Add($"RawLabel LIKE '%{Esc(FilterRawLabel)}%'");
            if (!string.IsNullOrWhiteSpace(FilterEventNum)) parts.Add($"Event_Num LIKE '%{Esc(FilterEventNum)}%'");
            // Persist Transaction Type filter: match either Ambre label field
            if (!string.IsNullOrWhiteSpace(FilterTransactionType))
            {
                var t = Esc(FilterTransactionType);
                parts.Add($"(Pivot_TransactionCodesFromLabel LIKE '%{t}%' OR Pivot_TRNFromLabel LIKE '%{t}%')");
            }
            // Persist Guarantee Status filter from DW Guarantee
            if (!string.IsNullOrWhiteSpace(FilterGuaranteeStatus))
            {
                var gs = Esc(FilterGuaranteeStatus);
                parts.Add($"GUARANTEE_STATUS LIKE '%{gs}%'");
            }
            if (!string.IsNullOrWhiteSpace(FilterDwGuaranteeId)) parts.Add($"DWINGS_GuaranteeID LIKE '%{Esc(FilterDwGuaranteeId)}%'");
            if (!string.IsNullOrWhiteSpace(FilterDwCommissionId)) parts.Add($"DWINGS_CommissionID LIKE '%{Esc(FilterDwCommissionId)}%'");
            if (!string.IsNullOrWhiteSpace(_filterStatus))
            {
                if (string.Equals(_filterStatus, "Matched", StringComparison.OrdinalIgnoreCase))
                    parts.Add("((DWINGS_GuaranteeID Is Not Null AND DWINGS_GuaranteeID <> '') OR (DWINGS_CommissionID Is Not Null AND DWINGS_CommissionID <> ''))");
                else if (string.Equals(_filterStatus, "Unmatched", StringComparison.OrdinalIgnoreCase))
                    parts.Add("((DWINGS_GuaranteeID Is Null OR DWINGS_GuaranteeID = '') AND (DWINGS_CommissionID Is Null OR DWINGS_CommissionID = ''))");
                if (string.Equals(_filterStatus, "Live", StringComparison.OrdinalIgnoreCase))
                    parts.Add("a.DeleteDate IS NULL");
                else if (string.Equals(_filterStatus, "Archived", StringComparison.OrdinalIgnoreCase))
                    parts.Add("a.DeleteDate IS NOT NULL");
            }
            return parts.Count == 0 ? string.Empty : ("WHERE " + string.Join(" AND ", parts));
        }

        // Parse the WHERE clause we generate and set bound properties accordingly
        private void ApplyWhereClause(string where)
        {
            if (string.IsNullOrWhiteSpace(where)) return;
            string s = where.Trim();
            if (s.StartsWith("WHERE ", StringComparison.OrdinalIgnoreCase)) s = s.Substring(6);

            string GetString(string pattern)
            {
                var m = Regex.Match(s, pattern, RegexOptions.IgnoreCase);
                return m.Success ? m.Groups[1].Value.Replace("''", "'") : null;
            }
            decimal? GetDecimal(string pattern)
            {
                var m = Regex.Match(s, pattern, RegexOptions.IgnoreCase);
                if (!m.Success) return null;
                return decimal.TryParse(m.Groups[1].Value, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var d) ? d : (decimal?)null;
            }
            DateTime? GetDate(string pattern)
            {
                var m = Regex.Match(s, pattern, RegexOptions.IgnoreCase);
                if (!m.Success) return null;
                var v = m.Groups[1].Value; // yyyy-MM-dd
                return DateTime.TryParse(v, out var dt) ? dt : (DateTime?)null;
            }

            FilterAccountId = GetString(@"Account_ID\s*=\s*'([^']*)'");
            FilterCurrency = GetString(@"CCY\s*=\s*'([^']*)'");
            FilterMinAmount = GetDecimal(@"SignedAmount\s*>=\s*([0-9]+(?:\.[0-9]+)?)");
            FilterMaxAmount = GetDecimal(@"SignedAmount\s*<=\s*([0-9]+(?:\.[0-9]+)?)");
            var d1 = GetDate(@"Operation_Date\s*>=\s*#([0-9]{4}-[0-9]{2}-[0-9]{2})#");
            var d2 = GetDate(@"Operation_Date\s*<=\s*#([0-9]{4}-[0-9]{2}-[0-9]{2})#");
            FilterFromDate = d1;
            FilterToDate = d2;
            FilterReconciliationNum = GetString(@"Reconciliation_Num\s+LIKE\s+'%([^']*)%'");
            FilterRawLabel = GetString(@"RawLabel\s+LIKE\s+'%([^']*)%'");
            FilterEventNum = GetString(@"Event_Num\s+LIKE\s+'%([^']*)%'");
            // Restore Transaction Type if present (from either field condition)
            var trn1 = GetString(@"Pivot_TransactionCodesFromLabel\s+LIKE\s+'%([^']*)%'");
            var trn2 = GetString(@"Pivot_TRNFromLabel\s+LIKE\s+'%([^']*)%'");
            if (!string.IsNullOrWhiteSpace(trn1)) FilterTransactionType = trn1;
            else if (!string.IsNullOrWhiteSpace(trn2)) FilterTransactionType = trn2;
            // Restore Guarantee Status
            var gs = GetString(@"GUARANTEE_STATUS\s+LIKE\s+'%([^']*)%'");
            if (!string.IsNullOrWhiteSpace(gs)) FilterGuaranteeStatus = gs;
            var hasMatched = Regex.IsMatch(s, @"\(\(DWINGS_GuaranteeID\s+Is\s+Not\s+Null\s+AND\s+DWINGS_GuaranteeID\s+<>\s+''\)\s+OR\s+\(DWINGS_CommissionID\s+Is\s+Not\s+Null\s+AND\s+DWINGS_CommissionID\s+<>\s+''\)\)", RegexOptions.IgnoreCase);
            var hasUnmatched = Regex.IsMatch(s, @"\(\(DWINGS_GuaranteeID\s+Is\s+Null\s+OR\s+DWINGS_GuaranteeID\s+=\s+''\)\s+AND\s+\(DWINGS_CommissionID\s+Is\s+Null\s+OR\s+DWINGS_CommissionID\s+=\s+''\)\)", RegexOptions.IgnoreCase);
            _filterStatus = hasMatched ? "Matched" : hasUnmatched ? "Unmatched" : _filterStatus;
            FilterDwGuaranteeId = GetString(@"DWINGS_GuaranteeID.*LIKE\s+'%([^']*)%'");
            FilterDwCommissionId = GetString(@"DWINGS_CommissionID.*LIKE\s+'%([^']*)%'");

            // Restore DeletedDate single-day filter if present (expects pattern a.DeleteDate >= #YYYY-MM-DD# AND a.DeleteDate < #YYYY-MM-DD#)
            try
            {
                var m1 = Regex.Match(s, @"a\.DeleteDate\s*>=\s*#([0-9]{4}-[0-9]{2}-[0-9]{2})#", RegexOptions.IgnoreCase);
                if (m1.Success)
                {
                    if (DateTime.TryParse(m1.Groups[1].Value, out var dd))
                        FilterDeletedDate = dd.Date;
                }
            }
            catch { }

            // Restore Live/Archived from DeleteDate presence conditions
            try
            {
                if (Regex.IsMatch(s, @"a\.DeleteDate\s+is\s+null", RegexOptions.IgnoreCase))
                    _filterStatus = "Live";
                else if (Regex.IsMatch(s, @"a\.DeleteDate\s+is\s+not\s+null", RegexOptions.IgnoreCase))
                    _filterStatus = "Archived";
            }
            catch { }
        }

        // Save current filters to DB (T_Ref_User_Filter)
        private void SaveFilter_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var name = Interaction.InputBox("Filter name:", "Save filter", "My Filter");
                if (string.IsNullOrWhiteSpace(name)) return;

                var where = GenerateWhereClause();
                // Embed a JSON snapshot for full restoration of all fields
                var sqlToSave = BuildSqlWithJsonComment(GetCurrentFilterPreset(), where);
                var service = new RecoTool.Services.UserFilterService(Settings.Default.ReferentialDB, Environment.UserName);
                service.SaveUserFilter(name, sqlToSave);
                // If hosted inside ReconciliationPage, refresh only the SavedFilters combo (no data reload)
                try
                {
                    var hostPage = FindAncestor<RecoTool.Windows.ReconciliationPage>(this);
                    if (hostPage != null)
                    {
                        _ = hostPage.ReloadSavedFiltersOnly();
                    }
                }
                catch { }
                UpdateStatusInfo($"Filter '{name}' saved");
            }
            catch (Exception ex)
            {
                ShowError($"Error saving filter: {ex.Message}");
            }
        }

        // Load filters from DB (T_Ref_User_Filter)
        private void LoadFilter_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                // Open filter picker
                var picker = new FilterPickerWindow { Owner = Window.GetWindow(this) };
                if (picker.ShowDialog() != true) return;
                var name = picker.SelectedFilterName;
                var service = new RecoTool.Services.UserFilterService(Settings.Default.ReferentialDB, Environment.UserName);
                var sql = service.LoadUserFilterWhere(name);
                if (sql == null)
                {
                    ShowError($"Filter '{name}' not found");
                    return;
                }
                // Apply saved filter SQL (restores UI preset and sets _backendFilterSql)
                ApplySavedFilterSql(sql);
                // Reload data from backend using _backendFilterSql
                Refresh();
                SetViewTitle(name);
                UpdateStatusInfo($"Filter '{name}' loaded");
            }
            catch (Exception ex)
            {
                ShowError($"Error loading filters: {ex.Message}");
            }
        }

        // Load Views from DB (T_Ref_User_Fields_Preference)
        private async void LoadView_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if (_reconciliationService == null)
                {
                    ShowError("Service not available");
                    return;
                }

                // Use the same FilterPickerWindow with providers backed by ReconciliationService
                var picker = new FilterPickerWindow("Saved views",
                    s => _reconciliationService.ListUserFieldsPreferenceNamesAsync(s).GetAwaiter().GetResult(),
                    name => _reconciliationService.DeleteUserFieldsPreferenceByNameAsync(name).GetAwaiter().GetResult(),
                    s => _reconciliationService.ListUserFieldsPreferenceDetailedAsync(s).GetAwaiter().GetResult())
                { Owner = Window.GetWindow(this) };

                if (picker.ShowDialog() != true) return;
                var name = picker.SelectedFilterName;
                if (string.IsNullOrWhiteSpace(name)) return;

                var pref = await _reconciliationService.GetUserFieldsPreferenceByNameAsync(name);
                if (pref == null || string.IsNullOrWhiteSpace(pref.UPF_SQL))
                {
                    ShowError($"View '{name}' not found");
                    return;
                }

                var sql = pref.UPF_SQL;
                // Apply saved SQL (restores UI preset and sets _backendFilterSql)
                ApplySavedFilterSql(sql);

                // If a saved layout exists, apply it to the grid
                if (!string.IsNullOrWhiteSpace(pref.UPF_ColumnWidths))
                {
                    try
                    {
                        var layout = JsonSerializer.Deserialize<GridLayout>(pref.UPF_ColumnWidths);
                        ApplyGridLayout(layout);
                    }
                    catch { /* ignore layout parsing errors */ }
                }

                // Reload data from backend using _backendFilterSql
                Refresh();
                SetViewTitle(name);
                UpdateStatusInfo($"View '{name}' loaded");
            }
            catch (Exception ex)
            {
                ShowError($"Error loading view: {ex.Message}");
            }
        }

        // Helper to find ancestor of a specific type in visual tree
        private static T FindAncestor<T>(DependencyObject current) where T : DependencyObject
        {
            while (current != null)
            {
                if (current is T match) return match;
                current = System.Windows.Media.VisualTreeHelper.GetParent(current);
            }
            return null;
        }

        

        // Bulk selection button behavior
        // Click: Select all visible rows
        // Shift+Click: Select matched rows only
        // Ctrl+Click: Select unmatched rows only
        // Alt+Click: Clear selection
        private void BulkSelection_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var dg = FindName("ResultsDataGrid") as DataGrid;
                if (dg == null) return;

                var modifiers = Keyboard.Modifiers;
                if ((modifiers & ModifierKeys.Alt) == ModifierKeys.Alt)
                {
                    dg.UnselectAll();
                    UpdateStatusInfo("Selection cleared");
                    return;
                }

                // Helper local predicate for matched
                bool IsMatched(object obj)
                {
                    var item = obj as ReconciliationViewData;
                    if (item == null) return false;
                    return !string.IsNullOrWhiteSpace(item.DWINGS_GuaranteeID)
                           || !string.IsNullOrWhiteSpace(item.DWINGS_InvoiceID)
                           || !string.IsNullOrWhiteSpace(item.DWINGS_CommissionID)
                           || !string.IsNullOrWhiteSpace(item.GUARANTEE_ID)
                           || !string.IsNullOrWhiteSpace(item.INVOICE_ID)
                           || !string.IsNullOrWhiteSpace(item.COMMISSION_ID);
                }

                dg.UnselectAll();

                // Iterate visible items in the DataGrid
                int selected = 0;
                foreach (var obj in dg.Items)
                {
                    var data = obj as ReconciliationViewData;
                    if (data == null) continue; // skip grouping placeholders, etc.

                    bool pick;
                    if ((modifiers & ModifierKeys.Shift) == ModifierKeys.Shift)
                        pick = IsMatched(data);
                    else if ((modifiers & ModifierKeys.Control) == ModifierKeys.Control)
                        pick = !IsMatched(data);
                    else
                        pick = true; // simple click => all visible

                    if (pick)
                    {
                        dg.SelectedItems.Add(data);
                        selected++;
                    }
                }

                if ((modifiers & ModifierKeys.Shift) == ModifierKeys.Shift)
                    UpdateStatusInfo($"{selected} matched selected");
                else if ((modifiers & ModifierKeys.Control) == ModifierKeys.Control)
                    UpdateStatusInfo($"{selected} unmatched selected");
                else
                    UpdateStatusInfo($"{selected} rows selected");
            }
            catch { }
        }

        private void ApplyAgeFilter_Click(object sender, RoutedEventArgs e)
        {

        }

        

        // Delete a saved filter from DB and refresh the page Saved Filters combo
        private void DeleteFilter_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var picker = new FilterPickerWindow { Owner = Window.GetWindow(this) };
                if (picker.ShowDialog() != true) return;
                var name = picker.SelectedFilterName;
                if (string.IsNullOrWhiteSpace(name)) return;

                var confirm = MessageBox.Show($"Supprimer le filtre '{name}' ?", "Confirmation", MessageBoxButton.YesNo, MessageBoxImage.Question);
                if (confirm != MessageBoxResult.Yes) return;

                var service = new RecoTool.Services.UserFilterService(Settings.Default.ReferentialDB, Environment.UserName);
                if (service.DeleteUserFilter(name))
                {
                    // Refresh only the filters ComboBox on the host page
                    try
                    {
                        var hostPage = FindAncestor<RecoTool.Windows.ReconciliationPage>(this);
                        if (hostPage != null)
                        {
                            _ = hostPage.ReloadSavedFiltersOnly();
                        }
                    }
                    catch { }
                    UpdateStatusInfo($"Filtre '{name}' supprimé");
                }
                else
                {
                    ShowError($"Le filtre '{name}' n'a pas été trouvé.");
                }
            }
            catch (Exception ex)
            {
                ShowError($"Erreur suppression filtre: {ex.Message}");
            }
        }
    }

    // Converter: returns a row background Brush based on Action's USR_Color
    public class ActionColorConverter : IMultiValueConverter
    {
        // Reusable static brushes (frozen) to avoid allocations
        private static readonly Brush LightRed = new SolidColorBrush(Color.FromArgb(40, 255, 0, 0));
        private static readonly Brush LightGreen = new SolidColorBrush(Color.FromArgb(40, 0, 128, 0));
        private static readonly Brush LightYellow = new SolidColorBrush(Color.FromArgb(60, 255, 255, 0));
        private static readonly Brush LightBlue = new SolidColorBrush(Color.FromArgb(40, 0, 0, 255));
        private static readonly Brush Transparent = Brushes.Transparent;

        // Cache: actionId -> brush, rebuilt when AllUserFields reference changes
        private IReadOnlyList<UserField> _lastAllRef;
        private Dictionary<int, Brush> _cacheByActionId = new Dictionary<int, Brush>();

        public object Convert(object[] values, Type targetType, object parameter, CultureInfo culture)
        {
            try
            {
                if (values == null || values.Length < 2) return Transparent;
                int? actionId = null;
                if (values[0] is int i0) actionId = i0;
                else if (values[0] is int?) actionId = (int?)values[0];
                else if (values[0] != null && int.TryParse(values[0].ToString(), out var parsed)) actionId = parsed;

                var all = values[1] as IReadOnlyList<UserField>;
                if (actionId == null || all == null) return Transparent;

                // Rebuild cache only when the source list instance changes (cheap reference check)
                if (!ReferenceEquals(all, _lastAllRef))
                {
                    _cacheByActionId.Clear();
                    // Precompute brushes for each action id
                    foreach (var uf in all)
                    {
                        if (!_cacheByActionId.ContainsKey(uf.USR_ID))
                        {
                            var brush = ToBrush(uf?.USR_Color);
                            _cacheByActionId[uf.USR_ID] = brush;
                        }
                    }
                    _lastAllRef = all;
                }

                if (_cacheByActionId.TryGetValue(actionId.Value, out var cached))
                    return cached ?? Transparent;

                return Transparent;
            }
            catch { return Transparent; }
        }

        private static Brush ToBrush(string colorRaw)
        {
            var color = colorRaw?.Trim()?.ToUpperInvariant();
            if (string.IsNullOrEmpty(color)) return Transparent;
            switch (color)
            {
                case "RED": return LightRed;
                case "GREEN": return LightGreen;
                case "YELLOW": return LightYellow;
                case "BLUE": return LightBlue;
                default:
                    try
                    {
                        var conv = new BrushConverter();
                        var b = conv.ConvertFromString(color) as Brush;
                        return b ?? Transparent;
                    }
                    catch { return Transparent; }
            }
        }

        public object[] ConvertBack(object value, Type[] targetTypes, object parameter, CultureInfo culture)
        {
            throw new NotSupportedException();
        }
    }

    #endregion

    // Converters for UserField ComboBoxes
    public class UserFieldOptionsConverter : IMultiValueConverter
    {
        // values: [0]=Account_ID (string), [1]=AllUserFields (IReadOnlyList<UserField>), [2]=CurrentCountry (Country)
        public object Convert(object[] values, Type targetType, object parameter, CultureInfo culture)
        {
            try
            {
                var accountId = values != null && values.Length > 0 ? values[0]?.ToString() : null;
                var all = values != null && values.Length > 1 ? values[1] as IReadOnlyList<UserField> : null;
                var country = values != null && values.Length > 2 ? values[2] as Country : null;
                var category = parameter?.ToString();

                if (all == null || string.IsNullOrWhiteSpace(category))
                    return Array.Empty<object>();

                bool isPivot = country != null && string.Equals(accountId?.Trim(), country.CNT_AmbrePivot?.Trim(), StringComparison.OrdinalIgnoreCase);
                bool isReceivable = country != null && string.Equals(accountId?.Trim(), country.CNT_AmbreReceivable?.Trim(), StringComparison.OrdinalIgnoreCase);

                // Category mapping: handle synonyms (e.g., Incident Type vs INC)
                bool incident = string.Equals(category, "Incident Type", StringComparison.OrdinalIgnoreCase)
                                 || string.Equals(category, "INC", StringComparison.OrdinalIgnoreCase);
                IEnumerable<UserField> query = incident
                    ? all.Where(u => string.Equals(u.USR_Category, "Incident Type", StringComparison.OrdinalIgnoreCase)
                                     || string.Equals(u.USR_Category, "INC", StringComparison.OrdinalIgnoreCase))
                    : all.Where(u => string.Equals(u.USR_Category, category, StringComparison.OrdinalIgnoreCase));
                // Apply Pivot/Receivable filtering only when we can resolve the account side.
                if (!string.IsNullOrWhiteSpace(accountId) && country != null)
                {
                    if (isPivot)
                        query = query.Where(u => u.USR_Pivot);
                    else if (isReceivable)
                        query = query.Where(u => u.USR_Receivable);
                    // else unknown account side: keep all items for that category
                }

                // Prepend null placeholder to allow clearing selection in ComboBoxes
                var list = new List<object>();
                list.Add(null);
                list.AddRange(query.OrderBy(u => u.USR_FieldName));
                return list;
            }
            catch { return Array.Empty<object>(); }
        }

        public object[] ConvertBack(object value, Type[] targetTypes, object parameter, CultureInfo culture)
            => throw new NotSupportedException();
    }

    public class UserFieldIdToNameConverter : IMultiValueConverter
    {
        // values: [0]=int? id, [1]=AllUserFields
        public object Convert(object[] values, Type targetType, object parameter, CultureInfo culture)
        {
            try
            {
                int? id = null;
                if (values != null && values.Length > 0 && values[0] != null)
                {
                    if (values[0] is int iid) id = iid;
                    else if (int.TryParse(values[0].ToString(), out var parsed)) id = parsed;
                }
                var all = values != null && values.Length > 1 ? values[1] as IReadOnlyList<UserField> : null;
                if (id == null || all == null) return string.Empty;
                var match = all.FirstOrDefault(u => u.USR_ID == id.Value);
                return match?.USR_FieldName ?? string.Empty;
            }
            catch { return string.Empty; }
        }

        public object[] ConvertBack(object value, Type[] targetTypes, object parameter, CultureInfo culture)
            => throw new NotSupportedException();
    }
}