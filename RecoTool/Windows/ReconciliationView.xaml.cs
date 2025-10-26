using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Input;
using System.Windows.Threading;
using RecoTool.Models;
using RecoTool.Services;
using RecoTool.UI.ViewModels;
using System.Windows.Media;
using System.Windows.Controls.Primitives;
using RecoTool.Services.DTOs;
using RecoTool.Services.Rules;
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
        private TodoListSessionTracker _todoSessionTracker; // Multi-user session tracking
        private int _currentTodoId = 0; // Currently active TodoList ID
        // MVVM bridge: lightweight ViewModel holder to gradually migrate bindings
        public ReconciliationViewViewModel VM { get; } = new ReconciliationViewViewModel();
        private string _currentCountryId;
        private string _currentView = "Default View";
        private bool _isLoading;
        private bool _canRefresh = true;
        private bool _initialLoaded;
        private string _initialFilterSql; // Capture initial filter state for reset
        private DispatcherTimer _filterDebounceTimer;
        private DispatcherTimer _highlightClearTimer;
        private DispatcherTimer _toastTimer;
        private DispatcherTimer _multiUserWarningRefreshTimer; // Timer to refresh multi-user warning
        private Action _toastClickAction;
        private string _toastTargetReconciliationId;
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
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }

        // Allows parent page to set a custom title, optionally marking it as a ToDo title
        public void SetViewTitle(string title, bool isTodo)
        {
            try
            {
                var text = string.IsNullOrWhiteSpace(title) ? "Default View" : (isTodo ? $"ToDo: {title}" : title);
                
                // Update _currentView so UpdateViewTitle() preserves it
                _currentView = text;
                
                if (TitleText != null)
                {
                    TitleText.Text = text;
                    TitleText.ToolTip = title;
                }
            }
            catch { }
        }

        // Backward-compatible overload: defaults to non-ToDo
        public void SetViewTitle(string title) => SetViewTitle(title, false);

        // Simple window to toggle visibility of multiple DataGrid columns
        private sealed class ManageColumnsWindow : Window
        {
            private readonly DataGrid _dg;
            private readonly List<ColumnItem> _items = new List<ColumnItem>();

            private sealed class ColumnItem
            {
                public string Header { get; set; }
                public bool IsVisible { get; set; }
                public bool IsProtected { get; set; }
                public DataGridColumn Column { get; set; }
            }

            public ManageColumnsWindow(DataGrid dg)
            {
                _dg = dg;
                Title = "Manage Columns";
                Width = 420;
                Height = 520;
                WindowStartupLocation = WindowStartupLocation.CenterOwner;
                ResizeMode = ResizeMode.CanResizeWithGrip;
                Content = BuildUI();
                // Load columns after window is loaded to ensure visual tree is ready
                this.Loaded += (s, e) => { try { LoadColumns(); } catch { } };
            }

            private UIElement BuildUI()
            {
                var root = new DockPanel { Margin = new Thickness(10) };

                var topBar = new StackPanel { Orientation = Orientation.Horizontal, Margin = new Thickness(0, 0, 0, 8) };
                var selectAllBtn = new Button { Content = "Select All", Height = 26, Margin = new Thickness(0, 0, 6, 0) };
                var deselectAllBtn = new Button { Content = "Deselect All", Height = 26, Margin = new Thickness(0, 0, 6, 0) };
                var resetBtn = new Button { Content = "Reset", Height = 26 };
                selectAllBtn.Click += (s, e) => { foreach (var it in _items.Where(i => !i.IsProtected)) it.IsVisible = true; RefreshList(); };
                deselectAllBtn.Click += (s, e) => { foreach (var it in _items.Where(i => !i.IsProtected)) it.IsVisible = false; RefreshList(); };
                resetBtn.Click += (s, e) => { TryResetToDefaults(); };
                topBar.Children.Add(selectAllBtn);
                topBar.Children.Add(deselectAllBtn);
                topBar.Children.Add(resetBtn);
                DockPanel.SetDock(topBar, Dock.Top);

                var scroll = new ScrollViewer { VerticalScrollBarVisibility = ScrollBarVisibility.Auto };
                var list = new StackPanel { Name = "ListPanel" };
                scroll.Content = list;

                var btnBar = new StackPanel { Orientation = Orientation.Horizontal, HorizontalAlignment = HorizontalAlignment.Right, Margin = new Thickness(0, 8, 0, 0) };
                var okBtn = new Button { Content = "OK", Width = 80, Height = 28, Margin = new Thickness(0, 0, 6, 0) };
                var cancelBtn = new Button { Content = "Cancel", Width = 80, Height = 28 };
                okBtn.Click += (s, e) => { try { Apply(); } catch { } this.DialogResult = true; };
                cancelBtn.Click += (s, e) => { this.DialogResult = false; };
                btnBar.Children.Add(okBtn);
                btnBar.Children.Add(cancelBtn);
                DockPanel.SetDock(btnBar, Dock.Bottom);

                root.Children.Add(topBar);
                root.Children.Add(btnBar);
                root.Children.Add(scroll);
                return root;
            }

            private void LoadColumns()
            {
                _items.Clear();
                if (_dg?.Columns == null) return;
                // Protect first 3 indicator columns (N, U, M)
                for (int i = 0; i < _dg.Columns.Count; i++)
                {
                    var col = _dg.Columns[i];
                    var header = Convert.ToString(col.Header);
                    bool isProtected = i < 3; // keep indicators always visible
                    _items.Add(new ColumnItem
                    {
                        Header = string.IsNullOrWhiteSpace(header) ? $"Column {i + 1}" : header,
                        IsVisible = col.Visibility == Visibility.Visible,
                        IsProtected = isProtected,
                        Column = col
                    });
                }
                RefreshList();
            }

            private void RefreshList()
            {
                var listPanel = FindDescendant<StackPanel>(this.Content as DependencyObject, "ListPanel");
                if (listPanel == null) return;
                listPanel.Children.Clear();
                foreach (var it in _items)
                {
                    var row = new StackPanel { Orientation = Orientation.Horizontal, Margin = new Thickness(0, 2, 0, 2) };
                    var cb = new CheckBox { IsChecked = it.IsVisible, IsEnabled = !it.IsProtected, VerticalAlignment = VerticalAlignment.Center };
                    cb.Checked += (s, e) => it.IsVisible = true;
                    cb.Unchecked += (s, e) => it.IsVisible = false;
                    var tb = new TextBlock { Text = it.Header, Margin = new Thickness(8, 0, 0, 0), VerticalAlignment = VerticalAlignment.Center };
                    if (it.IsProtected)
                    {
                        tb.Text += " (locked)";
                        tb.Foreground = Brushes.Gray;
                    }
                    row.Children.Add(cb);
                    row.Children.Add(tb);
                    listPanel.Children.Add(row);
                }
            }

            private void Apply()
            {
                foreach (var it in _items)
                {
                    try
                    {
                        if (it.IsProtected) continue;
                        it.Column.Visibility = it.IsVisible ? Visibility.Visible : Visibility.Collapsed;
                    }
                    catch { }
                }
            }

            private void TryResetToDefaults()
            {
                try
                {
                    // Simple default: first 12 data columns visible besides indicators, others visible too
                    for (int i = 0; i < _items.Count; i++)
                    {
                        var it = _items[i];
                        if (it.IsProtected) { it.IsVisible = true; continue; }
                        it.IsVisible = true;
                    }
                    RefreshList();
                }
                catch { }
            }

            private static T FindDescendant<T>(DependencyObject root, string name) where T : FrameworkElement
            {
                if (root == null) return null;
                if (root is T fe && (string.IsNullOrEmpty(name) || fe.Name == name)) return fe;
                int count = VisualTreeHelper.GetChildrenCount(root);
                for (int i = 0; i < count; i++)
                {
                    var child = VisualTreeHelper.GetChild(root, i);
                    var match = FindDescendant<T>(child, name);
                    if (match != null) return match;
                }
                return null;
            }
        }

        // Quick add rule based on the current line
        private async void QuickAddRuleFromLineMenuItem_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var mi = sender as MenuItem;
                var row = mi?.DataContext as ReconciliationViewData;
                if (row == null || _offlineFirstService == null) return;

                // Build a seed rule from the selected row
                string accountSide = string.IsNullOrWhiteSpace(row.AccountSide) ? "*" : row.AccountSide.Trim().ToUpperInvariant();
                string guaranteeType = string.IsNullOrWhiteSpace(row.G_GUARANTEE_TYPE) ? "*" : row.G_GUARANTEE_TYPE.Trim().ToUpperInvariant();
                string txName = row.Category.HasValue ? Enum.GetName(typeof(TransactionType), row.Category.Value) : "*";
                bool hasDw = !(string.IsNullOrWhiteSpace(row.DWINGS_InvoiceID) && string.IsNullOrWhiteSpace(row.DWINGS_GuaranteeID) && string.IsNullOrWhiteSpace(row.DWINGS_BGPMT));

                var seed = new TruthRule
                {
                    RuleId = $"USR_{row.ID}",
                    Enabled = true,
                    Priority = 100,
                    Scope = RuleScope.Both,
                    AccountSide = string.IsNullOrWhiteSpace(accountSide) ? "*" : accountSide,
                    GuaranteeType = string.IsNullOrWhiteSpace(guaranteeType) ? "*" : guaranteeType,
                    TransactionType = string.IsNullOrWhiteSpace(txName) ? "*" : txName,
                    HasDwingsLink = hasDw,
                    // Outputs from current row values
                    OutputActionId = row.Action,
                    OutputKpiId = row.KPI,
                    OutputIncidentTypeId = row.IncidentType,
                    ApplyTo = ApplyTarget.Self,
                    AutoApply = true
                };

                var win = new RuleEditorWindow(seed, _offlineFirstService)
                {
                    Owner = Window.GetWindow(this)
                };
                var ok = win.ShowDialog();
                if (ok == true && win.ResultRule != null)
                {
                    var repo = new TruthTableRepository(_offlineFirstService);
                    // Ensure table exists best-effort
                    try { await repo.EnsureRulesTableAsync(); } catch { }
                    var saved = await repo.UpsertRuleAsync(win.ResultRule);
                    if (saved)
                    {
                        UpdateStatusInfo($"Rule '{win.ResultRule.RuleId}' saved.");
                    }
                    else
                    {
                        ShowError("Failed to save rule (Upsert returned false)");
                    }
                }
            }
            catch (Exception ex)
            {
                ShowError($"Failed to add rule: {ex.Message}");
            }
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

        // Set date to today when clicking the calendar button
        private void SetDateToToday_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var button = sender as Button;
                if (button == null) return;

                var fieldName = button.Tag as string;
                if (string.IsNullOrEmpty(fieldName)) return;

                // Find the DatePicker in the same Grid
                var grid = button.Parent as Grid;
                if (grid == null) return;

                var datePicker = grid.Children.OfType<DatePicker>().FirstOrDefault();
                if (datePicker == null) return;

                // Set to today
                datePicker.SelectedDate = DateTime.Today;

                // Get the data context (the row)
                var row = datePicker.DataContext as RecoTool.Services.DTOs.ReconciliationViewData;
                if (row == null) return;

                // Update the property based on field name
                switch (fieldName)
                {
                    case "ActionDate":
                        row.ActionDate = DateTime.Today;
                        break;
                    case "FirstClaimDate":
                        row.FirstClaimDate = DateTime.Today;
                        break;
                    case "LastClaimDate":
                        row.LastClaimDate = DateTime.Today;
                        break;
                    case "ToRemindDate":
                        row.ToRemindDate = DateTime.Today;
                        break;
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Error setting date to today: {ex.Message}");
            }
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
                // Bring the window where the click occurred to front to ensure correct z-order
                try
                {
                    var owner = Window.GetWindow(this);
                    if (owner != null)
                    {
                        owner.Activate();
                        owner.Topmost = true; owner.Topmost = false; // bring-to-front trick without staying topmost
                    }
                }
                catch { }

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
                    foreach (var mi in cm.Items.OfType<MenuItem>().Where(m =>
                                 (m.Tag as string) == "__Take__"
                              || (m.Tag as string) == "__SetReminder__"
                              || (m.Tag as string) == "__MarkActionDone__"
                              || (m.Tag as string) == "__MarkActionPending__"
                              || (m.Tag as string) == "__AddRuleFromLine__"
                              || (m.Tag as string) == "__Copy__"
                              || (m.Tag as string) == "__SearchBGI__"
                              || (m.Tag as string) == "__DebugRules__"
                              || (m.Tag as string) == "__ActionStatusMenu__"
                              || (m.Tag as string) == "__SetFirstClaimToday__"
                              || (m.Tag as string) == "__OpenGrouped__").ToList())
                    {
                        cm.Items.Remove(mi);
                    }
                    foreach (var sp in cm.Items.OfType<Separator>().Where(s => (s.Tag as string) == "__InjectedSep__").ToList())
                    {
                        cm.Items.Remove(sp);
                    }

                    cm.Items.Add(new Separator { Tag = "__InjectedSep__" });
                    
                    // Add "Open Other Account grouped lines" if counterpart exists
                    if (rowData.CounterpartCount.HasValue && rowData.CounterpartCount.Value > 0)
                    {
                        var openGroupedItem = new MenuItem { Header = "Open Other Account grouped lines", Tag = "__OpenGrouped__", DataContext = rowData };
                        openGroupedItem.Click += (s2, e2) =>
                        {
                            try
                            {
                                OpenMatchedPopup(rowData);
                            }
                            catch { }
                        };
                        cm.Items.Add(openGroupedItem);
                    }
                    
                    var takeItem = new MenuItem { Header = "Take (Assign to me)", Tag = "__Take__", DataContext = rowData };
                    takeItem.Click += QuickTakeMenuItem_Click;
                    cm.Items.Add(takeItem);
                    var reminderItem = new MenuItem { Header = "Set Reminder Date…", Tag = "__SetReminder__", DataContext = rowData };
                    reminderItem.Click += QuickSetReminderMenuItem_Click;
                    cm.Items.Add(reminderItem);
                    var actionStatusMenu = new MenuItem { Header = "Set Action Status", Tag = "__ActionStatusMenu__" };
                    var doneItem = new MenuItem { Header = "DONE", Tag = "__MarkActionDone__", DataContext = rowData };
                    doneItem.Click += QuickMarkActionDoneMenuItem_Click;
                    var pendingItem = new MenuItem { Header = "PENDING", Tag = "__MarkActionPending__", DataContext = rowData };
                    pendingItem.Click += QuickMarkActionPendingMenuItem_Click;
                    actionStatusMenu.Items.Add(doneItem);
                    actionStatusMenu.Items.Add(pendingItem);
                    cm.Items.Add(actionStatusMenu);
                    var firstClaimToday = new MenuItem { Header = "Set First Claim Date = Today", Tag = "__SetFirstClaimToday__", DataContext = rowData };
                    firstClaimToday.Click += QuickSetFirstClaimTodayMenuItem_Click;
                    cm.Items.Add(firstClaimToday);
                    var addRuleItem = new MenuItem { Header = "Add Rule based on this line…", Tag = "__AddRuleFromLine__", DataContext = rowData };
                    addRuleItem.Click += QuickAddRuleFromLineMenuItem_Click;
                    cm.Items.Add(addRuleItem);
                    var commentItem = new MenuItem { Header = "Set Comment…", Tag = "__SetComment__" };
                    commentItem.Click += QuickSetCommentMenuItem_Click;
                    cm.Items.Add(commentItem);

                    // Copy submenu (ID / BGI Ref / All line with header)
                    var copyRoot = new MenuItem { Header = "Copy", Tag = "__Copy__" };
                    var copyId = new MenuItem { Header = "ID" };
                    copyId.Click += (s2, e2) => CopySelectionIds();
                    var copyDwInvoice = new MenuItem { Header = "DWINGS Invoice ID (BGI)" };
                    copyDwInvoice.Click += (s2, e2) => CopySelectionDwInvoiceId();
                    var copyDwBgpmt = new MenuItem { Header = "DWINGS BGPMT" };
                    copyDwBgpmt.Click += (s2, e2) => CopySelectionDwCommissionBgpmt();
                    var copyDwGuarantee = new MenuItem { Header = "DWINGS Guarantee ID" };
                    copyDwGuarantee.Click += (s2, e2) => CopySelectionDwGuaranteeId();
                    var copyAll = new MenuItem { Header = "All line (with header)" };
                    copyAll.Click += (s2, e2) => CopySelectionAllLines(includeHeader: true);
                    copyRoot.Items.Add(copyId);
                    copyRoot.Items.Add(copyDwInvoice);
                    copyRoot.Items.Add(copyDwBgpmt);
                    copyRoot.Items.Add(copyDwGuarantee);
                    copyRoot.Items.Add(new Separator());
                    copyRoot.Items.Add(copyAll);
                    cm.Items.Add(copyRoot);

                    // Search BGI in DWINGS (open or reuse Invoice Finder)
                    var searchBgi = new MenuItem { Header = "Search BGI…", Tag = "__SearchBGI__", DataContext = rowData };
                    searchBgi.Click += SearchBgiMenuItem_Click;
                    cm.Items.Add(searchBgi);

                    // Debug Rules - show detailed rule evaluation
                    cm.Items.Add(new Separator { Tag = "__InjectedSep__" });
                    var debugRulesItem = new MenuItem { Header = "Debug Rules…", Tag = "__DebugRules__", DataContext = rowData };
                    debugRulesItem.Click += DebugRulesMenuItem_Click;
                    cm.Items.Add(debugRulesItem);
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
                // Capture initial filter state for reset button
                _initialFilterSql = sql;
                
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
        }

        /// <summary>
        /// Sets the TodoList session tracker and TodoList ID for multi-user awareness
        /// </summary>
        public void SetTodoSessionTracker(TodoListSessionTracker tracker, int todoId)
        {
            _todoSessionTracker = tracker;
            _currentTodoId = todoId;

            // Setup refresh timer for multi-user warning
            SetupMultiUserWarningRefreshTimer();

            // Subscribe to rule application events to show floating toasts (edit/run-now)
            try
            {
                if (_reconciliationService != null)
                {
                    _reconciliationService.RuleApplied -= ReconciliationService_RuleApplied;
                    _reconciliationService.RuleApplied += ReconciliationService_RuleApplied;
                }
            }
            catch { }

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

        // Open "Manage columns" when user right-clicks a column header
        private void ResultsDataGrid_PreviewMouseRightButtonUp(object sender, MouseButtonEventArgs e)
        {
            try
            {
                var dg = sender as DataGrid;
                if (dg == null) return;

                var source = e.OriginalSource as DependencyObject;
                var header = VisualTreeHelpers.FindParent<DataGridColumnHeader>(source);
                if (header == null)
                {
                    // Not a header: let row context menu proceed
                    return;
                }

                var win = new ManageColumnsWindow(dg);
                try { win.Owner = Window.GetWindow(this); } catch { }
                win.ShowDialog();
                e.Handled = true;
            }
            catch { }
        }

        private void RulesAdmin_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var win = new RulesAdminWindow();
                var owner = Window.GetWindow(this);
                if (owner != null) win.Owner = owner;
                win.Show();
            }
            catch { }
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

        private void ReconciliationService_RuleApplied(object sender, ReconciliationService.RuleAppliedEventArgs e)
        {
            try
            {
                if (e == null) return;
                if (!string.Equals(e.Origin, "edit", StringComparison.OrdinalIgnoreCase)
                    && !string.Equals(e.Origin, "run-now", StringComparison.OrdinalIgnoreCase))
                {
                    return;
                }
                if (!Dispatcher.CheckAccess())
                {
                    Dispatcher.Invoke(() => ReconciliationService_RuleApplied(sender, e));
                    return;
                }
                // Real-time update of the affected row (if visible)
                try
                {
                    var row = (_allViewData ?? new List<ReconciliationViewData>()).FirstOrDefault(r => string.Equals(r?.ID, e.ReconciliationId, StringComparison.OrdinalIgnoreCase));
                    if (row != null && !string.IsNullOrWhiteSpace(e.Outputs))
                    {
                        ApplyOutputsToRow(row, e.Outputs);
                    }
                }
                catch { }

                var summary = !string.IsNullOrWhiteSpace(e.Outputs) ? e.Outputs : e.Message;
                var text = string.IsNullOrWhiteSpace(summary)
                    ? $"Rule '{e.RuleId}' applied"
                    : $"Rule '{e.RuleId}' applied: {summary}";
                ShowToast(text, onClick: () =>
                {
                    try { OpenSingleReconciliationPopup(e.ReconciliationId); } catch { }
                });
            }
            catch { }
        }

        private void ShowToast(string text, Action onClick = null, int durationSeconds = 5)
        {
            try
            {
                var panel = this.FindName("ToastPanel") as Border;
                var tb = this.FindName("ToastText") as TextBlock;
                if (panel == null || tb == null) return;
                
                tb.Text = text ?? string.Empty;
                _toastClickAction = onClick;
                _toastTargetReconciliationId = null;

                // Fade in animation
                panel.Opacity = 0;
                panel.Visibility = Visibility.Visible;
                var fadeIn = new System.Windows.Media.Animation.DoubleAnimation
                {
                    From = 0,
                    To = 1,
                    Duration = TimeSpan.FromMilliseconds(300)
                };
                panel.BeginAnimation(Border.OpacityProperty, fadeIn);

                if (_toastTimer == null)
                {
                    _toastTimer = new DispatcherTimer { Interval = TimeSpan.FromSeconds(durationSeconds) };
                    _toastTimer.Tick += (s, e) =>
                    {
                        try 
                        { 
                            // Fade out animation
                            var fadeOut = new System.Windows.Media.Animation.DoubleAnimation
                            {
                                From = 1,
                                To = 0,
                                Duration = TimeSpan.FromMilliseconds(300)
                            };
                            fadeOut.Completed += (_, __) => { panel.Visibility = Visibility.Collapsed; };
                            panel.BeginAnimation(Border.OpacityProperty, fadeOut);
                        } 
                        catch { }
                        finally { _toastTimer?.Stop(); }
                    };
                }
                else
                {
                    _toastTimer.Stop();
                    _toastTimer.Interval = TimeSpan.FromSeconds(durationSeconds);
                }
                _toastTimer.Start();
            }
            catch { }
        }

        private void ToastPanel_MouseLeftButtonUp(object sender, MouseButtonEventArgs e)
        {
            try
            {
                _toastClickAction?.Invoke();
            }
            catch { }
            finally
            {
                try
                {
                    var panel = this.FindName("ToastPanel") as Border;
                    if (panel != null) panel.Visibility = Visibility.Collapsed;
                }
                catch { }
            }
        }

        private void OpenSingleReconciliationPopup(string reconciliationId)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(reconciliationId)) return;
                var where = $"r.ID = '{reconciliationId.Replace("'", "''")}'";

                var targetView = new ReconciliationView(_reconciliationService, _offlineFirstService);
                try { targetView.SyncCountryFromService(refresh: false); } catch { }
                try { targetView.ApplySavedFilterSql(where); } catch { }
                try { targetView.Refresh(); } catch { }

                var win = new Window
                {
                    Title = $"Reconciliation - {reconciliationId}",
                    Content = targetView,
                    Owner = Window.GetWindow(this),
                    Width = 1100,
                    Height = 750,
                    WindowStartupLocation = WindowStartupLocation.CenterOwner
                };
                targetView.CloseRequested += (s, e) => { try { win.Close(); } catch { } };
                win.Show();
            }
            catch { }
        }

        private void ApplyOutputsToRow(ReconciliationViewData row, string outputs)
        {
            try
            {
                if (row == null || string.IsNullOrWhiteSpace(outputs)) return;
                var parts = outputs.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries);
                int? newActionId = null;
                foreach (var p in parts)
                {
                    var kv = p.Split(new[] { '=' }, 2);
                    if (kv.Length != 2) continue;
                    var key = kv[0].Trim();
                    var val = kv[1].Trim();
                    switch (key)
                    {
                        case "Action":
                            if (int.TryParse(val, out var a)) { row.Action = a; newActionId = a; }
                            break;
                        case "KPI":
                            if (int.TryParse(val, out var k)) row.KPI = k; break;
                        case "IncidentType":
                            if (int.TryParse(val, out var it)) row.IncidentType = it; break;
                        case "RiskyItem":
                            if (bool.TryParse(val, out var rb)) row.RiskyItem = rb; break;
                        case "ReasonNonRisky":
                            if (int.TryParse(val, out var rn)) row.ReasonNonRisky = rn; break;
                        case "ToRemind":
                            if (bool.TryParse(val, out var tr)) row.ToRemind = tr; break;
                        case "ToRemindDays":
                            if (int.TryParse(val, out var td))
                            {
                                try { row.ToRemindDate = DateTime.Today.AddDays(td); } catch { }
                            }
                            break;
                    }
                }

                // If Action was set, ensure status is PENDING and date is today, unless action is N/A
                if (newActionId.HasValue)
                {
                    try
                    {
                        var all = AllUserFields;
                        var isNA = UserFieldUpdateService.IsActionNA(newActionId.Value, all);
                        if (!isNA)
                        {
                            if (row.ActionStatus != false) row.ActionStatus = false; // PENDING
                            row.ActionDate = DateTime.Now;
                        }
                        else
                        {
                            row.ActionStatus = null;
                            row.ActionDate = null;
                        }
                    }
                    catch { }
                }
            }
            catch { }
        }

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
        

        #region Bound Filter Properties

        public string FilterAccountId { get => VM.FilterAccountId; set { VM.FilterAccountId = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterAccountId)); ScheduleApplyFiltersDebounced(); } }
        public string FilterCurrency { get => VM.FilterCurrency; set { VM.FilterCurrency = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterCurrency)); ScheduleApplyFiltersDebounced(); } }

        public string FilterCountry { get => _filterCountry; set { _filterCountry = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterCountry)); ScheduleApplyFiltersDebounced(); } }
        public DateTime? FilterFromDate { get => VM.FilterFromDate; set { VM.FilterFromDate = value; OnPropertyChanged(nameof(FilterFromDate)); ScheduleApplyFiltersDebounced(); } }
        public DateTime? FilterToDate { get => VM.FilterToDate; set { VM.FilterToDate = value; OnPropertyChanged(nameof(FilterToDate)); ScheduleApplyFiltersDebounced(); } }
        public DateTime? FilterOperationDate { get => VM.FilterOperationDate; set { VM.FilterOperationDate = value; OnPropertyChanged(nameof(FilterOperationDate)); ScheduleApplyFiltersDebounced(); } }
        public DateTime? FilterDeletedDate { get => VM.FilterDeletedDate; set { VM.FilterDeletedDate = value; OnPropertyChanged(nameof(FilterDeletedDate)); ScheduleApplyFiltersDebounced(); } }
        public string FilterAmount { get => VM.FilterAmount; set { VM.FilterAmount = value; OnPropertyChanged(nameof(FilterAmount)); ScheduleApplyFiltersDebounced(); } }
        public string FilterReconciliationNum { get => VM.FilterReconciliationNum; set { VM.FilterReconciliationNum = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterReconciliationNum)); ScheduleApplyFiltersDebounced(); } }
        public string FilterRawLabel { get => VM.FilterRawLabel; set { VM.FilterRawLabel = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterRawLabel)); ScheduleApplyFiltersDebounced(); } }
        public string FilterEventNum { get => VM.FilterEventNum; set { VM.FilterEventNum = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterEventNum)); ScheduleApplyFiltersDebounced(); } }
        public string FilterComments { get => VM.FilterComments; set { VM.FilterComments = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterComments)); ScheduleApplyFiltersDebounced(); } }
        public string FilterClient { get => VM.FilterClient; set { VM.FilterClient = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterClient)); ScheduleApplyFiltersDebounced(); } }
        public string FilterDwGuaranteeId { get => VM.FilterDwGuaranteeId; set { VM.FilterDwGuaranteeId = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterDwGuaranteeId)); ScheduleApplyFiltersDebounced(); } }
        public string FilterDwCommissionId { get => VM.FilterDwCommissionId; set { VM.FilterDwCommissionId = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterDwCommissionId)); ScheduleApplyFiltersDebounced(); } }
        public string FilterDwInvoiceId { get => VM.FilterDwInvoiceId; set { VM.FilterDwInvoiceId = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterDwInvoiceId)); ScheduleApplyFiltersDebounced(); } }
        public string FilterDwRef { get => VM.FilterDwRef; set { VM.FilterDwRef = string.IsNullOrWhiteSpace(value) ? null : value; OnPropertyChanged(nameof(FilterDwRef)); ScheduleApplyFiltersDebounced(); } }
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

        public DateTime? FilterActionDate
        {
            get => VM.FilterActionDate;
            set { VM.FilterActionDate = value; OnPropertyChanged(nameof(FilterActionDate)); ScheduleApplyFiltersDebounced(); }
        }

        public bool? FilterToRemind
        {
            get => VM.FilterToRemind;
            set { VM.FilterToRemind = value; OnPropertyChanged(nameof(FilterToRemind)); ScheduleApplyFiltersDebounced(); }
        }

        public DateTime? FilterRemindDate
        {
            get => VM.FilterRemindDate;
            set { VM.FilterRemindDate = value; OnPropertyChanged(nameof(FilterRemindDate)); ScheduleApplyFiltersDebounced(); }
        }

        public string FilterLastReviewed
        {
            get => VM.FilterLastReviewed;
            set { VM.FilterLastReviewed = value == "All" ? null : value; OnPropertyChanged(nameof(FilterLastReviewed)); ScheduleApplyFiltersDebounced(); }
        }

        #endregion


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
            catch { }
        }

        // --- Copy helpers ---
        private List<ReconciliationViewData> GetCurrentSelection()
        {
            try
            {
                var dg = this.FindName("ResultsDataGrid") as DataGrid;
                if (dg == null) return new List<ReconciliationViewData>();
                var items = dg.SelectedItems?.Cast<ReconciliationViewData>().ToList() ?? new List<ReconciliationViewData>();
                if (items.Count == 0 && dg.CurrentItem is ReconciliationViewData one) items.Add(one);
                return items;
            }
            catch { return new List<ReconciliationViewData>(); }
        }

        private void CopySelectionIds()
        {
            try
            {
                var items = GetCurrentSelection();
                if (items.Count == 0) return;
                var sb = new StringBuilder();
                foreach (var it in items)
                {
                    sb.AppendLine(it?.ID ?? string.Empty);
                }
                Clipboard.SetText(sb.ToString());
            }
            catch { }
        }

        private void CopySelectionDwInvoiceId()
        {
            try
            {
                var items = GetCurrentSelection();
                if (items.Count == 0) return;
                var sb = new StringBuilder();
                foreach (var it in items)
                    sb.AppendLine(it?.DWINGS_InvoiceID ?? string.Empty);
                Clipboard.SetText(sb.ToString());
            }
            catch { }
        }

        private void CopySelectionDwCommissionBgpmt()
        {
            try
            {
                var items = GetCurrentSelection();
                if (items.Count == 0) return;
                var sb = new StringBuilder();
                foreach (var it in items)
                    sb.AppendLine(it?.DWINGS_BGPMT ?? string.Empty);
                Clipboard.SetText(sb.ToString());
            }
            catch { }
        }

        private void CopySelectionDwGuaranteeId()
        {
            try
            {
                var items = GetCurrentSelection();
                if (items.Count == 0) return;
                var sb = new StringBuilder();
                foreach (var it in items)
                    sb.AppendLine(it?.DWINGS_GuaranteeID ?? string.Empty);
                Clipboard.SetText(sb.ToString());
            }
            catch { }
        }

        private void SearchBgiMenuItem_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var row = GetCurrentSelection().FirstOrDefault();
                if (row == null) return;

                // Find existing window or create new
                var existing = Application.Current?.Windows?.OfType<InvoiceFinderWindow>()?.FirstOrDefault();
                InvoiceFinderWindow win = existing ?? new InvoiceFinderWindow();
                if (win.Owner == null)
                {
                    try { win.Owner = Window.GetWindow(this); } catch { }
                }
                try { win.Show(); } catch { }
                try { win.Activate(); } catch { }

                if (!string.IsNullOrWhiteSpace(row.DWINGS_InvoiceID))
                {
                    try { win.SetSearchInvoiceId(row.DWINGS_InvoiceID); } catch { }
                }
                else if (!string.IsNullOrWhiteSpace(row.DWINGS_GuaranteeID))
                {
                    try { win.SetSearchGuaranteeId(row.DWINGS_GuaranteeID); } catch { }
                }
            }
            catch { }
        }

        private async void DebugRulesMenuItem_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var menuItem = sender as MenuItem;
                var rowData = menuItem?.DataContext as ReconciliationViewData;
                if (rowData == null || string.IsNullOrWhiteSpace(rowData.ID)) return;

                if (_reconciliationService == null)
                {
                    MessageBox.Show("Reconciliation service not available.", "Error", MessageBoxButton.OK, MessageBoxImage.Warning);
                    return;
                }

                // Get debug information from the service
                var (context, evaluations) = await _reconciliationService.GetRuleDebugInfoAsync(rowData.ID);
                if (context == null || evaluations == null || evaluations.Count == 0)
                {
                    MessageBox.Show("Unable to load rule debug information for this line.", "Debug Rules", MessageBoxButton.OK, MessageBoxImage.Information);
                    return;
                }

                // Build summary strings
                var lineInfo = $"ID: {rowData.ID}  |  Account: {rowData.Account_ID}  |  Amount: {rowData.SignedAmount:N2}";
                var contextInfo = $"IsPivot: {context.IsPivot}  |  Country: {context.CountryId}  |  " +
                                 $"TransactionType: {context.TransactionType ?? "(null)"}  |  " +
                                 $"GuaranteeType: {context.GuaranteeType ?? "(null)"}  |  " +
                                 $"IsGrouped: {context.IsGrouped?.ToString() ?? "(null)"}  |  " +
                                 $"HasDwingsLink: {context.HasDwingsLink?.ToString() ?? "(null)"}";

                // Convert to display items
                var debugItems = new List<RuleDebugItem>();
                int displayOrder = 1;
                foreach (var eval in evaluations)
                {
                    var item = new RuleDebugItem
                    {
                        DisplayOrder = displayOrder++,
                        RuleName = eval.Rule.RuleId ?? "(unnamed)",
                        IsEnabled = eval.IsEnabled,
                        IsMatch = eval.IsMatch,
                        MatchStatus = eval.IsMatch ? "✓ MATCH" : (eval.IsEnabled ? "✗ No Match" : "⊘ Disabled"),
                        OutputAction = eval.Rule.OutputActionId.HasValue 
                            ? EnumHelper.GetActionName(eval.Rule.OutputActionId.Value, _offlineFirstService?.UserFields) 
                            : "-",
                        OutputKPI = eval.Rule.OutputKpiId.HasValue 
                            ? EnumHelper.GetKPIName(eval.Rule.OutputKpiId.Value, _offlineFirstService?.UserFields) 
                            : "-",
                        Conditions = eval.Conditions.Select(c => new ConditionDebugItem
                        {
                            Field = c.Field,
                            Expected = c.Expected,
                            Actual = c.Actual,
                            IsMet = c.IsMet,
                            Status = c.IsMet ? "✓" : "✗"
                        }).ToList()
                    };
                    debugItems.Add(item);
                }

                // Create and show the debug window
                var debugWindow = new RuleDebugWindow();
                debugWindow.SetDebugInfo(lineInfo, contextInfo, debugItems);
                try { debugWindow.Owner = Window.GetWindow(this); } catch { }
                debugWindow.Show();
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Error displaying rule debug information: {ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        private void CopySelectionBgiRef()
        {
            try
            {
                var dg = this.FindName("ResultsDataGrid") as DataGrid;
                var items = GetCurrentSelection();
                if (dg == null || items.Count == 0) return;

                // Find the column bound to the displayed "BGI Ref" header
                string headerName = "BGI Ref";
                var col = dg.Columns.FirstOrDefault(c => string.Equals(c.Header?.ToString(), headerName, StringComparison.OrdinalIgnoreCase)) as DataGridBoundColumn;
                // Fallback to data property if column not found
                string path = null;
                if (col?.Binding is Binding b && b.Path != null) path = b.Path.Path;
                if (string.IsNullOrWhiteSpace(path)) path = nameof(ReconciliationViewData.Receivable_DWRefFromAmbre);

                var sb = new StringBuilder();
                foreach (var it in items)
                {
                    sb.AppendLine(GetPropertyString(it, path));
                }
                Clipboard.SetText(sb.ToString());
            }
            catch { }
        }

        private void CopySelectionAllLines(bool includeHeader)
        {
            try
            {
                var dg = this.FindName("ResultsDataGrid") as DataGrid;
                var items = GetCurrentSelection();
                if (dg == null || items.Count == 0) return;

                // Build list of exportable columns in current display order
                var cols = dg.Columns
                    .Where(c => c.Visibility == Visibility.Visible)
                    .Where(c => !string.IsNullOrWhiteSpace(c.Header?.ToString()))
                    .OfType<DataGridBoundColumn>()
                    .ToList();

                var sb = new StringBuilder();
                // Header
                if (includeHeader)
                {
                    sb.AppendLine(string.Join("\t", cols.Select(c => c.Header?.ToString()?.Trim())));
                }

                foreach (var it in items)
                {
                    var values = cols.Select(c =>
                    {
                        var p = (c.Binding as Binding)?.Path?.Path;
                        return GetPropertyString(it, p);
                    });
                    sb.AppendLine(string.Join("\t", values));
                }

                Clipboard.SetText(sb.ToString());
            }
            catch { }
        }

        private string GetPropertyString(object obj, string path)
        {
            if (obj == null || string.IsNullOrWhiteSpace(path)) return string.Empty;
            try
            {
                var prop = obj.GetType().GetProperty(path);
                if (prop == null) return string.Empty;
                var val = prop.GetValue(obj);
                if (val == null) return string.Empty;
                // Format dates like grid
                if (val is DateTime dt) return dt.ToString("dd/MM/yyyy", CultureInfo.InvariantCulture);
                if (val is DateTime ndt) return ndt.ToString("dd/MM/yyyy", CultureInfo.InvariantCulture);
                if (val is decimal dec) return dec.ToString("N2", CultureInfo.InvariantCulture);
                if (val is decimal ndec) return ndec.ToString("N2", CultureInfo.InvariantCulture);
                return Convert.ToString(val, CultureInfo.InvariantCulture) ?? string.Empty;
            }
            catch { return string.Empty; }
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

       

        #region Multi-User Session Checks

        /// <summary>
        /// Checks if another user is editing the current TodoList and warns before modification.
        /// Automatically marks the current user as editing and tracks activity.
        /// </summary>
        /// <returns>True if safe to proceed, False if user cancelled</returns>
        private async Task<bool> CheckMultiUserBeforeEditAsync()
        {
            try
            {
                if (_todoSessionTracker == null || _currentTodoId <= 0)
                    return true; // No tracking active, proceed

                var sessions = await _todoSessionTracker.GetActiveSessionsAsync(_currentTodoId);
                if (sessions == null || !sessions.Any())
                {
                    // No other users, but still notify edit activity
                    await _todoSessionTracker.NotifyEditActivityAsync(_currentTodoId);
                    return true;
                }

                // Check if anyone is editing (excluding current user)
                var currentUserId = Environment.UserName;
                var otherEditingSessions = sessions.Where(s => 
                    s.IsEditing && 
                    !string.Equals(s.UserId, currentUserId, StringComparison.OrdinalIgnoreCase))
                    .ToList();

                if (otherEditingSessions.Any())
                {
                    // Show warning dialog
                    var result = await MultiUserHelper.ShowEditWarningAsync(otherEditingSessions);
                    if (!result)
                        return false; // User cancelled
                }

                // Notify edit activity (marks as editing and updates timestamp)
                await _todoSessionTracker.NotifyEditActivityAsync(_currentTodoId);

                return true;
            }
            catch
            {
                // Best effort, don't block on error
                return true;
            }
        }

        /// <summary>
        /// Setup timer to refresh multi-user warning banner
        /// </summary>
        private void SetupMultiUserWarningRefreshTimer()
        {
            _multiUserWarningRefreshTimer?.Stop();
            _multiUserWarningRefreshTimer = new DispatcherTimer
            {
                Interval = TimeSpan.FromSeconds(30) // Refresh every 30 seconds (reduced frequency to improve UI performance)
            };
            _multiUserWarningRefreshTimer.Tick += async (s, e) => await UpdateMultiUserWarningAsync();
            _multiUserWarningRefreshTimer.Start();

            // Initial update
            _ = UpdateMultiUserWarningAsync();
        }

        /// <summary>
        /// Updates the multi-user warning banner based on active sessions
        /// </summary>
        private async Task UpdateMultiUserWarningAsync()
        {
            try
            {
                if (_todoSessionTracker == null || _currentTodoId <= 0)
                {
                    // Hide warning if no tracking (only update if currently visible)
                    if (MultiUserWarningBanner.Visibility == Visibility.Visible)
                    {
                        Dispatcher.Invoke(() => MultiUserWarningBanner.Visibility = Visibility.Collapsed);
                    }
                    return;
                }

                var sessions = await _todoSessionTracker.GetActiveSessionsAsync(_currentTodoId);
                var currentUserId = Environment.UserName;

                // Filter out current user
                var otherSessions = sessions?.Where(s => 
                    !string.Equals(s.UserId, currentUserId, StringComparison.OrdinalIgnoreCase))
                    .ToList();

                if (otherSessions == null || !otherSessions.Any())
                {
                    // No other users, hide warning (only update if currently visible)
                    if (MultiUserWarningBanner.Visibility == Visibility.Visible)
                    {
                        Dispatcher.Invoke(() => MultiUserWarningBanner.Visibility = Visibility.Collapsed);
                    }
                    return;
                }

                // Check if anyone is editing
                var editingSessions = otherSessions.Where(s => s.IsEditing).ToList();
                var viewingSessions = otherSessions.Where(s => !s.IsEditing).ToList();

                // Prepare UI updates on background thread
                string newTag = null;
                string newText = null;
                bool shouldShow = false;

                if (editingSessions.Any())
                {
                    newTag = "Editing";
                    var userNames = string.Join(", ", editingSessions.Select(s => s.UserName));
                    newText = editingSessions.Count == 1
                        ? $"{userNames} is currently editing this TodoList"
                        : $"{userNames} are currently editing this TodoList";
                    shouldShow = true;
                }
                else if (viewingSessions.Any())
                {
                    newTag = "Viewing";
                    var userNames = string.Join(", ", viewingSessions.Select(s => s.UserName));
                    newText = viewingSessions.Count == 1
                        ? $"{userNames} is viewing this TodoList"
                        : $"{userNames} are viewing this TodoList";
                    shouldShow = true;
                }

                // Single Dispatcher call with all updates
                Dispatcher.Invoke(() =>
                {
                    if (shouldShow)
                    {
                        MultiUserWarningBanner.Tag = newTag;
                        MultiUserWarningText.Text = newText;
                        MultiUserWarningBanner.Visibility = Visibility.Visible;
                    }
                    else
                    {
                        MultiUserWarningBanner.Visibility = Visibility.Collapsed;
                    }
                });
            }
            catch
            {
                // Best effort, don't crash on error
            }
        }

        #endregion

        #region Status Filtering

        private string _activeStatusFilter = null;

        private void KpiFilter_Click(object sender, MouseButtonEventArgs e)
        {
            try
            {
                var border = sender as System.Windows.Controls.Border;
                if (border == null) return;

                var filterType = border.Tag as string;
                if (string.IsNullOrEmpty(filterType)) return;

                // Toggle filter: if same filter is active, clear it; otherwise set new filter
                if (_activeStatusFilter == filterType)
                {
                    _activeStatusFilter = null;
                    ShowToast("Filter cleared");
                }
                else
                {
                    _activeStatusFilter = filterType;
                    ShowToast($"Filtering by: {filterType}");
                }

                // Update visual indication
                UpdateKpiFilterVisuals();
                ApplyStatusFilter();
            }
            catch (Exception ex)
            {
                ShowToast($"Error applying KPI filter: {ex.Message}");
            }
        }
        private void UpdateKpiFilterVisuals()
        {
            try
            {
                // Reset all KPI borders to default
                ResetKpiBorder("KpiToReviewBorder", "#FFEDD5", 1);
                ResetKpiBorder("KpiToRemindBorder", "#FFCC80", 1);
                ResetKpiBorder("KpiReviewedBorder", "#D1FAE5", 1);
                ResetKpiBorder("KpiNotLinkedBorder", "#EF9A9A", 1);
                ResetKpiBorder("KpiNotGroupedBorder", "#FFCC80", 1);
                ResetKpiBorder("KpiHasDifferencesBorder", "#FFF59D", 1);
                ResetKpiBorder("KpiMatchedBorder", "#A5D6A7", 1);

                // Highlight active filter
                if (!string.IsNullOrEmpty(_activeStatusFilter))
                {
                    var borderName = _activeStatusFilter switch
                    {
                        "ToReview" => "KpiToReviewBorder",
                        "ToRemind" => "KpiToRemindBorder",
                        "Reviewed" => "KpiReviewedBorder",
                        "NotLinked" => "KpiNotLinkedBorder",
                        "NotGrouped" => "KpiNotGroupedBorder",
                        "HasDifferences" => "KpiHasDifferencesBorder",
                        "Matched" => "KpiMatchedBorder",
                        _ => null
                    };

                    if (borderName != null)
                    {
                        var border = this.FindName(borderName) as System.Windows.Controls.Border;
                        if (border != null)
                        {
                            border.BorderThickness = new Thickness(3);
                            border.BorderBrush = new SolidColorBrush(Color.FromRgb(33, 150, 243)); // Blue highlight
                        }
                    }
                }
            }
            catch { }
        }

        private void ResetKpiBorder(string borderName, string defaultColor, double thickness)
        {
            try
            {
                var border = this.FindName(borderName) as System.Windows.Controls.Border;
                if (border != null)
                {
                    border.BorderThickness = new Thickness(thickness);
                    border.BorderBrush = new SolidColorBrush((Color)ColorConverter.ConvertFromString(defaultColor));
                }
            }
            catch { }
        }

        private void ApplyStatusFilter()
        {
            try
            {
                // Simply trigger the existing filter system
                // The status filter will be applied in ApplyFilters via VM.ApplyFilters
                ApplyFilters();
            }
            catch (Exception ex)
            {
                ShowToast($"Error applying status filter: {ex.Message}");
            }
        }

        /// <summary>
        /// Filter predicate for status-based filtering (called by VM.ApplyFilters if needed)
        /// </summary>
        private bool MatchesStatusFilter(ReconciliationViewData row)
        {
            if (string.IsNullOrEmpty(_activeStatusFilter)) return true;

            var color = row.StatusColor;
            return _activeStatusFilter switch
            {
                "ToReview" => !row.IsReviewed,
                "ToRemind" => row.HasActiveReminder, // Active reminders (ToRemind = true and ToRemindDate <= today)
                "Reviewed" => row.IsReviewed,
                "NotLinked" => color == "#F44336", // Red - No DWINGS link
                "NotGrouped" => !row.IsMatchedAcrossAccounts, // NOT grouped (no "G" in grid)
                "HasDifferences" => color == "#FFC107" || color == "#FF6F00", // Yellow or Dark Amber
                "Discrepancy" => color == "#FFC107" || color == "#FF6F00", // Yellow or Dark Amber (legacy)
                "Matched" => color == "#4CAF50", // Green - Balanced and grouped
                "Balanced" => color == "#4CAF50", // Green (legacy)
                _ => true
            };
        }

        #endregion

        #endregion
    }
}