using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Data;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using LiveCharts;
using LiveCharts.Wpf;
using Microsoft.Extensions.DependencyInjection;
using RecoTool.Models;
using RecoTool.Services;
using System.Reflection;
using System.Windows.Media;
using System.Text.Json;
using System.Text;
using System.Globalization;
using RecoTool.Services.DTOs;

namespace RecoTool.Windows
{
    /// <summary>
    /// Logique d'interaction pour HomePage.xaml
    /// Page d'accueil avec dashboard et KPI
    /// </summary>
    public partial class HomePage : UserControl, IRefreshable, INotifyPropertyChanged
    {
        #region Fields and Properties

        private ReconciliationService _reconciliationService;
        private OfflineFirstService _offlineFirstService;
        private KpiSnapshotService _kpiSnapshotService;
        private bool _isLoading;
        private bool _canRefresh = true;
        private List<ReconciliationViewData> _reconciliationViewData;
        private bool _usingSnapshot;
        private DateTime? _selectedSnapshotDate;
        private List<DateTime> _availableSnapshotDates;
        private Brush _defaultBackground;
        
        // Champs pour les propriétés de binding
        private List<Country> _availableCountries;
        private int _missingInvoicesCount;
        private int _paidButNotReconciledCount;
        private int _underInvestigationCount;
        private decimal _totalReceivableAmount;
        private int _receivableAccountsCount;
        private decimal _totalPivotAmount;
        private int _pivotAccountsCount;
        private SeriesCollection _currencyDistributionSeries;
        private SeriesCollection _actionDistributionSeries;
        private SeriesCollection _kpiRiskSeries;
        private string _statusMessage;
        private string _lastUpdateTime;
        
        // Champs pour les propriétés de graphiques manquantes
        private ChartValues<double> _receivableChartData;
        private ChartValues<double> _pivotChartData;
        private List<string> _actionLabels;
        private List<string> _kpiRiskLabels;
        private SeriesCollection _receivablePivotByActionSeries;
        private List<string> _receivablePivotByActionLabels;
        private SeriesCollection _deletionDelaySeries;
        private List<string> _deletionDelayLabels;
        private SeriesCollection _newDeletedDailySeries;
        private List<string> _newDeletedDailyLabels;

        public event PropertyChangedEventHandler PropertyChanged;

        /// <summary>
        /// Constructeur de la page d'accueil (pour le designer/XAML)
        /// </summary>
        public HomePage()
        {
            InitializeComponent();
            InitializeProperties();
            DataContext = this;
            _defaultBackground = MainScrollViewer?.Background;
        }

        private async void UserControl_Loaded(object sender, RoutedEventArgs e)
        {
            await RefreshSnapshotDatePickerAsync();
        }

        /// <summary>
        /// Load available snapshot dates for current country and restrict DatePicker to only those dates.
        /// </summary>
        private async Task RefreshSnapshotDatePickerAsync()
        {
            try
            {
                if ((_reconciliationService == null && _kpiSnapshotService == null) || string.IsNullOrWhiteSpace(CurrentCountryId) || SnapshotDatePicker == null)
                    return;

                if (_kpiSnapshotService == null && _offlineFirstService != null && _reconciliationService != null)
                    _kpiSnapshotService = new KpiSnapshotService(_offlineFirstService, _reconciliationService);
                var dates = await _kpiSnapshotService.GetKpiSnapshotDatesAsync(CurrentCountryId);
                _availableSnapshotDates = dates ?? new List<DateTime>();

                await Dispatcher.InvokeAsync(() =>
                {
                    // Reset picker config
                    SnapshotDatePicker.BlackoutDates.Clear();

                    if (_availableSnapshotDates.Count == 0)
                    {
                        SnapshotDatePicker.DisplayDateStart = null;
                        SnapshotDatePicker.DisplayDateEnd = null;
                        SnapshotDatePicker.IsEnabled = false;
                        return;
                    }

                    SnapshotDatePicker.IsEnabled = true;

                    var min = _availableSnapshotDates.Min();
                    var max = _availableSnapshotDates.Max();
                    SnapshotDatePicker.DisplayDateStart = min;
                    SnapshotDatePicker.DisplayDateEnd = max;
                    // Ensure calendar opens around a valid date
                    SnapshotDatePicker.DisplayDate = _selectedSnapshotDate ?? max;

                    // Build blackout ranges to exclude all days not present in the allowed set between min and max
                    var allowed = new HashSet<DateTime>(_availableSnapshotDates.Select(d => d.Date));
                    DateTime cursor = min.Date;
                    DateTime? blackoutStart = null;
                    while (cursor <= max.Date)
                    {
                        bool isAllowed = allowed.Contains(cursor);
                        if (!isAllowed && blackoutStart == null)
                        {
                            blackoutStart = cursor;
                        }
                        else if (isAllowed && blackoutStart != null)
                        {
                            // Close blackout range on previous day
                            var blackoutEnd = cursor.AddDays(-1);
                            if (blackoutEnd >= blackoutStart.Value)
                                SnapshotDatePicker.BlackoutDates.Add(new CalendarDateRange(blackoutStart.Value, blackoutEnd));
                            blackoutStart = null;
                        }
                        cursor = cursor.AddDays(1);
                    }
                    if (blackoutStart != null)
                    {
                        SnapshotDatePicker.BlackoutDates.Add(new CalendarDateRange(blackoutStart.Value, max.Date));
                    }

                    // Also blackout outside of [min, max] to prevent navigation selection
                    if (min.Date > DateTime.MinValue.Date)
                    {
                        SnapshotDatePicker.BlackoutDates.Add(new CalendarDateRange(DateTime.MinValue.Date, min.AddDays(-1).Date));
                    }
                    if (max.Date < DateTime.MaxValue.Date.AddDays(-2))
                    {
                        SnapshotDatePicker.BlackoutDates.Add(new CalendarDateRange(max.AddDays(1).Date, DateTime.MaxValue.Date.AddDays(-1)));
                    }

                    // If a selected date is not allowed anymore, clear it
                    if (_selectedSnapshotDate.HasValue && !(_availableSnapshotDates?.Contains(_selectedSnapshotDate.Value.Date) ?? false))
                    {
                        _selectedSnapshotDate = null;
                        SnapshotDatePicker.SelectedDate = null;
                    }
                });
            }
            catch
            {
                // Non-fatal UI constraint
            }
        }

        private void SnapshotDatePicker_DateValidationError(object sender, DatePickerDateValidationErrorEventArgs e)
        {
            e.ThrowException = false;
            if (sender is DatePicker dp)
            {
                dp.SelectedDate = null;
                StatusMessage = "Invalid date for snapshot. Please pick a highlighted date.";
            }
        }

        private async void SnapshotDatePicker_CalendarOpened(object sender, RoutedEventArgs e)
        {
            await RefreshSnapshotDatePickerAsync();
            // Align display to a valid date
            if (_availableSnapshotDates != null && _availableSnapshotDates.Count > 0)
            {
                var target = _selectedSnapshotDate ?? _availableSnapshotDates.Max();
                SnapshotDatePicker.DisplayDate = target;
            }
        }

        private async void SnapshotDatePicker_SelectedDateChanged(object sender, SelectionChangedEventArgs e)
        {
            try
            {
                _selectedSnapshotDate = (sender as DatePicker)?.SelectedDate;
                await RefreshAsync();
            }
            catch { }
        }

        private async void ClearSnapshotDate_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                _selectedSnapshotDate = null;
                _usingSnapshot = false;
                if (SnapshotDatePicker != null) SnapshotDatePicker.SelectedDate = null;
                await RefreshAsync();
            }
            catch { }
        }

        /// <summary>
        /// Durée moyenne avant suppression (réconciliation) par paliers: 0-14j, 15-30j, 1-3 mois, >3 mois
        /// </summary>
        private void UpdateDeletionDelayChart()
        {
            try
            {
                if (_reconciliationViewData == null || !_reconciliationViewData.Any())
                {
                    DeletionDelaySeries = new SeriesCollection();
                    DeletionDelayLabels = new List<string>();
                    return;
                }

                var items = _reconciliationViewData
                    .Where(r => r.CreationDate.HasValue && r.DeleteDate.HasValue)
                    .Select(r => (int)(r.DeleteDate.Value.Date - r.CreationDate.Value.Date).TotalDays)
                    .Where(d => d >= 0)
                    .ToList();

                var buckets = new[]
                {
                    new { Key = "0-14j", Min = 0, Max = 14 },
                    new { Key = "15-30j", Min = 15, Max = 30 },
                    new { Key = "1-3 mois", Min = 31, Max = 92 }, // approx 3 months = 92 days
                    new { Key = ">3 mois", Min = 93, Max = int.MaxValue }
                };

                var labels = new List<string>();
                var avgDaysValues = new ChartValues<double>();
                foreach (var b in buckets)
                {
                    var inBucket = items.Where(d => d >= b.Min && d <= b.Max).ToList();
                    double avg = inBucket.Any() ? inBucket.Average() : 0;
                    labels.Add(b.Key);
                    avgDaysValues.Add(avg);
                }

                DeletionDelayLabels = labels;
                DeletionDelaySeries = new SeriesCollection
                {
                    new ColumnSeries
                    {
                        Title = "Average duration (days)",
                        Values = avgDaysValues,
                        DataLabels = true,
                        LabelPoint = cp => (cp.Y > 0 ? cp.Y.ToString("N0") : string.Empty)
                    }
                };
            }
            catch (Exception ex)
            {
                ShowError($"Error updating 'Duration before reconciliation': {ex.Message}");
                DeletionDelaySeries = new SeriesCollection();
                DeletionDelayLabels = new List<string>();
            }
        }

        /// <summary>
        /// Récap quotidien: Nouveau vs Supprimé (Deleted), axe X = jours basé sur DeleteDate
        /// </summary>
        private void UpdateNewDeletedDailyChart()
        {
            try
            {
                if (_reconciliationViewData == null || !_reconciliationViewData.Any())
                {
                    NewDeletedDailySeries = new SeriesCollection();
                    NewDeletedDailyLabels = new List<string>();
                    return;
                }

                var deletions = _reconciliationViewData.Where(r => r.DeleteDate.HasValue).ToList();
                if (!deletions.Any())
                {
                    NewDeletedDailySeries = new SeriesCollection();
                    NewDeletedDailyLabels = new List<string>();
                    return;
                }

                var minDay = deletions.Min(r => r.DeleteDate.Value.Date);
                var maxDay = deletions.Max(r => r.DeleteDate.Value.Date);
                // Add a small margin of days around
                minDay = minDay.AddDays(-3);
                maxDay = maxDay.AddDays(3);

                var dayCount = (int)(maxDay - minDay).TotalDays + 1;
                var labels = new List<string>(dayCount);
                var newPerDay = new ChartValues<int>();
                var deletedPerDay = new ChartValues<int>();

                var creations = _reconciliationViewData.Where(r => r.CreationDate.HasValue).ToList();

                for (int i = 0; i < dayCount; i++)
                {
                    var day = minDay.AddDays(i);
                    labels.Add(day.ToString("dd/MM", CultureInfo.InvariantCulture));
                    int newCount = creations.Count(r => r.CreationDate.Value.Date == day);
                    int delCount = deletions.Count(r => r.DeleteDate.Value.Date == day);
                    newPerDay.Add(newCount);
                    deletedPerDay.Add(delCount);
                }

                NewDeletedDailyLabels = labels;
                NewDeletedDailySeries = new SeriesCollection
                {
                    new LineSeries { Title = "New", Values = newPerDay, PointGeometry = null },
                    new LineSeries { Title = "Deleted", Values = deletedPerDay, PointGeometry = null }
                };
            }
            catch (Exception ex)
            {
                ShowError($"Error updating 'New vs Deleted (day)': {ex.Message}");
                NewDeletedDailySeries = new SeriesCollection();
                NewDeletedDailyLabels = new List<string>();
            }
        }

        /// <summary>
        /// Met à jour le graphique empilé Receivable vs Pivot par Action
        /// </summary>
        private void UpdateReceivablePivotByActionChart()
        {
            try
            {
                if (_reconciliationViewData == null || !_reconciliationViewData.Any())
                {
                    ReceivablePivotByActionSeries = new SeriesCollection();
                    ReceivablePivotByActionLabels = new List<string>();
                    return;
                }

                var currentCountry = _offlineFirstService?.CurrentCountry;
                if (currentCountry == null)
                {
                    ReceivablePivotByActionSeries = new SeriesCollection();
                    ReceivablePivotByActionLabels = new List<string>();
                    return;
                }

                var receivableId = currentCountry.CNT_AmbreReceivable;
                var pivotId = currentCountry.CNT_AmbrePivot;

                // Resolve action labels from referential user fields (prefer USR_FieldName, then description)
                var userFields = _offlineFirstService?.UserFields ?? new List<UserField>();
                var actionFieldMap = userFields
                    .Where(u => string.Equals(u.USR_Category, "Action", StringComparison.OrdinalIgnoreCase))
                    .ToDictionary(u => u.USR_ID, u => u);

                var actions = _reconciliationViewData
                    .Where(r => r.Action.HasValue)
                    .Select(r => r.Action.Value)
                    .Distinct()
                    .OrderBy(a =>
                    {
                        if (actionFieldMap.TryGetValue(a, out var uf) && uf != null)
                            return string.IsNullOrWhiteSpace(uf.USR_FieldName) ? (uf.USR_FieldDescription ?? EnumHelper.GetActionName(a, _offlineFirstService?.UserFields)) : uf.USR_FieldName;
                        return EnumHelper.GetActionName(a, _offlineFirstService?.UserFields);
                    })
                    .ToList();

                var labels = actions.Select(a =>
                {
                    if (actionFieldMap.TryGetValue(a, out var uf) && uf != null)
                        return !string.IsNullOrWhiteSpace(uf.USR_FieldName) ? uf.USR_FieldName : (!string.IsNullOrWhiteSpace(uf.USR_FieldDescription) ? uf.USR_FieldDescription : EnumHelper.GetActionName(a, _offlineFirstService?.UserFields));
                    return EnumHelper.GetActionName(a, _offlineFirstService?.UserFields);
                }).ToList();

                var receivableValues = new ChartValues<int>();
                var pivotValues = new ChartValues<int>();

                foreach (var a in actions)
                {
                    var rCount = _reconciliationViewData.Count(x => x.Action == a && x.Account_ID == receivableId);
                    var pCount = _reconciliationViewData.Count(x => x.Action == a && x.Account_ID == pivotId);
                    receivableValues.Add(rCount);
                    pivotValues.Add(pCount);
                }

                var series = new SeriesCollection
                {
                    new StackedColumnSeries { Title = "Receivable", Values = receivableValues, DataLabels = true, LabelPoint = cp => cp.Y.ToString("N0") },
                    new StackedColumnSeries { Title = "Pivot", Values = pivotValues, DataLabels = true, LabelPoint = cp => cp.Y.ToString("N0") }
                };

                ReceivablePivotByActionLabels = labels;
                ReceivablePivotByActionSeries = series;
            }
            catch (Exception ex)
            {
                ShowError($"Error updating Receivable vs Pivot by Action: {ex.Message}");
                ReceivablePivotByActionSeries = new SeriesCollection();
                ReceivablePivotByActionLabels = new List<string>();
            }
        }

        /// <summary>
        /// Met à jour le graphique KPI × RiskyItem (stacked columns: Risky, NonRisky, Unknown)
        /// </summary>
        private void UpdateKpiRiskChart()
        {
            try
            {
                if (_reconciliationViewData == null || !_reconciliationViewData.Any())
                {
                    KpiRiskSeries = new SeriesCollection();
                    KpiRiskLabels = new List<string>();
                    return;
                }

                // Group by KPI; consider null as Non-Risky
                var grouped = _reconciliationViewData
                    .Where(r => r.KPI.HasValue)
                    .GroupBy(r => r.KPI.Value)
                    .OrderBy(g => g.Key)
                    .ToList();

                var labels = new List<string>();
                var riskyValues = new ChartValues<int>();
                var nonRiskyValues = new ChartValues<int>();
                var unknownValues = new ChartValues<int>();

                foreach (var g in grouped)
                {
                    labels.Add(EnumHelper.GetKPIName(g.Key, _offlineFirstService?.UserFields));
                    riskyValues.Add(g.Count(x => x.RiskyItem == true));
                    // null is treated as false
                    nonRiskyValues.Add(g.Count(x => x.RiskyItem != true));
                    // keep unknownValues zero to maintain variable integrity (not used in series)
                    unknownValues.Add(0);
                }

                var series = new SeriesCollection
                {
                    new StackedColumnSeries { Title = "Risky", Values = riskyValues, DataLabels = true, LabelPoint = cp => cp.Y.ToString("N0") },
                    new StackedColumnSeries { Title = "Non-Risky", Values = nonRiskyValues, DataLabels = true, LabelPoint = cp => cp.Y.ToString("N0") }
                };

                KpiRiskLabels = labels;
                KpiRiskSeries = series;
            }
            catch (Exception ex)
            {
                ShowError($"Error updating KPI × RiskyItem: {ex.Message}");
                KpiRiskSeries = new SeriesCollection();
                KpiRiskLabels = new List<string>();
            }
        }

        /// <summary>
        /// Constructeur injecté avec les services nécessaires
        /// </summary>
        public HomePage(OfflineFirstService offlineFirstService, ReconciliationService reconciliationService)
        {
            InitializeComponent();
            InitializeProperties();
            _offlineFirstService = offlineFirstService;
            _reconciliationService = reconciliationService;
            if (_offlineFirstService != null && _reconciliationService != null)
                _kpiSnapshotService = new KpiSnapshotService(_offlineFirstService, _reconciliationService);
            DataContext = this;
            _defaultBackground = MainScrollViewer?.Background;
        }

        /// <summary>
        /// Met à jour les services injectés (appelé après changement de country)
        /// </summary>
        /// <param name="offlineFirstService">Service OfflineFirst actualisé</param>
        /// <param name="reconciliationService">Service de réconciliation actualisé</param>
        public void UpdateServices(OfflineFirstService offlineFirstService, ReconciliationService reconciliationService)
        {
            _offlineFirstService = offlineFirstService;
            _reconciliationService = reconciliationService;
            if (_offlineFirstService != null && _reconciliationService != null)
                _kpiSnapshotService = new KpiSnapshotService(_offlineFirstService, _reconciliationService);
            DataContext = this;
            // Reload available snapshot dates when services/country change
            _ = RefreshSnapshotDatePickerAsync();
        }

        /// <summary>
        /// Initialise toutes les propriétés avec des valeurs par défaut
        /// </summary>
        private void InitializeProperties()
        {
            _availableCountries = new List<Country>();
            _missingInvoicesCount = 0;
            _paidButNotReconciledCount = 0;
            _underInvestigationCount = 0;
            _totalReceivableAmount = 0m;
            _receivableAccountsCount = 0;
            _totalPivotAmount = 0m;
            _pivotAccountsCount = 0;
            _currencyDistributionSeries = new SeriesCollection();
            _actionDistributionSeries = new SeriesCollection();
            _kpiRiskSeries = new SeriesCollection();
            _statusMessage = "Ready";
            _lastUpdateTime = DateTime.Now.ToString("HH:mm:ss", CultureInfo.InvariantCulture);
            
            // Initialiser les nouvelles propriétés de graphiques
            _receivableChartData = new ChartValues<double>();
            _pivotChartData = new ChartValues<double>();
            _receivablePivotByActionSeries = new SeriesCollection();
            _receivablePivotByActionLabels = new List<string>();
            _actionLabels = new List<string>();
            _kpiRiskLabels = new List<string>();
            _deletionDelaySeries = new SeriesCollection();
            _deletionDelayLabels = new List<string>();
            _newDeletedDailySeries = new SeriesCollection();
            _newDeletedDailyLabels = new List<string>();
        }

        public string CurrentCountryName => _offlineFirstService?.CurrentCountry?.CNT_Name;
        public string CurrentCountryId => _offlineFirstService?.CurrentCountry?.CNT_Id;

        public bool IsLoading
        {
            get => _isLoading;
            set
            {
                _isLoading = value;
                OnPropertyChanged();
                _canRefresh = !value;
                OnPropertyChanged(nameof(CanRefresh));
            }
        }

        // Propriétés pour le binding XAML
        public List<Country> AvailableCountries
        {
            get => _availableCountries;
            set
            {
                _availableCountries = value;
                OnPropertyChanged();
            }
        }

        public int MissingInvoicesCount
        {
            get => _missingInvoicesCount;
            set
            {
                _missingInvoicesCount = value;
                OnPropertyChanged();
            }
        }

        public int PaidButNotReconciledCount
        {
            get => _paidButNotReconciledCount;
            set
            {
                _paidButNotReconciledCount = value;
                OnPropertyChanged();
            }
        }

        public int UnderInvestigationCount
        {
            get => _underInvestigationCount;
            set
            {
                _underInvestigationCount = value;
                OnPropertyChanged();
            }
        }

        public decimal TotalReceivableAmount
        {
            get => _totalReceivableAmount;
            set
            {
                _totalReceivableAmount = value;
                OnPropertyChanged();
            }
        }

        public int ReceivableAccountsCount
        {
            get => _receivableAccountsCount;
            set
            {
                _receivableAccountsCount = value;
                OnPropertyChanged();
            }
        }

        public decimal TotalPivotAmount
        {
            get => _totalPivotAmount;
            set
            {
                _totalPivotAmount = value;
                OnPropertyChanged();
            }
        }

        public int PivotAccountsCount
        {
            get => _pivotAccountsCount;
            set
            {
                _pivotAccountsCount = value;
                OnPropertyChanged();
            }
        }

        public SeriesCollection CurrencyDistributionSeries
        {
            get => _currencyDistributionSeries;
            set
            {
                _currencyDistributionSeries = value;
                OnPropertyChanged();
            }
        }

        public SeriesCollection ActionDistributionSeries
        {
            get => _actionDistributionSeries;
            set
            {
                _actionDistributionSeries = value;
                OnPropertyChanged();
            }
        }

        public SeriesCollection KpiRiskSeries
        {
            get => _kpiRiskSeries;
            set
            {
                _kpiRiskSeries = value;
                OnPropertyChanged();
            }
        }

        public string StatusMessage
        {
            get => _statusMessage;
            set
            {
                _statusMessage = value;
                OnPropertyChanged();
            }
        }

        public string LastUpdateTime
        {
            get => _lastUpdateTime;
            set
            {
                _lastUpdateTime = value;
                OnPropertyChanged();
            }
        }

        public ChartValues<double> ReceivableChartData
        {
            get => _receivableChartData;
            set
            {
                _receivableChartData = value;
                OnPropertyChanged();
            }
        }

        public ChartValues<double> PivotChartData
        {
            get => _pivotChartData;
            set
            {
                _pivotChartData = value;
                OnPropertyChanged();
            }
        }

        public List<string> ActionLabels
        {
            get => _actionLabels;
            set
            {
                _actionLabels = value;
                OnPropertyChanged();
            }
        }

        public List<string> KpiRiskLabels
        {
            get => _kpiRiskLabels;
            set
            {
                _kpiRiskLabels = value;
                OnPropertyChanged();
            }
        }

        public SeriesCollection ReceivablePivotByActionSeries
        {
            get => _receivablePivotByActionSeries;
            set
            {
                _receivablePivotByActionSeries = value;
                OnPropertyChanged();
            }
        }

        public List<string> ReceivablePivotByActionLabels
        {
            get => _receivablePivotByActionLabels;
            set
            {
                _receivablePivotByActionLabels = value;
                OnPropertyChanged();
            }
        }

        public SeriesCollection DeletionDelaySeries
        {
            get => _deletionDelaySeries;
            set
            {
                _deletionDelaySeries = value;
                OnPropertyChanged();
            }
        }

        public List<string> DeletionDelayLabels
        {
            get => _deletionDelayLabels;
            set
            {
                _deletionDelayLabels = value;
                OnPropertyChanged();
            }
        }

        public SeriesCollection NewDeletedDailySeries
        {
            get => _newDeletedDailySeries;
            set
            {
                _newDeletedDailySeries = value;
                OnPropertyChanged();
            }
        }

        public List<string> NewDeletedDailyLabels
        {
            get => _newDeletedDailyLabels;
            set
            {
                _newDeletedDailyLabels = value;
                OnPropertyChanged();
            }
        }

        #endregion

        #region IRefreshable Implementation

        public bool CanRefresh => _canRefresh && !string.IsNullOrEmpty(_offlineFirstService?.CurrentCountryId);

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
                // Keep date picker in sync with DB snapshot dates
                await RefreshSnapshotDatePickerAsync();
                await LoadDashboardDataAsync();
            }
            finally
            {
                RefreshCompleted?.Invoke(this, EventArgs.Empty);
            }
        }

        #endregion

        #region Data Loading

        /// <summary>
        /// Charge les données (snapshot si sélectionné, sinon live) et met à jour les KPIs/graphes.
        /// </summary>
        private async Task LoadDashboardDataAsync(bool retryIfCountryNotReady = true)
        {
            try
            {
                IsLoading = true;
                ShowLoadingIndicator(true);

                // S'assurer qu'un pays est prêt avant de charger
                if (!await EnsureCountryReadyAsync(retryIfCountryNotReady))
                    return; // sortie silencieuse si toujours pas prêt

                if (_selectedSnapshotDate.HasValue)
                {
                    var loaded = await TryLoadSnapshotAsync(_selectedSnapshotDate.Value, _offlineFirstService.CurrentCountryId);
                    if (!loaded)
                    {
                        // Fallback aux données live
                        await LoadLiveDashboardAsync();
                    }
                    else
                    {
                        // Graphes restent live; seules tuiles KPI proviennent du snapshot
                        UpdateCountryInfo();
                    }
                }
                else
                {
                    await LoadLiveDashboardAsync();
                }
                ApplyHistoricalBackground();
            }
            catch (Exception ex)
            {
                ShowError($"Error loading data: {ex.Message}");
            }
            finally
            {
                IsLoading = false;
                ShowLoadingIndicator(false);
            }
        }

        /// <summary>
        /// Vérifie que le pays courant est prêt. Optionnellement, effectue une unique attente courte avant nouvel essai.
        /// </summary>
        private async Task<bool> EnsureCountryReadyAsync(bool allowSingleRetry)
        {
            if (_offlineFirstService != null && !string.IsNullOrEmpty(_offlineFirstService.CurrentCountryId))
                return true;

            System.Diagnostics.Debug.WriteLine("HomePage.EnsureCountryReadyAsync: country not ready; scheduling a short retry...");
            if (allowSingleRetry)
            {
                await Task.Delay(200);
                return _offlineFirstService != null && !string.IsNullOrEmpty(_offlineFirstService.CurrentCountryId);
            }
            return false;
        }

        /// <summary>
        /// Charge les données live et met à jour KPIs, graphes et infos pays.
        /// </summary>
        private async Task LoadLiveDashboardAsync()
        {
            await LoadRealDataFromDatabase();
            UpdateKPISummary();
            UpdateCharts();
            UpdateCountryInfo();
        }

        private async Task<bool> TryLoadSnapshotAsync(DateTime date, string countryId)
        {
            try
            {
                if ((_reconciliationService == null && _kpiSnapshotService == null) || string.IsNullOrWhiteSpace(countryId)) return false;
                if (_kpiSnapshotService == null && _offlineFirstService != null && _reconciliationService != null)
                    _kpiSnapshotService = new KpiSnapshotService(_offlineFirstService, _reconciliationService);
                var table = await _kpiSnapshotService.GetKpiSnapshotAsync(date.Date, countryId);

                if (table == null || table.Rows.Count == 0)
                {
                    _usingSnapshot = false;
                    StatusMessage = $"No snapshot found for {date.ToString("dd/MM/yyyy", CultureInfo.InvariantCulture)}. Showing live data.";
                    return false;
                }

                var row = table.Rows[0];
                // Mettre à jour les tuiles KPI à partir des colonnes agrégées du snapshot
                MissingInvoicesCount = SafeGetInt(row, "MissingInvoices");
                PaidButNotReconciledCount = SafeGetInt(row, "PaidNotReconciled");
                UnderInvestigationCount = SafeGetInt(row, "UnderInvestigation");
                ReceivableAccountsCount = SafeGetInt(row, "ReceivableCount");
                TotalReceivableAmount = SafeGetDecimal(row, "ReceivableAmount");
                PivotAccountsCount = SafeGetInt(row, "PivotCount");
                TotalPivotAmount = SafeGetDecimal(row, "PivotAmount");

                // Charger les séries de graphiques depuis le snapshot
                ApplySnapshotCharts(row);

                _usingSnapshot = true;
                StatusMessage = $"Snapshot loaded for {date.ToString("dd/MM/yyyy", CultureInfo.InvariantCulture)}.";
                LastUpdateTime = DateTime.Now.ToString("HH:mm:ss", CultureInfo.InvariantCulture);
                return true;
            }
            catch (Exception ex)
            {
                _usingSnapshot = false;
                StatusMessage = "Snapshot load error";
                System.Diagnostics.Debug.WriteLine($"TryLoadSnapshotAsync error: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Utilise les données sérialisées du snapshot pour remplir les graphiques sans recharger la base.
        /// </summary>
        private void ApplySnapshotCharts(DataRow row)
        {
            try
            {
                _reconciliationViewData = new List<ReconciliationViewData>();

                // KPI distribution
                var kpiChart = FindName("KPIChart") as LiveCharts.Wpf.PieChart;
                var kpiJson = row["KpiDistributionJson"]?.ToString();
                if (kpiChart != null && !string.IsNullOrWhiteSpace(kpiJson))
                {
                    using var doc = JsonDocument.Parse(kpiJson);
                    var sc = new SeriesCollection();
                    foreach (var el in doc.RootElement.EnumerateArray())
                    {
                        var name = el.GetProperty("kpi").GetString();
                        var count = el.GetProperty("count").GetInt32();
                        sc.Add(new PieSeries
                        {
                            Title = $"{name} ({count})",
                            Values = new ChartValues<int> { count },
                            LabelPoint = cp => $"{count} ({cp.Participation:P})"
                        });
                    }
                    kpiChart.Series = sc;
                }
                else if (kpiChart != null)
                {
                    kpiChart.Series = new SeriesCollection();
                }

                // Currency distribution
                var ccyJson = row["CurrencyDistributionJson"]?.ToString();
                if (!string.IsNullOrWhiteSpace(ccyJson))
                {
                    using var doc = JsonDocument.Parse(ccyJson);
                    var sc = new SeriesCollection();
                    foreach (var el in doc.RootElement.EnumerateArray())
                    {
                        var ccy = el.GetProperty("currency").GetString();
                        var amount = el.GetProperty("amount").GetDouble();
                        var count = el.GetProperty("count").GetInt32();
                        sc.Add(new PieSeries
                        {
                            Title = $"{ccy} ({count})",
                            Values = new ChartValues<double> { amount },
                            LabelPoint = cp => $"{ccy}: {amount:N2} ({cp.Participation:P})"
                        });
                    }
                    CurrencyDistributionSeries = sc;
                }
                else
                {
                    CurrencyDistributionSeries = new SeriesCollection();
                }

                // Action distribution
                var actionJson = row["ActionDistributionJson"]?.ToString();
                if (!string.IsNullOrWhiteSpace(actionJson))
                {
                    using var doc = JsonDocument.Parse(actionJson);
                    var sc = new SeriesCollection();
                    var labels = new List<string>();
                    foreach (var el in doc.RootElement.EnumerateArray())
                    {
                        var name = el.GetProperty("action").GetString();
                        var count = el.GetProperty("count").GetInt32();
                        labels.Add(name);
                        sc.Add(new ColumnSeries { Title = name, Values = new ChartValues<int> { count } });
                    }
                    ActionLabels = labels;
                    ActionDistributionSeries = sc;
                }
                else
                {
                    ActionLabels = new List<string>();
                    ActionDistributionSeries = new SeriesCollection();
                }

                // KPI × Risky
                var riskJson = row["KpiRiskMatrixJson"]?.ToString();
                if (!string.IsNullOrWhiteSpace(riskJson))
                {
                    using var doc = JsonDocument.Parse(riskJson);
                    var labels = doc.RootElement.GetProperty("kpiLabels").EnumerateArray().Select(e => e.GetString()).ToList();
                    var values = doc.RootElement.GetProperty("values").EnumerateArray().ToList();
                    var riskyVals = new ChartValues<int>(values[0].EnumerateArray().Select(e => e.GetInt32()));
                    var nonRiskyVals = new ChartValues<int>(values[1].EnumerateArray().Select(e => e.GetInt32()));
                    KpiRiskLabels = labels;
                    KpiRiskSeries = new SeriesCollection
                    {
                        new StackedColumnSeries { Title = "Risky", Values = riskyVals, DataLabels = true, LabelPoint = cp => cp.Y.ToString("N0") },
                        new StackedColumnSeries { Title = "Non-Risky", Values = nonRiskyVals, DataLabels = true, LabelPoint = cp => cp.Y.ToString("N0") }
                    };
                }
                else
                {
                    KpiRiskLabels = new List<string>();
                    KpiRiskSeries = new SeriesCollection();
                }

                // Receivable vs Pivot by Action
                var rpaJson = row["ReceivablePivotByActionJson"]?.ToString();
                if (!string.IsNullOrWhiteSpace(rpaJson))
                {
                    using var doc = JsonDocument.Parse(rpaJson);
                    var labels = doc.RootElement.GetProperty("labels").EnumerateArray().Select(e => e.GetString()).ToList();
                    var recv = new ChartValues<int>(doc.RootElement.GetProperty("receivable").EnumerateArray().Select(e => e.GetInt32()));
                    var piv = new ChartValues<int>(doc.RootElement.GetProperty("pivot").EnumerateArray().Select(e => e.GetInt32()));
                    ReceivablePivotByActionLabels = labels;
                    ReceivablePivotByActionSeries = new SeriesCollection
                    {
                        new StackedColumnSeries { Title = "Receivable", Values = recv, DataLabels = true, LabelPoint = cp => cp.Y.ToString("N0") },
                        new StackedColumnSeries { Title = "Pivot", Values = piv, DataLabels = true, LabelPoint = cp => cp.Y.ToString("N0") }
                    };
                }
                else
                {
                    ReceivablePivotByActionLabels = new List<string>();
                    ReceivablePivotByActionSeries = new SeriesCollection();
                }

                // Deletion delay
                var delayJson = row["DeletionDelayBucketsJson"]?.ToString();
                if (!string.IsNullOrWhiteSpace(delayJson))
                {
                    using var doc = JsonDocument.Parse(delayJson);
                    var labels = new List<string>();
                    var vals = new ChartValues<double>();
                    foreach (var el in doc.RootElement.EnumerateArray())
                    {
                        labels.Add(el.GetProperty("bucket").GetString());
                        vals.Add(el.GetProperty("avgDays").GetDouble());
                    }
                    DeletionDelayLabels = labels;
                    DeletionDelaySeries = new SeriesCollection
                    {
                        new ColumnSeries { Title = "Average duration (days)", Values = vals, DataLabels = true, LabelPoint = cp => (cp.Y > 0 ? cp.Y.ToString("N0") : string.Empty) }
                    };
                }
                else
                {
                    DeletionDelayLabels = new List<string>();
                    DeletionDelaySeries = new SeriesCollection();
                }

                // Charts not included in snapshot -> reset
                ReceivableChartData = new ChartValues<double>();
                PivotChartData = new ChartValues<double>();
                NewDeletedDailySeries = new SeriesCollection();
                NewDeletedDailyLabels = new List<string>();
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"ApplySnapshotCharts error: {ex.Message}");
            }
        }

        private void ApplyHistoricalBackground()
        {
            try
            {
                if (MainScrollViewer == null) return;
                bool isHistorical = _usingSnapshot && _selectedSnapshotDate.HasValue && _selectedSnapshotDate.Value.Date != DateTime.Today.Date;
                if (isHistorical)
                {
                    MainScrollViewer.Background = new SolidColorBrush(Color.FromRgb(0xF2, 0xF2, 0xF2));
                }
                else
                {
                    MainScrollViewer.Background = _defaultBackground ?? MainScrollViewer.Background;
                }
            }
            catch { }
        }

        private async void TodaySnapshotDate_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var today = DateTime.Today.Date;
                // If today is a snapshot date, select it; else revert to live view
                if (_availableSnapshotDates != null && _availableSnapshotDates.Any(d => d.Date == today))
                {
                    _selectedSnapshotDate = today;
                    if (SnapshotDatePicker != null)
                    {
                        SnapshotDatePicker.SelectedDate = today;
                        SnapshotDatePicker.DisplayDate = today;
                    }
                }
                else
                {
                    _selectedSnapshotDate = null;
                    _usingSnapshot = false;
                    if (SnapshotDatePicker != null)
                    {
                        SnapshotDatePicker.SelectedDate = null;
                        SnapshotDatePicker.DisplayDate = today;
                    }
                }
                await RefreshAsync();
            }
            catch { }
        }

        private async void ExportDailyKpi_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if (_reconciliationService == null || string.IsNullOrWhiteSpace(CurrentCountryId))
                {
                    ShowError("Export is unavailable: missing service or country.");
                    return;
                }

                // Determine range: from first to last available snapshot
                if (_kpiSnapshotService == null && _offlineFirstService != null && _reconciliationService != null)
                    _kpiSnapshotService = new KpiSnapshotService(_offlineFirstService, _reconciliationService);
                var dates = _availableSnapshotDates ?? await _kpiSnapshotService.GetKpiSnapshotDatesAsync(CurrentCountryId);
                if (dates == null || dates.Count == 0)
                {
                    ShowError("No snapshots to export.");
                    return;
                }
                var from = dates.Min().Date;
                var to = dates.Max().Date;

                var table = await _kpiSnapshotService.GetKpiSnapshotsAsync(from, to, CurrentCountryId);
                if (table == null || table.Rows.Count == 0)
                {
                    ShowError("No data returned for export.");
                    return;
                }

                // Base columns: keep non-JSON fields
                var jsonCols = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (DataColumn col in table.Columns)
                {
                    if (col.ColumnName.IndexOf("json", StringComparison.OrdinalIgnoreCase) >= 0)
                        jsonCols.Add(col.ColumnName);
                }
                var exportCols = table.Columns.Cast<DataColumn>()
                    .Where(c => !jsonCols.Contains(c.ColumnName))
                    .ToList();

                // Build flattened schema from JSON across all rows
                var flatHeaders = BuildFlattenedHeaders(table);

                var sfd = new Microsoft.Win32.SaveFileDialog
                {
                    FileName = $"KPI_{from:yyyyMMdd}_to_{to:yyyyMMdd}_{CurrentCountryId}.csv",
                    Filter = "CSV Files (*.csv)|*.csv|All Files (*.*)|*.*",
                    DefaultExt = ".csv"
                };
                var ok = sfd.ShowDialog();
                if (ok != true) return;

                WriteDataTableToCsv(table, exportCols, flatHeaders, sfd.FileName);
                StatusMessage = $"Exported {table.Rows.Count} rows to {System.IO.Path.GetFileName(sfd.FileName)}";
            }
            catch (Exception ex)
            {
                ShowError($"Export failed: {ex.Message}");
            }
        }

        private void ReportMissingInvoices_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if (_reconciliationViewData == null || _reconciliationViewData.Count == 0)
                {
                    ShowError("No reconciliation data loaded yet.");
                    return;
                }

                var rows = _reconciliationViewData
                    .Where(r => r.KPI == (int)KPIType.NotClaimed)
                    .Select(r => new
                    {
                        Ref = !string.IsNullOrWhiteSpace(r.DWINGS_GuaranteeID) ? r.DWINGS_GuaranteeID : r.GUARANTEE_ID,
                        Inv = !string.IsNullOrWhiteSpace(r.DWINGS_InvoiceID) ? r.DWINGS_InvoiceID : r.INVOICE_ID
                    })
                    .Where(x => !string.IsNullOrWhiteSpace(x.Ref) || !string.IsNullOrWhiteSpace(x.Inv))
                    .ToList();

                if (rows.Count == 0)
                {
                    ShowError("There are no missing invoices to report.");
                    return;
                }

                string subject = $"Missing invoices report - {CurrentCountryName ?? "Unknown Country"} - {DateTime.Today.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)}";

                var sb = new StringBuilder();
                sb.Append("<html><body>");
                sb.Append("<p>Dear Correspondent,<br/><br/>");
                sb.Append("Please find below the list of invoices that remain missing in our records. Could you kindly review and provide the missing items or advise on their status?<br/><br/>");
                sb.Append("Thank you in advance.<br/><br/></p>");
                sb.Append("<table style='border-collapse:collapse;font-family:Segoe UI, Arial, sans-serif;font-size:12px'>");
                sb.Append("<thead><tr>");
                sb.Append("<th style='border:1px solid #ccc;padding:6px 8px;background:#f5f5f5;text-align:left'>DWINGS REFERENCE</th>");
                sb.Append("<th style='border:1px solid #ccc;padding:6px 8px;background:#f5f5f5;text-align:left'>INVOICE_ID</th>");
                sb.Append("</tr></thead><tbody>");
                foreach (var x in rows)
                {
                    sb.Append("<tr>");
                    sb.AppendFormat("<td style='border:1px solid #ccc;padding:6px 8px'>{0}</td>", HtmlEncode(x.Ref));
                    sb.AppendFormat("<td style='border:1px solid #ccc;padding:6px 8px'>{0}</td>", HtmlEncode(x.Inv));
                    sb.Append("</tr>");
                }
                sb.Append("</tbody></table>");
                sb.Append("<p><br/>Best regards,</p>");
                sb.Append("</body></html>");
                string bodyHtml = sb.ToString();

                // Create Outlook email via late binding to avoid COM reference requirement
                var outlookType = Type.GetTypeFromProgID("Outlook.Application");
                if (outlookType == null)
                {
                    ShowError("Microsoft Outlook is not installed or not available on this machine.");
                    return;
                }

                dynamic outlookApp = Activator.CreateInstance(outlookType);
                if (outlookApp == null)
                {
                    ShowError("Failed to start Microsoft Outlook.");
                    return;
                }

                dynamic mail = outlookApp.CreateItem(0); // 0 = olMailItem
                mail.Subject = subject;

                // Display first to allow default signature to load, then prepend our content
                mail.Display(false);
                string signature = string.Empty;
                try { signature = mail.HTMLBody as string ?? string.Empty; } catch { signature = string.Empty; }
                mail.HTMLBody = bodyHtml + signature;
            }
            catch (Exception ex)
            {
                ShowError($"Unable to compose Outlook email: {ex.Message}");
            }
        }

        private static string HtmlEncode(string s)
        {
            if (string.IsNullOrEmpty(s)) return string.Empty;
            return s
                .Replace("&", "&amp;")
                .Replace("<", "&lt;")
                .Replace(">", "&gt;")
                .Replace("\"", "&quot;")
                .Replace("'", "&#39;");
        }

        private List<string> BuildFlattenedHeaders(DataTable table)
        {
            var headers = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (DataRow row in table.Rows)
            {
                TryAddDeletionDelayHeaders(row, headers);
                TryAddReceivablePivotByActionHeaders(row, headers);
                TryAddKpiDistributionHeaders(row, headers);
                TryAddKpiRiskHeaders(row, headers);
                TryAddCurrencyDistributionHeaders(row, headers);
                TryAddActionDistributionHeaders(row, headers);
            }
            // Return ordered headers for stable CSV
            return headers.OrderBy(h => h, StringComparer.OrdinalIgnoreCase).ToList();
        }

        private void WriteDataTableToCsv(DataTable table, List<DataColumn> cols, List<string> flatHeaders, string path)
        {
            using (var fs = new System.IO.FileStream(path, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.None))
            using (var writer = new System.IO.StreamWriter(fs, new System.Text.UTF8Encoding(true)))
            {
                // header: base columns + flattened
                var header = cols.Select(c => c.ColumnName).Concat(flatHeaders).Select(CsvEscape);
                writer.WriteLine(string.Join(",", header));
                foreach (DataRow row in table.Rows)
                {
                    var values = cols.Select(c => CsvEscape(FormatCsvValue(row[c]))).ToList();
                    var flatMap = ExtractFlattenedValues(row);
                    foreach (var h in flatHeaders)
                    {
                        flatMap.TryGetValue(h, out var v);
                        values.Add(CsvEscape(v ?? string.Empty));
                    }
                    writer.WriteLine(string.Join(",", values));
                }
            }
        }

        private static string FormatCsvValue(object v)
        {
            if (v == null || v == DBNull.Value) return string.Empty;
            if (v is DateTime dt) return dt.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
            return v.ToString();
        }

        private static string CsvEscape(string s)
        {
            if (s == null) return string.Empty;
            bool needQuotes = s.Contains(',') || s.Contains('"') || s.Contains('\n') || s.Contains('\r');
            if (s.Contains('"')) s = s.Replace("\"", "\"\"");
            return needQuotes ? "\"" + s + "\"" : s;
        }

        private Dictionary<string, string> ExtractFlattenedValues(DataRow row)
        {
            var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            TryFillDeletionDelay(row, map);
            TryFillReceivablePivotByAction(row, map);
            TryFillKpiDistribution(row, map);
            TryFillKpiRisk(row, map);
            TryFillCurrencyDistribution(row, map);
            TryFillActionDistribution(row, map);
            return map;
        }

        private static string SanitizeHeader(string s)
        {
            if (string.IsNullOrWhiteSpace(s)) return "_";
            var bad = new char[] { ' ', '\\', '/', '\t', '\n', '\r', ',', ';', ':', '"', '\'', '(', ')', '[', ']', '{', '}', '<', '>', '|', '#', '%', '+', '=' };
            foreach (var ch in bad) s = s.Replace(ch, '_');
            return s;
        }

        private void TryAddDeletionDelayHeaders(DataRow row, HashSet<string> headers)
        {
            if (!row.Table.Columns.Contains("DeletionDelayBucketsJson")) return;
            var json = row["DeletionDelayBucketsJson"]?.ToString();
            if (string.IsNullOrWhiteSpace(json)) return;
            try
            {
                using var doc = JsonDocument.Parse(json);
                if (doc.RootElement.ValueKind != JsonValueKind.Array) return;
                foreach (var el in doc.RootElement.EnumerateArray())
                {
                    var bucket = el.GetProperty("bucket").GetString() ?? "Bucket";
                    var key1 = $"DelDelay_avgDays_{SanitizeHeader(bucket)}";
                    var key2 = $"DelDelay_count_{SanitizeHeader(bucket)}";
                    headers.Add(key1);
                    headers.Add(key2);
                }
            }
            catch { }
        }

        private void TryFillDeletionDelay(DataRow row, Dictionary<string, string> map)
        {
            if (!row.Table.Columns.Contains("DeletionDelayBucketsJson")) return;
            var json = row["DeletionDelayBucketsJson"]?.ToString();
            if (string.IsNullOrWhiteSpace(json)) return;
            try
            {
                using var doc = JsonDocument.Parse(json);
                if (doc.RootElement.ValueKind != JsonValueKind.Array) return;
                foreach (var el in doc.RootElement.EnumerateArray())
                {
                    var bucket = el.TryGetProperty("bucket", out var p) ? p.GetString() : null;
                    var avg = el.TryGetProperty("avgDays", out var a) ? a.ToString() : null;
                    var cnt = el.TryGetProperty("count", out var c) ? c.ToString() : null;
                    if (!string.IsNullOrEmpty(bucket))
                    {
                        map[$"DelDelay_avgDays_{SanitizeHeader(bucket)}"] = avg ?? string.Empty;
                        map[$"DelDelay_count_{SanitizeHeader(bucket)}"] = cnt ?? string.Empty;
                    }
                }
            }
            catch { }
        }

        private void TryAddReceivablePivotByActionHeaders(DataRow row, HashSet<string> headers)
        {
            if (!row.Table.Columns.Contains("ReceivablePivotByActionJson")) return;
            var json = row["ReceivablePivotByActionJson"]?.ToString();
            if (string.IsNullOrWhiteSpace(json)) return;
            try
            {
                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;
                if (!root.TryGetProperty("labels", out var labels) || labels.ValueKind != JsonValueKind.Array) return;
                foreach (var l in labels.EnumerateArray())
                {
                    var label = l.GetString() ?? "Action";
                    headers.Add($"ActPivot_Receivable_{SanitizeHeader(label)}");
                    headers.Add($"ActPivot_Pivot_{SanitizeHeader(label)}");
                }
            }
            catch { }
        }

        private void TryFillReceivablePivotByAction(DataRow row, Dictionary<string, string> map)
        {
            if (!row.Table.Columns.Contains("ReceivablePivotByActionJson")) return;
            var json = row["ReceivablePivotByActionJson"]?.ToString();
            if (string.IsNullOrWhiteSpace(json)) return;
            try
            {
                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;
                var labels = root.GetProperty("labels").EnumerateArray().Select(x => x.GetString()).ToList();
                var recv = root.GetProperty("receivable").EnumerateArray().Select(x => x.ToString()).ToList();
                var piv = root.GetProperty("pivot").EnumerateArray().Select(x => x.ToString()).ToList();
                for (int i = 0; i < labels.Count; i++)
                {
                    var label = labels[i] ?? "Action";
                    var r = i < recv.Count ? recv[i] : string.Empty;
                    var p = i < piv.Count ? piv[i] : string.Empty;
                    map[$"ActPivot_Receivable_{SanitizeHeader(label)}"] = r;
                    map[$"ActPivot_Pivot_{SanitizeHeader(label)}"] = p;
                }
            }
            catch { }
        }

        private void TryAddKpiDistributionHeaders(DataRow row, HashSet<string> headers)
        {
            if (!row.Table.Columns.Contains("KpiDistributionJson")) return;
            var json = row["KpiDistributionJson"]?.ToString();
            if (string.IsNullOrWhiteSpace(json)) return;
            try
            {
                using var doc = JsonDocument.Parse(json);
                if (doc.RootElement.ValueKind != JsonValueKind.Array) return;
                foreach (var el in doc.RootElement.EnumerateArray())
                {
                    var kpi = el.TryGetProperty("kpi", out var p) ? (p.GetString() ?? "KPI") : "KPI";
                    headers.Add($"KpiDist_Count_{SanitizeHeader(kpi)}");
                }
            }
            catch { }
        }

        private void TryFillKpiDistribution(DataRow row, Dictionary<string, string> map)
        {
            if (!row.Table.Columns.Contains("KpiDistributionJson")) return;
            var json = row["KpiDistributionJson"]?.ToString();
            if (string.IsNullOrWhiteSpace(json)) return;
            try
            {
                using var doc = JsonDocument.Parse(json);
                if (doc.RootElement.ValueKind != JsonValueKind.Array) return;
                foreach (var el in doc.RootElement.EnumerateArray())
                {
                    var kpi = el.TryGetProperty("kpi", out var p) ? p.GetString() : null;
                    var cnt = el.TryGetProperty("count", out var c) ? c.ToString() : null;
                    if (!string.IsNullOrEmpty(kpi))
                        map[$"KpiDist_Count_{SanitizeHeader(kpi)}"] = cnt ?? string.Empty;
                }
            }
            catch { }
        }

        private void TryAddKpiRiskHeaders(DataRow row, HashSet<string> headers)
        {
            if (!row.Table.Columns.Contains("KpiRiskMatrixJson")) return;
            var json = row["KpiRiskMatrixJson"]?.ToString();
            if (string.IsNullOrWhiteSpace(json)) return;
            try
            {
                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;
                if (!root.TryGetProperty("kpiLabels", out var labels) || labels.ValueKind != JsonValueKind.Array) return;
                foreach (var l in labels.EnumerateArray())
                {
                    var label = l.GetString() ?? "KPI";
                    headers.Add($"KpiRisk_Risky_{SanitizeHeader(label)}");
                    headers.Add($"KpiRisk_NonRisky_{SanitizeHeader(label)}");
                }
            }
            catch { }
        }

        private void TryFillKpiRisk(DataRow row, Dictionary<string, string> map)
        {
            if (!row.Table.Columns.Contains("KpiRiskMatrixJson")) return;
            var json = row["KpiRiskMatrixJson"]?.ToString();
            if (string.IsNullOrWhiteSpace(json)) return;
            try
            {
                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;
                var labels = root.GetProperty("kpiLabels").EnumerateArray().Select(x => x.GetString()).ToList();
                var values = root.GetProperty("values").EnumerateArray().Select(arr => arr.EnumerateArray().Select(v => v.ToString()).ToList()).ToList();
                // Expect values[0]=risky, values[1]=nonRisky
                var risky = values.Count > 0 ? values[0] : new List<string>();
                var nonRisky = values.Count > 1 ? values[1] : new List<string>();
                for (int i = 0; i < labels.Count; i++)
                {
                    var label = labels[i] ?? "KPI";
                    var r = i < risky.Count ? risky[i] : string.Empty;
                    var n = i < nonRisky.Count ? nonRisky[i] : string.Empty;
                    map[$"KpiRisk_Risky_{SanitizeHeader(label)}"] = r;
                    map[$"KpiRisk_NonRisky_{SanitizeHeader(label)}"] = n;
                }
            }
            catch { }
        }

        private void TryAddCurrencyDistributionHeaders(DataRow row, HashSet<string> headers)
        {
            if (!row.Table.Columns.Contains("CurrencyDistributionJson")) return;
            var json = row["CurrencyDistributionJson"]?.ToString();
            if (string.IsNullOrWhiteSpace(json)) return;
            try
            {
                using var doc = JsonDocument.Parse(json);
                if (doc.RootElement.ValueKind != JsonValueKind.Array) return;
                foreach (var el in doc.RootElement.EnumerateArray())
                {
                    var ccy = el.TryGetProperty("ccy", out var p) ? (p.GetString() ?? "CCY") : "CCY";
                    headers.Add($"Ccy_Amount_{SanitizeHeader(ccy)}");
                }
            }
            catch { }
        }

        private void TryFillCurrencyDistribution(DataRow row, Dictionary<string, string> map)
        {
            if (!row.Table.Columns.Contains("CurrencyDistributionJson")) return;
            var json = row["CurrencyDistributionJson"]?.ToString();
            if (string.IsNullOrWhiteSpace(json)) return;
            try
            {
                using var doc = JsonDocument.Parse(json);
                if (doc.RootElement.ValueKind != JsonValueKind.Array) return;
                foreach (var el in doc.RootElement.EnumerateArray())
                {
                    var ccy = el.TryGetProperty("ccy", out var p) ? p.GetString() : null;
                    var amt = el.TryGetProperty("amount", out var a) ? a.ToString() : null;
                    if (!string.IsNullOrEmpty(ccy))
                        map[$"Ccy_Amount_{SanitizeHeader(ccy)}"] = amt ?? string.Empty;
                }
            }
            catch { }
        }

        private void TryAddActionDistributionHeaders(DataRow row, HashSet<string> headers)
        {
            if (!row.Table.Columns.Contains("ActionDistributionJson")) return;
            var json = row["ActionDistributionJson"]?.ToString();
            if (string.IsNullOrWhiteSpace(json)) return;
            try
            {
                using var doc = JsonDocument.Parse(json);
                if (doc.RootElement.ValueKind != JsonValueKind.Array) return;
                foreach (var el in doc.RootElement.EnumerateArray())
                {
                    var action = el.TryGetProperty("action", out var p) ? (p.GetString() ?? "Action") : "Action";
                    headers.Add($"ActionDist_Count_{SanitizeHeader(action)}");
                }
            }
            catch { }
        }

        private void TryFillActionDistribution(DataRow row, Dictionary<string, string> map)
        {
            if (!row.Table.Columns.Contains("ActionDistributionJson")) return;
            var json = row["ActionDistributionJson"]?.ToString();
            if (string.IsNullOrWhiteSpace(json)) return;
            try
            {
                using var doc = JsonDocument.Parse(json);
                if (doc.RootElement.ValueKind != JsonValueKind.Array) return;
                foreach (var el in doc.RootElement.EnumerateArray())
                {
                    var action = el.TryGetProperty("action", out var p) ? p.GetString() : null;
                    var cnt = el.TryGetProperty("count", out var c) ? c.ToString() : null;
                    if (!string.IsNullOrEmpty(action))
                        map[$"ActionDist_Count_{SanitizeHeader(action)}"] = cnt ?? string.Empty;
                }
            }
            catch { }
        }

        private int SafeGetInt(DataRow row, string column)
        {
            try
            {
                if (!row.Table.Columns.Contains(column)) return 0;
                var obj = row[column];
                if (obj == null || obj == DBNull.Value) return 0;
                if (int.TryParse(obj.ToString(), out var i)) return i;
                if (double.TryParse(obj.ToString(), out var d)) return (int)Math.Round(d);
                return 0;
            }
            catch { return 0; }
        }

        private decimal SafeGetDecimal(DataRow row, string column)
        {
            try
            {
                if (!row.Table.Columns.Contains(column)) return 0m;
                var obj = row[column];
                if (obj == null || obj == DBNull.Value) return 0m;
                if (decimal.TryParse(obj.ToString(), out var m)) return m;
                if (double.TryParse(obj.ToString(), out var d)) return (decimal)d;
                return 0m;
            }
            catch { return 0m; }
        }

        /// <summary>
        /// Charge les données réelles depuis la base de données via ReconciliationService
        /// </summary>
        private async Task LoadRealDataFromDatabase()
        {
            try
            {
                if (_reconciliationService == null)
                {
                    System.Diagnostics.Debug.WriteLine("ReconciliationService non disponible");
                    _reconciliationViewData = new List<ReconciliationViewData>();
                    return;
                }

                // Récupérer uniquement les colonnes nécessaires pour le dashboard
                var reconciliationViewData = await _reconciliationService.GetReconciliationViewAsync(_offlineFirstService.CurrentCountryId, null, true);
                _reconciliationViewData = reconciliationViewData ?? new List<ReconciliationViewData>();

                // Analyser la répartition des comptes pour diagnostic
                AnalyzeAccountDistribution();

                StatusMessage = $"Data loaded: {_reconciliationViewData.Count} rows";
                LastUpdateTime = DateTime.Now.ToString("HH:mm:ss", CultureInfo.InvariantCulture);

                System.Diagnostics.Debug.WriteLine($"Data loaded via ReconciliationService: {_reconciliationViewData.Count} rows for {_offlineFirstService.CurrentCountryId}");
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Error loading data: {ex.Message}");
                StatusMessage = "Data load error";
                _reconciliationViewData = new List<ReconciliationViewData>();
                throw new Exception($"Error loading database data: {ex.Message}", ex);
            }
        }

        private decimal ConvertToDecimal(object value)
        {
            if (value == null || value == DBNull.Value) return 0m;
            if (decimal.TryParse(value.ToString(), out decimal result)) return result;
            return 0m;
        }

        private DateTime? ConvertToDateTime(object value)
        {
            if (value == null || value == DBNull.Value) return null;
            if (DateTime.TryParse(value.ToString(), out DateTime result)) return result;
            return null;
        }

        private int? ConvertToNullableInt(object value)
        {
            if (value == null || value == DBNull.Value) return null;
            if (int.TryParse(value.ToString(), out int result)) return result;
            return null;
        }

        private bool ConvertToBool(object value)
        {
            if (value == null || value == DBNull.Value) return false;
            if (bool.TryParse(value.ToString(), out bool result)) return result;
            return value.ToString() == "1" || value.ToString().ToLower() == "true";
        }

        private bool? ConvertToNullableBool(object value)
        {
            if (value == null || value == DBNull.Value) return null;
            if (value is bool b) return b;
            var s = value.ToString();
            if (bool.TryParse(s, out var parsed)) return parsed;
            if (int.TryParse(s, out var i)) return i != 0;
            return null;
        }

        #endregion

        #region KPI Updates

        /// <summary>
        /// Met à jour le résumé des KPI avec les données réelles
        /// </summary>
        private void UpdateKPISummary()
        {
            if (_reconciliationViewData == null) return;

            var currentCountry = _offlineFirstService.CurrentCountry;
            if (currentCountry == null) return;

            var totalLines = _reconciliationViewData.Count;
            
            // Séparer les comptes Receivable et Pivot selon la configuration de la country
            var receivableData = _reconciliationViewData.Where(r => r.Account_ID == currentCountry.CNT_AmbreReceivable).ToList();
            var pivotData = _reconciliationViewData.Where(r => r.Account_ID == currentCountry.CNT_AmbrePivot).ToList();

            // Calcul des KPI réels
            var paidButNotReconciled = _reconciliationViewData.Count(r => r.KPI == (int)KPIType.PaidButNotReconciled);
            var underInvestigation = _reconciliationViewData.Count(r => r.KPI == (int)KPIType.UnderInvestigation);
            var itIssues = _reconciliationViewData.Count(r => r.KPI == (int)KPIType.ITIssues);
            var notClaimed = _reconciliationViewData.Count(r => r.KPI == (int)KPIType.NotClaimed);

            // Mise à jour des propriétés de binding
            MissingInvoicesCount = notClaimed;
            PaidButNotReconciledCount = paidButNotReconciled;
            UnderInvestigationCount = underInvestigation;

            // Calcul des montants réels
            TotalReceivableAmount = receivableData.Sum(r => r.SignedAmount);
            ReceivableAccountsCount = receivableData.Count;
            TotalPivotAmount = pivotData.Sum(r => r.SignedAmount);
            PivotAccountsCount = pivotData.Count;

            // Mise à jour des TextBlocks pour compatibilité avec l'ancien XAML
            UpdateTextBlock("TotalLinesText", totalLines.ToString("N0"));
            UpdateTextBlock("ReconciledText", paidButNotReconciled.ToString("N0"));
            UpdateTextBlock("PendingText", underInvestigation.ToString("N0"));
            UpdateTextBlock("IssuesText", itIssues.ToString("N0"));
            UpdateTextBlock("TotalAmountText", $"{(TotalReceivableAmount + TotalPivotAmount):N2}");
            UpdateTextBlock("ReconciledAmountText", $"{receivableData.Where(r => r.KPI == (int)KPIType.PaidButNotReconciled).Sum(r => r.SignedAmount):N2}");
        }

        #endregion

        #region Charts Updates

        /// <summary>
        /// Met à jour les graphiques avec les données réelles
        /// </summary>
        private void UpdateCharts()
        {
            if (_reconciliationViewData == null) return;

            UpdateKPIChart();
            UpdateCurrencyChart();
            UpdateActionChart();
            UpdateKpiRiskChart();
            UpdateReceivablePivotMiniCharts();
            UpdateReceivablePivotByActionChart();
            UpdateDeletionDelayChart();
            UpdateNewDeletedDailyChart();
        }

        /// <summary>
        /// Met à jour le graphique des KPI
        /// </summary>
        private void UpdateKPIChart()
        {
            try
            {
                if (_reconciliationViewData == null || !_reconciliationViewData.Any())
                {
                    return;
                }

                var kpiData = _reconciliationViewData
                    .Where(r => r.KPI.HasValue)
                    .GroupBy(r => r.KPI.Value)
                    .Select(g => new { 
                        KPI = g.Key, 
                        Count = g.Count(),
                        TotalAmount = g.Sum(x => Math.Abs(x.SignedAmount))
                    })
                    .OrderByDescending(x => x.Count)
                    .ToList();

                if (!kpiData.Any())
                {
                    return;
                }

                var seriesCollection = new SeriesCollection();

                foreach (var item in kpiData)
                {
                    var kpiName = EnumHelper.GetKPIName(item.KPI, _offlineFirstService?.UserFields);
                    seriesCollection.Add(new PieSeries
                    {
                        Title = $"{kpiName} ({item.Count})",
                        Values = new ChartValues<int> { item.Count },
                        LabelPoint = chartPoint => $"{item.Count} ({chartPoint.Participation:P})"
                    });
                }

                // Mise à jour du graphique KPI
                var kpiChart = FindName("KPIChart") as LiveCharts.Wpf.PieChart;
                if (kpiChart != null)
                {
                    kpiChart.Series = seriesCollection;
                }
            }
            catch (Exception ex)
            {
                ShowError($"Error updating KPI chart: {ex.Message}");
            }
        }

        /// <summary>
        /// Met à jour le graphique des devises
        /// </summary>
        private void UpdateCurrencyChart()
        {
            try
            {
                if (_reconciliationViewData == null || !_reconciliationViewData.Any())
                {
                    return;
                }

                // Calculer la répartition réelle par devise depuis T_Data_Ambre.CCY
                var currencyData = _reconciliationViewData
                    .Where(r => !string.IsNullOrEmpty(r.CCY) && r.SignedAmount != 0)
                    .GroupBy(r => r.CCY)
                    .Select(g => new { 
                        Currency = g.Key, 
                        Amount = Math.Abs(g.Sum(x => x.SignedAmount)),
                        Count = g.Count()
                    })
                    .OrderByDescending(c => c.Amount)
                    .Take(10)
                    .ToList();

                if (!currencyData.Any())
                {
                    return;
                }

                var seriesCollection = new SeriesCollection();

                // Remplir PieSeries (un secteur par devise)
                foreach (var item in currencyData)
                {
                    var title = $"{item.Currency} ({item.Count})";
                    seriesCollection.Add(new PieSeries
                    {
                        Title = title,
                        Values = new ChartValues<double> { Convert.ToDouble(item.Amount) },
                        LabelPoint = chartPoint => $"{item.Currency}: {item.Amount:N2} ({chartPoint.Participation:P})"
                    });
                }

                // Mise à jour de la propriété liée au PieChart
                CurrencyDistributionSeries = seriesCollection;
            }
            catch (Exception ex)
            {
                ShowError($"Error updating currency chart: {ex.Message}");
            }
        }

        /// <summary>
        /// Met à jour le graphique des actions
        /// </summary>
        private void UpdateActionChart()
        {
            try
            {
                if (_reconciliationViewData == null || !_reconciliationViewData.Any())
                {
                    return;
                }

                var actionData = _reconciliationViewData
                    .Where(r => r.Action.HasValue)
                    .GroupBy(r => r.Action.Value)
                    .Select(g => new { 
                        Action = g.Key, 
                        Count = g.Count(),
                        TotalAmount = g.Sum(x => Math.Abs(x.SignedAmount))
                    })
                    .OrderByDescending(x => x.Count)
                    .ToList();

                if (!actionData.Any())
                {
                    return;
                }

                // Build a lookup for user fields in the "Action" category to resolve labels/colors
                var userFields = _offlineFirstService?.UserFields ?? new List<UserField>();
                var actionFields = userFields
                    .Where(u => string.Equals(u.USR_Category, "Action", StringComparison.OrdinalIgnoreCase))
                    .ToDictionary(u => u.USR_ID, u => u);

                var seriesCollection = new SeriesCollection();
                var labels = new List<string>();

                // Build labels first to determine each action's index
                foreach (var item in actionData)
                {
                    // Prefer USR_FieldName from referential; fallback to description, then enum description
                    string actionName;
                    if (actionFields.TryGetValue(item.Action, out var uf) && uf != null)
                    {
                        actionName = !string.IsNullOrWhiteSpace(uf.USR_FieldName)
                            ? uf.USR_FieldName
                            : (!string.IsNullOrWhiteSpace(uf.USR_FieldDescription) ? uf.USR_FieldDescription : EnumHelper.GetActionName(item.Action, _offlineFirstService?.UserFields));
                    }
                    else
                    {
                        actionName = EnumHelper.GetActionName(item.Action, _offlineFirstService?.UserFields);
                    }
                    labels.Add(actionName);
                }

                // Create one colored series per action; each series has a single non-zero point at its index
                for (int i = 0; i < actionData.Count; i++)
                {
                    var item = actionData[i];
                    var values = new ChartValues<int>();
                    for (int j = 0; j < actionData.Count; j++)
                        values.Add(j == i ? item.Count : 0);

                    Brush fillBrush = null;
                    if (actionFields.TryGetValue(item.Action, out var uf))
                    {
                        fillBrush = MapActionColor(uf?.USR_Color);
                    }

                    var series = new ColumnSeries
                    {
                        Title = labels[i],
                        Values = values,
                        DataLabels = true,
                        LabelPoint = cp => cp.Y > 0 ? cp.Y.ToString("N0") : string.Empty
                    };
                    if (fillBrush != null)
                    {
                        series.Fill = fillBrush;
                        series.Stroke = fillBrush;
                    }
                    seriesCollection.Add(series);
                }

                // Mettre à jour les propriétés liées au CartesianChart
                ActionLabels = labels;
                ActionDistributionSeries = seriesCollection;
            }
            catch (Exception ex)
            {
                ShowError($"Error updating action chart: {ex.Message}");
            }
        }

        // Map referential color names (e.g., "RED", "GREEN") and common strings to a Brush.
        // Reuses the same semantics as the existing mapping used in ReconciliationView (ActionColorConverter).
        private static Brush MapActionColor(string colorRaw)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(colorRaw)) return null;
                var color = colorRaw.Trim();
                switch (color.ToUpperInvariant())
                {
                    case "RED": return Brushes.Red;
                    case "GREEN": return Brushes.Green;
                    case "YELLOW": return Brushes.Yellow;
                    case "BLUE": return Brushes.Blue;
                    case "AMBER": return Brushes.Orange;
                }

                // Try parse via BrushConverter with raw string first
                try
                {
                    var conv = new BrushConverter();
                    if (conv.ConvertFromString(color) is Brush b1) return b1;
                }
                catch { }

                // If it's a hex code without '#', try prefixing '#'
                if (!color.StartsWith("#"))
                {
                    try
                    {
                        var conv = new BrushConverter();
                        if (conv.ConvertFromString("#" + color) is Brush b2) return b2;
                    }
                    catch { }
                }

                return null;
            }
            catch { return null; }
        }

        /// <summary>
        /// Met à jour les mini graphiques (sparklines) Receivable/Pivot (12 derniers mois)
        /// </summary>
        private void UpdateReceivablePivotMiniCharts()
        {
            try
            {
                if (_reconciliationViewData == null || !_reconciliationViewData.Any())
                {
                    ReceivableChartData = new ChartValues<double>();
                    PivotChartData = new ChartValues<double>();
                    return;
                }

                var currentCountry = _offlineFirstService?.CurrentCountry;
                if (currentCountry == null)
                {
                    ReceivableChartData = new ChartValues<double>();
                    PivotChartData = new ChartValues<double>();
                    return;
                }

                // Période: 12 derniers mois (inclus le mois courant)
                var endMonth = new DateTime(DateTime.Today.Year, DateTime.Today.Month, 1);
                var months = Enumerable.Range(0, 12)
                    .Select(offset => endMonth.AddMonths(-11 + offset))
                    .ToList();

                // Sélection date préférée: Value_Date sinon Operation_Date si dispo dans le type
                DateTime? GetDate(dynamic r)
                {
                    try
                    {
                        DateTime? vd = r.Value_Date;
                        if (vd.HasValue) return new DateTime(vd.Value.Year, vd.Value.Month, 1);
                    }
                    catch { /* ignore */ }
                    try
                    {
                        DateTime? od = r.Operation_Date;
                        if (od.HasValue) return new DateTime(od.Value.Year, od.Value.Month, 1);
                    }
                    catch { /* ignore */ }
                    return null;
                }

                var receivableId = currentCountry.CNT_AmbreReceivable;
                var pivotId = currentCountry.CNT_AmbrePivot;

                var receivableByMonth = _reconciliationViewData
                    .Where(r => r.Account_ID == receivableId)
                    .Select(r => new { Date = GetDate(r), Amount = Math.Abs(Convert.ToDouble(r.SignedAmount)) })
                    .Where(x => x.Date.HasValue)
                    .GroupBy(x => x.Date.Value)
                    .ToDictionary(g => g.Key, g => g.Sum(x => x.Amount));

                var pivotByMonth = _reconciliationViewData
                    .Where(r => r.Account_ID == pivotId)
                    .Select(r => new { Date = GetDate(r), Amount = Math.Abs(Convert.ToDouble(r.SignedAmount)) })
                    .Where(x => x.Date.HasValue)
                    .GroupBy(x => x.Date.Value)
                    .ToDictionary(g => g.Key, g => g.Sum(x => x.Amount));

                var receivableSeries = new ChartValues<double>();
                var pivotSeries = new ChartValues<double>();

                foreach (var m in months)
                {
                    receivableSeries.Add(receivableByMonth.TryGetValue(m, out var rv) ? rv : 0d);
                    pivotSeries.Add(pivotByMonth.TryGetValue(m, out var pv) ? pv : 0d);
                }

                ReceivableChartData = receivableSeries;
                PivotChartData = pivotSeries;
            }
            catch (Exception ex)
            {
                // Ne pas bloquer le dashboard si la génération des séries échoue
                System.Diagnostics.Debug.WriteLine($"Mini charts error: {ex.Message}");
                ReceivableChartData = new ChartValues<double>();
                PivotChartData = new ChartValues<double>();
            }
        }

        /// <summary>
        /// Analyse la répartition entre comptes Receivable et Pivot
        /// </summary>
        private void AnalyzeAccountDistribution()
        {
            try
            {
                if (_reconciliationViewData == null || !_reconciliationViewData.Any())
                {
                    return;
                }

                var currentCountry = _offlineFirstService.CurrentCountry;
                if (currentCountry == null) return;

                var receivableData = _reconciliationViewData.Where(r => r.Account_ID == currentCountry.CNT_AmbreReceivable).ToList();
                var pivotData = _reconciliationViewData.Where(r => r.Account_ID == currentCountry.CNT_AmbrePivot).ToList();
                var otherData = _reconciliationViewData.Where(r => r.Account_ID != currentCountry.CNT_AmbreReceivable && 
                                                              r.Account_ID != currentCountry.CNT_AmbrePivot).ToList();

                // Log pour diagnostic
                System.Diagnostics.Debug.WriteLine($"Account distribution for {currentCountry.CNT_Id}:");
                System.Diagnostics.Debug.WriteLine($"- Receivable ({currentCountry.CNT_AmbreReceivable}): {receivableData.Count} lignes, {receivableData.Sum(r => r.SignedAmount):N2}");
                System.Diagnostics.Debug.WriteLine($"- Pivot ({currentCountry.CNT_AmbrePivot}): {pivotData.Count} lignes, {pivotData.Sum(r => r.SignedAmount):N2}");
                if (otherData.Any())
                {
                    var distinctAccounts = otherData.Select(r => r.Account_ID).Distinct().ToList();
                    System.Diagnostics.Debug.WriteLine($"- Autres comptes: {otherData.Count} lignes sur {distinctAccounts.Count} comptes distincts: {string.Join(", ", distinctAccounts)}");
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Error analyzing distribution: {ex.Message}");
            }
        }

        #endregion

        #region Event Handlers

        /// <summary>
        /// Gestion du clic sur le graphique en secteurs
        /// </summary>
        private void PieChart_DataClick(object sender, ChartPoint chartPoint)
        {
            try
            {
                var series = chartPoint.SeriesView as PieSeries;
                if (series != null)
                {
                    MessageBox.Show($"Category: {series.Title}\nValue: {chartPoint.Y}",
                                   "KPI Detail", MessageBoxButton.OK, MessageBoxImage.Information);
                }
            }
            catch (Exception ex)
            {
                ShowError($"Error displaying detail: {ex.Message}");
            }
        }

        #endregion

        #region Helper Methods

        /// <summary>
        /// Met à jour un TextBlock par son nom
        /// </summary>
        private void UpdateTextBlock(string name, string value)
        {
            try
            {
                var textBlock = FindName(name) as TextBlock;
                if (textBlock != null)
                {
                    textBlock.Text = value;
                }
            }
            catch (Exception ex)
            {
                // Log silencieux - ne pas interrompre pour un TextBlock manquant
                System.Diagnostics.Debug.WriteLine($"TextBlock '{name}' not found: {ex.Message}");
            }
        }

        /// <summary>
        /// Met à jour les informations du pays
        /// </summary>
        private void UpdateCountryInfo()
        {
            try
            {
                UpdateTextBlock("CountryNameText", _offlineFirstService.CurrentCountryId ?? "N/A");
                UpdateTextBlock("LastUpdateText", DateTime.Now.ToString("dd/MM/yyyy HH:mm", CultureInfo.InvariantCulture));
            }
            catch (Exception ex)
            {
                ShowError($"Error updating country info: {ex.Message}");
            }
        }

        /// <summary>
        /// Affiche/masque l'indicateur de chargement
        /// </summary>
        private void ShowLoadingIndicator(bool show)
        {
            try
            {
                var loadingIndicator = FindName("LoadingIndicator") as FrameworkElement;
                if (loadingIndicator != null)
                {
                    loadingIndicator.Visibility = show ? Visibility.Visible : Visibility.Collapsed;
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Loading indicator not found: {ex.Message}");
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
        /// Déclenche l'événement PropertyChanged
        /// </summary>
        /// <param name="propertyName">Nom de la propriété modifiée</param>
        protected virtual void OnPropertyChanged([System.Runtime.CompilerServices.CallerMemberName] string propertyName = null)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }

        #endregion

        #region Event Handlers

        /// <summary>
        /// Ouvre la fenêtre d'import Ambre
        /// </summary>
        private void ImportAmbre_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                // Utiliser le DI container pour obtenir ImportAmbreWindow avec toutes ses dépendances
                var importWindow = App.ServiceProvider.GetRequiredService<ImportAmbreWindow>();
                importWindow.ShowDialog();
                
                // Actualiser les données après import
                _ = RefreshAsync();
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Error opening import: {ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        /// <summary>
        /// Ouvre la fenêtre de rapports
        /// </summary>
        private void OpenReports_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var reportsWindow = App.ServiceProvider.GetRequiredService<ReportsWindow>();
                reportsWindow.ShowDialog();
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Error opening reports: {ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        #endregion
    }
}
