using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using System.Windows.Media;
using LiveCharts;
using LiveCharts.Wpf;
using RecoTool.Models;
using RecoTool.Services;
using RecoTool.Services.Analytics;
using RecoTool.Services.DTOs;

namespace RecoTool.UI.ViewModels
{
    /// <summary>
    /// ViewModel for HomePage - separates business logic from UI
    /// </summary>
    public class HomePageViewModel : INotifyPropertyChanged
    {
        #region Services
        private readonly ReconciliationService _reconciliationService;
        private readonly OfflineFirstService _offlineFirstService;
        private readonly KpiSnapshotService _kpiSnapshotService;
        #endregion

        #region Observable Properties
        private bool _isLoading;
        public bool IsLoading
        {
            get => _isLoading;
            set => SetProperty(ref _isLoading, value);
        }

        private string _statusMessage;
        public string StatusMessage
        {
            get => _statusMessage;
            set => SetProperty(ref _statusMessage, value);
        }

        private int _totalLiveCount;
        public int TotalLiveCount
        {
            get => _totalLiveCount;
            set => SetProperty(ref _totalLiveCount, value);
        }

        private double _matchedPercentage;
        public double MatchedPercentage
        {
            get => _matchedPercentage;
            set => SetProperty(ref _matchedPercentage, value);
        }

        private int _totalToReviewCount;
        public int TotalToReviewCount
        {
            get => _totalToReviewCount;
            set => SetProperty(ref _totalToReviewCount, value);
        }

        private int _reviewedTodayCount;
        public int ReviewedTodayCount
        {
            get => _reviewedTodayCount;
            set => SetProperty(ref _reviewedTodayCount, value);
        }

        private decimal _totalReceivableAmount;
        public decimal TotalReceivableAmount
        {
            get => _totalReceivableAmount;
            set => SetProperty(ref _totalReceivableAmount, value);
        }

        private int _receivableAccountsCount;
        public int ReceivableAccountsCount
        {
            get => _receivableAccountsCount;
            set => SetProperty(ref _receivableAccountsCount, value);
        }

        private decimal _totalPivotAmount;
        public decimal TotalPivotAmount
        {
            get => _totalPivotAmount;
            set => SetProperty(ref _totalPivotAmount, value);
        }

        private int _pivotAccountsCount;
        public int PivotAccountsCount
        {
            get => _pivotAccountsCount;
            set => SetProperty(ref _pivotAccountsCount, value);
        }

        private SeriesCollection _currencyDistributionSeries;
        public SeriesCollection CurrencyDistributionSeries
        {
            get => _currencyDistributionSeries;
            set => SetProperty(ref _currencyDistributionSeries, value);
        }

        private SeriesCollection _actionDistributionSeries;
        public SeriesCollection ActionDistributionSeries
        {
            get => _actionDistributionSeries;
            set => SetProperty(ref _actionDistributionSeries, value);
        }

        private ObservableCollection<TodoCard> _todoCards;
        public ObservableCollection<TodoCard> TodoCards
        {
            get => _todoCards;
            set => SetProperty(ref _todoCards, value);
        }

        private ObservableCollection<AlertItem> _alertItems;
        public ObservableCollection<AlertItem> AlertItems
        {
            get => _alertItems;
            set => SetProperty(ref _alertItems, value);
        }

        #endregion

        #region Constructor
        public HomePageViewModel(
            ReconciliationService reconciliationService,
            OfflineFirstService offlineFirstService)
        {
            _reconciliationService = reconciliationService ?? throw new ArgumentNullException(nameof(reconciliationService));
            _offlineFirstService = offlineFirstService ?? throw new ArgumentNullException(nameof(offlineFirstService));
            _kpiSnapshotService = new KpiSnapshotService(_offlineFirstService, _reconciliationService);

            InitializeProperties();
        }
        #endregion

        #region Initialization
        private void InitializeProperties()
        {
            StatusMessage = "Ready";
            CurrencyDistributionSeries = new SeriesCollection();
            ActionDistributionSeries = new SeriesCollection();
            TodoCards = new ObservableCollection<TodoCard>();
            AlertItems = new ObservableCollection<AlertItem>();
        }
        #endregion

        #region Public Methods
        public async Task LoadDashboardAsync()
        {
            try
            {
                IsLoading = true;
                StatusMessage = "Loading data...";

                var data = await LoadReconciliationDataAsync();
                if (data == null || !data.Any())
                {
                    StatusMessage = "No data available";
                    return;
                }

                UpdateKpiSummary(data);
                UpdateCharts(data);
                await UpdateTodoCardsAsync(data);
                UpdateAnalytics(data);

                StatusMessage = $"Data loaded: {data.Count} rows";
            }
            catch (Exception ex)
            {
                StatusMessage = $"Error: {ex.Message}";
            }
            finally
            {
                IsLoading = false;
            }
        }
        #endregion

        #region Private Data Loading
        private async Task<List<ReconciliationViewData>> LoadReconciliationDataAsync()
        {
            if (_reconciliationService == null || string.IsNullOrEmpty(_offlineFirstService?.CurrentCountryId))
                return new List<ReconciliationViewData>();

            return await _reconciliationService.GetReconciliationViewAsync(
                _offlineFirstService.CurrentCountryId, 
                null, 
                dashboardOnly: true);
        }
        #endregion

        #region Private KPI Updates
        private void UpdateKpiSummary(List<ReconciliationViewData> data)
        {
            var currentCountry = _offlineFirstService?.CurrentCountry;
            if (currentCountry == null) return;

            TotalLiveCount = data.Count;

            var receivableData = data.Where(r => r.Account_ID == currentCountry.CNT_AmbreReceivable).ToList();
            var pivotData = data.Where(r => r.Account_ID == currentCountry.CNT_AmbrePivot).ToList();

            TotalReceivableAmount = receivableData.Sum(r => r.SignedAmount);
            ReceivableAccountsCount = receivableData.Count;
            TotalPivotAmount = pivotData.Sum(r => r.SignedAmount);
            PivotAccountsCount = pivotData.Count;

            int matchedCount = data.Count(r => !string.IsNullOrWhiteSpace(r.DWINGS_GuaranteeID)
                                            || !string.IsNullOrWhiteSpace(r.DWINGS_InvoiceID)
                                            || !string.IsNullOrWhiteSpace(r.DWINGS_BGPMT));
            MatchedPercentage = TotalLiveCount > 0 ? (matchedCount * 100.0 / TotalLiveCount) : 0.0;

            TotalToReviewCount = data.Count(r => r.IsToReview);
            ReviewedTodayCount = data.Count(r => r.IsReviewedToday);
        }
        #endregion

        #region Private Chart Updates
        private void UpdateCharts(List<ReconciliationViewData> data)
        {
            UpdateCurrencyChart(data);
            UpdateActionChart(data);
        }

        private void UpdateCurrencyChart(List<ReconciliationViewData> data)
        {
            var grouped = data
                .Where(r => !string.IsNullOrWhiteSpace(r.CCY))
                .GroupBy(r => r.CCY.Trim().ToUpperInvariant())
                .Select(g => new { Currency = g.Key, Amount = g.Sum(x => Math.Abs(x.SignedAmount)) })
                .OrderByDescending(x => x.Amount)
                .Take(10)
                .ToList();

            var series = new SeriesCollection();
            foreach (var item in grouped)
            {
                series.Add(new PieSeries
                {
                    Title = item.Currency,
                    Values = new ChartValues<double> { Convert.ToDouble(item.Amount) },
                    DataLabels = true
                });
            }

            CurrencyDistributionSeries = series;
        }

        private void UpdateActionChart(List<ReconciliationViewData> data)
        {
            var userFields = _offlineFirstService?.UserFields ?? new List<UserField>();
            var actionFieldMap = userFields
                .Where(u => string.Equals(u.USR_Category, "Action", StringComparison.OrdinalIgnoreCase))
                .ToDictionary(u => u.USR_ID, u => u);

            var grouped = data
                .Where(r => r.Action.HasValue)
                .GroupBy(r => r.Action.Value)
                .Select(g => new
                {
                    ActionId = g.Key,
                    Count = g.Count(),
                    Label = GetActionLabel(g.Key, actionFieldMap)
                })
                .OrderBy(x => x.Label)
                .ToList();

            var series = new SeriesCollection();
            foreach (var item in grouped)
            {
                series.Add(new PieSeries
                {
                    Title = item.Label,
                    Values = new ChartValues<int> { item.Count },
                    DataLabels = true
                });
            }

            ActionDistributionSeries = series;
        }

        private string GetActionLabel(int actionId, Dictionary<int, UserField> actionFieldMap)
        {
            if (actionFieldMap.TryGetValue(actionId, out var uf) && uf != null)
            {
                if (!string.IsNullOrWhiteSpace(uf.USR_FieldName))
                    return uf.USR_FieldName;
                if (!string.IsNullOrWhiteSpace(uf.USR_FieldDescription))
                    return uf.USR_FieldDescription;
            }
            return $"Action {actionId}";
        }
        #endregion

        #region Private Todo & Analytics
        private async Task UpdateTodoCardsAsync(List<ReconciliationViewData> data)
        {
            // TODO: Implement TodoCard loading logic
            await Task.CompletedTask;
        }

        private void UpdateAnalytics(List<ReconciliationViewData> data)
        {
            var alerts = DashboardAnalyticsService.GetUrgentAlerts(data);
            AlertItems = new ObservableCollection<AlertItem>(alerts);
        }
        #endregion

        #region INotifyPropertyChanged Implementation
        public event PropertyChangedEventHandler PropertyChanged;

        protected virtual void OnPropertyChanged([CallerMemberName] string propertyName = null)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }

        protected bool SetProperty<T>(ref T field, T value, [CallerMemberName] string propertyName = null)
        {
            if (EqualityComparer<T>.Default.Equals(field, value))
                return false;

            field = value;
            OnPropertyChanged(propertyName);
            return true;
        }
        #endregion

        #region Nested Types (temporary - should be moved to separate files)
        public class TodoCard : INotifyPropertyChanged
        {
            private int _activeUsersCount;
            public int ActiveUsersCount
            {
                get => _activeUsersCount;
                set
                {
                    _activeUsersCount = value;
                    OnPropertyChanged(nameof(ActiveUsersCount));
                }
            }

            public event PropertyChangedEventHandler PropertyChanged;
            private void OnPropertyChanged(string propertyName)
            {
                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
            }
        }
        #endregion
    }
}
