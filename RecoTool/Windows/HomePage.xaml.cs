using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using LiveCharts;
using LiveCharts.Wpf;
using Microsoft.Extensions.DependencyInjection;
using RecoTool.Models;
using RecoTool.Services;

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
        private bool _isLoading;
        private bool _canRefresh = true;
        private List<ReconciliationViewData> _reconciliationViewData;
        
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
        private string _statusMessage;
        private string _lastUpdateTime;
        
        // Champs pour les propriétés de graphiques manquantes
        private ChartValues<double> _receivableChartData;
        private ChartValues<double> _pivotChartData;
        private List<string> _actionLabels;

        public event PropertyChangedEventHandler PropertyChanged;

        /// <summary>
        /// Constructeur de la page d'accueil (pour le designer/XAML)
        /// </summary>
        public HomePage()
        {
            InitializeComponent();
            InitializeProperties();
            DataContext = this;
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
            DataContext = this;
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
            _statusMessage = "Prêt";
            _lastUpdateTime = DateTime.Now.ToString("HH:mm:ss");
            
            // Initialiser les nouvelles propriétés de graphiques
            _receivableChartData = new ChartValues<double>();
            _pivotChartData = new ChartValues<double>();
            _actionLabels = new List<string>();
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
        /// Charge les données réelles depuis la base
        /// </summary>
        private async Task LoadDashboardDataAsync()
        {
            try
            {
                IsLoading = true;
                ShowLoadingIndicator(true);

                if (_offlineFirstService == null || string.IsNullOrEmpty(_offlineFirstService.CurrentCountryId))
                {
                    ShowError("Aucune country sélectionnée");
                    return;
                }

                // Charger les données réelles depuis T_Data_Ambre et T_Reconciliation
                await LoadRealDataFromDatabase();
                
                UpdateKPISummary();
                UpdateCharts();
                UpdateCountryInfo();
            }
            catch (Exception ex)
            {
                ShowError($"Erreur lors du chargement des données: {ex.Message}");
                LoadSampleData();
            }
            finally
            {
                IsLoading = false;
                ShowLoadingIndicator(false);
            }
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

                // Utiliser le service existant qui fait déjà la jointure DataAmbre + Reconciliation
                var reconciliationViewData = await _reconciliationService.GetReconciliationViewAsync(_offlineFirstService.CurrentCountryId);
                _reconciliationViewData = reconciliationViewData ?? new List<ReconciliationViewData>();

                // Analyser la répartition des comptes pour diagnostic
                AnalyzeAccountDistribution();

                StatusMessage = $"Données chargées: {_reconciliationViewData.Count} lignes";
                LastUpdateTime = DateTime.Now.ToString("HH:mm:ss");

                System.Diagnostics.Debug.WriteLine($"Données chargées via ReconciliationService: {_reconciliationViewData.Count} lignes pour {_offlineFirstService.CurrentCountryId}");
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Erreur lors du chargement des données: {ex.Message}");
                StatusMessage = "Erreur de chargement des données";
                _reconciliationViewData = new List<ReconciliationViewData>();
                throw new Exception($"Erreur lors du chargement des données de la base: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Convertit une entité OfflineFirst vers DataAmbre (legacy - plus utilisé)
        /// </summary>
        private DataAmbre ConvertToDataAmbre(dynamic entity)
        {
            try
            {
                return new DataAmbre
                {
                    ID = entity.ID?.ToString(),
                    ROWGUID = entity.ROWGUID?.ToString(),
                    Account_ID = entity.Account_ID?.ToString(),
                    CCY = entity.CCY?.ToString(),
                    Country = entity.Country?.ToString(),
                    Event_Num = entity.Event_Num?.ToString(),
                    Folder = entity.Folder?.ToString(),
                    RawLabel = entity.RawLabel?.ToString(),
                    SignedAmount = ConvertToDecimal(entity.SignedAmount),
                    LocalSignedAmount = ConvertToDecimal(entity.LocalSignedAmount),
                    Operation_Date = ConvertToDateTime(entity.Operation_Date),
                    Value_Date = ConvertToDateTime(entity.Value_Date),
                    Reconciliation_Num = entity.Reconciliation_Num?.ToString(),
                    ReconciliationOrigin_Num = entity.ReconciliationOrigin_Num?.ToString()
                };
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Erreur conversion DataAmbre: {ex.Message}");
                return null;
            }
        }

        /// <summary>
        /// Convertit une entité OfflineFirst vers Reconciliation
        /// </summary>
        private Reconciliation ConvertToReconciliation(dynamic entity)
        {
            try
            {
                return new Reconciliation
                {
                    ID = entity.ID?.ToString(),
                    ROWGUID = entity.ROWGUID?.ToString(),
                    DWINGS_GuaranteeID = entity.DWINGS_GuaranteeID?.ToString(),
                    DWINGS_InvoiceID = entity.DWINGS_InvoiceID?.ToString(),
                    DWINGS_CommissionID = entity.DWINGS_CommissionID?.ToString(),
                    Action = ConvertToNullableInt(entity.Action),
                    KPI = ConvertToNullableInt(entity.KPI),
                    Comments = entity.Comments?.ToString(),
                    InternalInvoiceReference = entity.InternalInvoiceReference?.ToString(),
                    FirstClaimDate = ConvertToDateTime(entity.FirstClaimDate),
                    LastClaimDate = ConvertToDateTime(entity.LastClaimDate),
                    ToRemind = ConvertToBool(entity.ToRemind),
                    ToRemindDate = ConvertToDateTime(entity.ToRemindDate),
                    ACK = ConvertToBool(entity.ACK),
                    SwiftCode = entity.SwiftCode?.ToString(),
                    PaymentReference = entity.PaymentReference?.ToString(),
                    IncidentType = ConvertToNullableInt(entity.IncidentType),
                    RiskyItem = ConvertToNullableInt(entity.RiskyItem),
                    ReasonNonRisky = entity.ReasonNonRisky?.ToString()
                };
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Erreur conversion Reconciliation: {ex.Message}");
                return null;
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

        /// <summary>
        /// Charge des données d'exemple pour la démo
        /// </summary>
        private void LoadSampleData()
        {
            try
            {
                // Données de démonstration
                UpdateKPISummaryWithSampleData();
                UpdateChartsWithSampleData();
            }
            catch (Exception ex)
            {
                ShowError($"Erreur lors du chargement des données d'exemple: {ex.Message}");
            }
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

        /// <summary>
        /// Met à jour le résumé des KPI avec des données d'exemple
        /// </summary>
        private void UpdateKPISummaryWithSampleData()
        {
            UpdateTextBlock("TotalLinesText", "1,234");
            UpdateTextBlock("ReconciledText", "987");
            UpdateTextBlock("PendingText", "184");
            UpdateTextBlock("IssuesText", "63");
            UpdateTextBlock("TotalAmountText", "€ 2,456,789.12");
            UpdateTextBlock("ReconciledAmountText", "€ 1,987,654.32");
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
            UpdateReceivablePivotMiniCharts();
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
                    var kpiName = GetKPIName(item.KPI);
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
                ShowError($"Erreur lors de la mise à jour du graphique KPI: {ex.Message}");
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
                ShowError($"Erreur lors de la mise à jour du graphique devise: {ex.Message}");
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

                var seriesCollection = new SeriesCollection();
                var labels = new List<string>();
                var counts = new ChartValues<int>();

                foreach (var item in actionData)
                {
                    var actionName = GetActionName(item.Action);
                    labels.Add(actionName);
                    counts.Add(item.Count);
                }

                seriesCollection.Add(new ColumnSeries
                {
                    Title = "Actions",
                    Values = counts
                });

                // Mettre à jour les propriétés liées au CartesianChart
                ActionLabels = labels;
                ActionDistributionSeries = seriesCollection;
            }
            catch (Exception ex)
            {
                ShowError($"Erreur lors de la mise à jour du graphique actions: {ex.Message}");
            }
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
                System.Diagnostics.Debug.WriteLine($"Erreur mini charts: {ex.Message}");
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
                System.Diagnostics.Debug.WriteLine($"Répartition des comptes pour {currentCountry.CNT_Id}:");
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
                System.Diagnostics.Debug.WriteLine($"Erreur lors de l'analyse de la répartition: {ex.Message}");
            }
        }

        /// <summary>
        /// Met à jour les graphiques avec des données d'exemple
        /// </summary>
        private void UpdateChartsWithSampleData()
        {
            try
            {
                // Graphique KPI exemple
                var kpiSeriesCollection = new SeriesCollection
                {
                    new PieSeries { Title = "Paid But Not Reconciled", Values = new ChartValues<int> { 450 } },
                    new PieSeries { Title = "Under Investigation", Values = new ChartValues<int> { 230 } },
                    new PieSeries { Title = "IT Issues", Values = new ChartValues<int> { 180 } },
                    new PieSeries { Title = "Not Claimed", Values = new ChartValues<int> { 120 } }
                };

                var kpiChart = FindName("KPIChart") as LiveCharts.Wpf.PieChart;
                if (kpiChart != null)
                {
                    kpiChart.Series = kpiSeriesCollection;
                }

                // Graphique devise exemple
                var currencySeriesCollection = new SeriesCollection
                {
                    new ColumnSeries
                    {
                        Title = "Montants par devise",
                        Values = new ChartValues<decimal> { 1200000, 850000, 650000, 450000, 320000 }
                    }
                };

                var currencyChart = FindName("CurrencyChart") as LiveCharts.Wpf.CartesianChart;
                if (currencyChart != null)
                {
                    currencyChart.Series = currencySeriesCollection;
                    currencyChart.AxisX[0].Labels = new[] { "EUR", "USD", "GBP", "JPY", "CHF" };
                }
            }
            catch (Exception ex)
            {
                ShowError($"Erreur lors de la mise à jour des graphiques d'exemple: {ex.Message}");
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
                    MessageBox.Show($"Catégorie: {series.Title}\nValeur: {chartPoint.Y}", 
                                   "Détail KPI", MessageBoxButton.OK, MessageBoxImage.Information);
                }
            }
            catch (Exception ex)
            {
                ShowError($"Erreur lors de l'affichage du détail: {ex.Message}");
            }
        }

        /// <summary>
        /// Rafraîchissement manuel des données
        /// </summary>
        private async void RefreshButton_Click(object sender, RoutedEventArgs e)
        {
            await LoadDashboardDataAsync();
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
                System.Diagnostics.Debug.WriteLine($"TextBlock '{name}' non trouvé: {ex.Message}");
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
                UpdateTextBlock("LastUpdateText", DateTime.Now.ToString("dd/MM/yyyy HH:mm"));
            }
            catch (Exception ex)
            {
                ShowError($"Erreur lors de la mise à jour des informations pays: {ex.Message}");
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
                System.Diagnostics.Debug.WriteLine($"Indicateur de chargement non trouvé: {ex.Message}");
            }
        }

        /// <summary>
        /// Obtient le nom d'un KPI
        /// </summary>
        private string GetKPIName(int kpiId)
        {
            return Enum.GetName(typeof(KPIType), kpiId) ?? $"KPI {kpiId}";
        }

        /// <summary>
        /// Obtient le nom d'une action
        /// </summary>
        private string GetActionName(int actionId)
        {
            return Enum.GetName(typeof(ActionType), actionId) ?? $"Action {actionId}";
        }

        /// <summary>
        /// Affiche un message d'erreur
        /// </summary>
        private void ShowError(string message)
        {
            MessageBox.Show(message, "Erreur", MessageBoxButton.OK, MessageBoxImage.Error);
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
                MessageBox.Show($"Erreur lors de l'ouverture de l'import: {ex.Message}", "Erreur", MessageBoxButton.OK, MessageBoxImage.Error);
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
                MessageBox.Show($"Erreur lors de l'ouverture des rapports: {ex.Message}", "Erreur", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        #endregion
    }
}
