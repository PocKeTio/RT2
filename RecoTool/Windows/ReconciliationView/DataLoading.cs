using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using RecoTool.Services.DTOs;

namespace RecoTool.Windows
{
    // Partial: Initialization, data loading, service sync for ReconciliationView
    public partial class ReconciliationView
    {
        /// <summary>
        /// Initialise les données par défaut
        /// </summary>
        private void InitializeData()
        {
            _viewData = new ObservableCollection<ReconciliationViewData>();
            _allViewData = new List<ReconciliationViewData>();
            // Keep VM in sync with initial collection used across the view
            try { VM.ViewData = _viewData; } catch { }
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
                    // Charger les utilisateurs pour le filtre d'assignation
                    await LoadAssigneeOptionsAsync();

                    // Charger dynamiquement les options des filtres de haut de page
                    await LoadGuaranteeStatusOptionsAsync();
                    await LoadGuaranteeTypeOptionsAsync();
                    await LoadCurrencyOptionsAsync();
                    // Charger les types de transaction (enum) dans le VM
                    VM.LoadTransactionTypeOptions();
                }
                // Ne pas effectuer de chargement automatique ici; la page parente appliquera
                // les filtres et la mise en page, puis appellera explicitement Refresh().
            }
            catch (Exception ex)
            {
                // Log l'erreur si nécessaire
                System.Diagnostics.Debug.WriteLine($"Error initializing services: {ex.Message}");
            }
        }

        private void ReconciliationView_Loaded(object sender, RoutedEventArgs e)
        {
            // Make sure we are subscribed (handles re-loads)
            SubscribeToSyncEvents();
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
                ShowError($"Error during initial load: {ex.Message}");
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
                UpdateStatusInfo("Loading data...");
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
                UpdateStatusInfo($"{ViewData?.Count ?? 0} lines loaded");
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
                ShowError($"Error loading data: {ex.Message}");
                UpdateStatusInfo("Load error");
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
                FilterAccountId = string.Equals(acc, "All", StringComparison.OrdinalIgnoreCase) ? null : ResolveAccountIdForFilter(acc);
                // Status = All/Active/Deleted (use IsDeleted)
                FilterStatus = stat;
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
                    var inner = display.Substring(open + 1, (close - open - 1)).Trim();
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
        public void SyncCountryFromService(bool refresh = true)
        {
            try
            {
                var cc = _offlineFirstService?.CurrentCountry;
                var cid = cc?.CNT_Id;
                if (string.IsNullOrWhiteSpace(cid))
                {
                    // Fallback to CurrentCountryId if CurrentCountry object isn't yet populated
                    try { cid = _offlineFirstService?.CurrentCountryId; } catch { }
                }
                _currentCountryId = cid;
                _filterCountry = cc?.CNT_Name;
                UpdateCountryPivotReceivableInfo();

                // Recharger les options de devise dépendantes du pays
                _ = LoadCurrencyOptionsAsync();
                _ = LoadGuaranteeTypeOptionsAsync();
                _ = LoadGuaranteeStatusOptionsAsync();
                if (refresh)
                    Refresh();
            }
            catch { }
        }
    }
}
