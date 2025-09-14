using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using RecoTool;
using Microsoft.Extensions.DependencyInjection;
using RecoTool.UI.Models;
using RecoTool.Models;
using RecoTool.Services;
using RecoTool.Services.DTOs;

namespace RecoTool.Windows
{
    // Partial: Options (collections + loaders) for ReconciliationView
    public partial class ReconciliationView
    {
        // Thin wrapper for cached option lists
        private OptionsService _optionsService;
        // Options for referential ComboBoxes
        private ObservableCollection<OptionItem> _actionOptions = new ObservableCollection<OptionItem>();
        private ObservableCollection<OptionItem> _kpiOptions = new ObservableCollection<OptionItem>();
        private ObservableCollection<OptionItem> _incidentTypeOptions = new ObservableCollection<OptionItem>();
        public ObservableCollection<OptionItem> ActionOptions { get => _actionOptions; private set { _actionOptions = value; OnPropertyChanged(nameof(ActionOptions)); } }
        public ObservableCollection<OptionItem> KpiOptions { get => _kpiOptions; private set { _kpiOptions = value; OnPropertyChanged(nameof(KpiOptions)); } }
        public ObservableCollection<OptionItem> IncidentTypeOptions { get => _incidentTypeOptions; private set { _incidentTypeOptions = value; OnPropertyChanged(nameof(IncidentTypeOptions)); } }

        // Options for Assignee ComboBox (users from T_User)
        private ObservableCollection<UserOption> _assigneeOptions = new ObservableCollection<UserOption>();
        public ObservableCollection<UserOption> AssigneeOptions
        {
            get => _assigneeOptions;
            private set { _assigneeOptions = value; OnPropertyChanged(nameof(AssigneeOptions)); }
        }

        // Dynamic options for filter ComboBoxes (Currency / Guarantee Type / Guarantee Status)
        private ObservableCollection<string> _currencyOptions = new ObservableCollection<string>();
        private ObservableCollection<string> _guaranteeTypeOptions = new ObservableCollection<string>();
        private ObservableCollection<string> _guaranteeStatusOptions = new ObservableCollection<string>();
        public ObservableCollection<string> CurrencyOptions { get => _currencyOptions; private set { _currencyOptions = value; OnPropertyChanged(nameof(CurrencyOptions)); } }
        public ObservableCollection<string> GuaranteeTypeOptions { get => _guaranteeTypeOptions; private set { _guaranteeTypeOptions = value; OnPropertyChanged(nameof(GuaranteeTypeOptions)); } }
        public ObservableCollection<string> GuaranteeStatusOptions { get => _guaranteeStatusOptions; private set { _guaranteeStatusOptions = value; OnPropertyChanged(nameof(GuaranteeStatusOptions)); } }

        // TransactionType options are now owned by the ViewModel (VM.TransactionTypeOptions)

        // Build Action/KPI/Incident user-field referential options
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

        // Load Assignee options (users)
        private async Task LoadAssigneeOptionsAsync()
        {
            try
            {
                if (_reconciliationService == null) return;
                if (_optionsService == null)
                {
                    _optionsService = App.ServiceProvider?.GetService<OptionsService>()
                        ?? new OptionsService(
                            _reconciliationService,
                            new ReferentialService(_offlineFirstService, _reconciliationService?.CurrentUser),
                            new LookupService(_offlineFirstService));
                }
                var users = await _optionsService.GetUsersAsync();
                AssigneeOptions.Clear();
                AssigneeOptions.Add(new UserOption { Id = null, Name = string.Empty });
                foreach (var u in users)
                {
                    AssigneeOptions.Add(new UserOption { Id = u.Id, Name = string.IsNullOrWhiteSpace(u.Name) ? u.Id : u.Name });
                }
            }
            catch { }
        }

        // Load currency options
        private async Task LoadCurrencyOptionsAsync()
        {
            try
            {
                if (_reconciliationService == null) return;
                if (_optionsService == null)
                {
                    _optionsService = App.ServiceProvider?.GetService<OptionsService>()
                        ?? new OptionsService(
                            _reconciliationService,
                            new ReferentialService(_offlineFirstService, _reconciliationService?.CurrentUser),
                            new LookupService(_offlineFirstService));
                }
                var countryId = _currentCountryId ?? _offlineFirstService?.CurrentCountry?.CNT_Id;
                CurrencyOptions.Clear();
                CurrencyOptions.Add(string.Empty);
                if (string.IsNullOrWhiteSpace(countryId)) return;
                var list = await _optionsService.GetCurrenciesAsync(countryId);
                foreach (var s in list)
                {
                    if (!string.IsNullOrWhiteSpace(s)) CurrencyOptions.Add(s);
                }
            }
            catch { }
        }

        // Load Guarantee Status values
        private async Task LoadGuaranteeStatusOptionsAsync()
        {
            try
            {
                if (_reconciliationService == null) return;
                if (_optionsService == null)
                {
                    _optionsService = App.ServiceProvider?.GetService<OptionsService>()
                        ?? new OptionsService(
                            _reconciliationService,
                            new ReferentialService(_offlineFirstService, _reconciliationService?.CurrentUser),
                            new LookupService(_offlineFirstService));
                }
                GuaranteeStatusOptions.Clear();
                GuaranteeStatusOptions.Add(string.Empty);
                var list = await _optionsService.GetGuaranteeStatusesAsync();
                foreach (var s in list)
                {
                    if (!string.IsNullOrWhiteSpace(s)) GuaranteeStatusOptions.Add(s);
                }
            }
            catch { }
        }

        // Load Guarantee Type values (mapped to UI-friendly display)
        private async Task LoadGuaranteeTypeOptionsAsync()
        {
            try
            {
                if (_reconciliationService == null) return;
                if (_optionsService == null)
                {
                    _optionsService = App.ServiceProvider?.GetService<OptionsService>()
                        ?? new OptionsService(
                            _reconciliationService,
                            new ReferentialService(_offlineFirstService, _reconciliationService?.CurrentUser),
                            new LookupService(_offlineFirstService));
                }
                GuaranteeTypeOptions.Clear();
                GuaranteeTypeOptions.Add(string.Empty);
                var raw = await _optionsService.GetGuaranteeTypesAsync();
                var conv = new GuaranteeTypeDisplayConverter();
                foreach (var code in raw)
                {
                    if (string.IsNullOrWhiteSpace(code)) continue;
                    var ui = conv.Convert(code, typeof(string), null, System.Globalization.CultureInfo.InvariantCulture)?.ToString();
                    if (!string.IsNullOrWhiteSpace(ui) && !GuaranteeTypeOptions.Any(s => string.Equals(s, ui, StringComparison.OrdinalIgnoreCase)))
                        GuaranteeTypeOptions.Add(ui);
                }
            }
            catch { }
        }
    }
}
