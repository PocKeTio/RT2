using System;
using System.Collections.ObjectModel;
using System.Collections.Generic;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Linq;
using RecoTool.Services.DTOs;
using RecoTool.Domain.Filters;
using RecoTool.UI.Models;

namespace RecoTool.UI.ViewModels
{
    /// <summary>
    /// MVVM skeleton for ReconciliationView. Holds the list bound to the grid and future filter state.
    /// </summary>
    public sealed class ReconciliationViewViewModel : INotifyPropertyChanged
    {
        private ObservableCollection<ReconciliationViewData> _viewData = new ObservableCollection<ReconciliationViewData>();
        public ObservableCollection<ReconciliationViewData> ViewData
        {
            get => _viewData;
            set { _viewData = value ?? new ObservableCollection<ReconciliationViewData>(); OnPropertyChanged(); }
        }

        // Centralized filter state for the view
        private FilterState _currentFilter = new FilterState();
        public FilterState CurrentFilter
        {
            get => _currentFilter;
            set { _currentFilter = value ?? new FilterState(); OnPropertyChanged(); }
        }

        // Wrapper properties for XAML binding (TwoWay) that proxy to CurrentFilter
        public string FilterReconciliationNum
        {
            get => CurrentFilter.ReconciliationNum;
            set { if (CurrentFilter.ReconciliationNum != value) { CurrentFilter.ReconciliationNum = value; OnPropertyChanged(); } }
        }

        public string FilterRawLabel
        {
            get => CurrentFilter.RawLabel;
            set { if (CurrentFilter.RawLabel != value) { CurrentFilter.RawLabel = value; OnPropertyChanged(); } }
        }

        public string FilterEventNum
        {
            get => CurrentFilter.EventNum;
            set { if (CurrentFilter.EventNum != value) { CurrentFilter.EventNum = value; OnPropertyChanged(); } }
        }

        public string FilterComments
        {
            get => CurrentFilter.Comments;
            set { if (CurrentFilter.Comments != value) { CurrentFilter.Comments = value; OnPropertyChanged(); } }
        }

        public DateTime? FilterFromDate
        {
            get => CurrentFilter.FromDate;
            set { if (CurrentFilter.FromDate != value) { CurrentFilter.FromDate = value; OnPropertyChanged(); } }
        }

        public DateTime? FilterToDate
        {
            get => CurrentFilter.ToDate;
            set { if (CurrentFilter.ToDate != value) { CurrentFilter.ToDate = value; OnPropertyChanged(); } }
        }

        public DateTime? FilterDeletedDate
        {
            get => CurrentFilter.DeletedDate;
            set { if (CurrentFilter.DeletedDate != value) { CurrentFilter.DeletedDate = value; OnPropertyChanged(); } }
        }

        public string FilterCurrency
        {
            get => CurrentFilter.Currency;
            set { if (CurrentFilter.Currency != value) { CurrentFilter.Currency = value; OnPropertyChanged(); } }
        }

        public decimal? FilterMinAmount
        {
            get => CurrentFilter.MinAmount;
            set { if (CurrentFilter.MinAmount != value) { CurrentFilter.MinAmount = value; OnPropertyChanged(); } }
        }

        public decimal? FilterMaxAmount
        {
            get => CurrentFilter.MaxAmount;
            set { if (CurrentFilter.MaxAmount != value) { CurrentFilter.MaxAmount = value; OnPropertyChanged(); } }
        }

        public string FilterDwGuaranteeId
        {
            get => CurrentFilter.DwGuaranteeId;
            set { if (CurrentFilter.DwGuaranteeId != value) { CurrentFilter.DwGuaranteeId = value; OnPropertyChanged(); } }
        }

        public string FilterDwCommissionId
        {
            get => CurrentFilter.DwCommissionId;
            set { if (CurrentFilter.DwCommissionId != value) { CurrentFilter.DwCommissionId = value; OnPropertyChanged(); } }
        }

        public string FilterGuaranteeType
        {
            get => CurrentFilter.GuaranteeType;
            set { if (CurrentFilter.GuaranteeType != value) { CurrentFilter.GuaranteeType = value; OnPropertyChanged(); } }
        }

        public int? FilterTransactionTypeId
        {
            get => CurrentFilter.TransactionTypeId;
            set { if (CurrentFilter.TransactionTypeId != value) { CurrentFilter.TransactionTypeId = value; OnPropertyChanged(); } }
        }

        public string FilterGuaranteeStatus
        {
            get => CurrentFilter.GuaranteeStatus;
            set { if (CurrentFilter.GuaranteeStatus != value) { CurrentFilter.GuaranteeStatus = value; OnPropertyChanged(); } }
        }

        public int? FilterActionId
        {
            get => CurrentFilter.ActionId;
            set { if (CurrentFilter.ActionId != value) { CurrentFilter.ActionId = value; OnPropertyChanged(); } }
        }

        public int? FilterKpiId
        {
            get => CurrentFilter.KpiId;
            set { if (CurrentFilter.KpiId != value) { CurrentFilter.KpiId = value; OnPropertyChanged(); } }
        }

        public int? FilterIncidentTypeId
        {
            get => CurrentFilter.IncidentTypeId;
            set { if (CurrentFilter.IncidentTypeId != value) { CurrentFilter.IncidentTypeId = value; OnPropertyChanged(); } }
        }

        public bool? FilterActionDone
        {
            get => CurrentFilter.ActionDone;
            set { if (CurrentFilter.ActionDone != value) { CurrentFilter.ActionDone = value; OnPropertyChanged(); } }
        }

        public DateTime? FilterActionDateFrom
        {
            get => CurrentFilter.ActionDateFrom;
            set { if (CurrentFilter.ActionDateFrom != value) { CurrentFilter.ActionDateFrom = value; OnPropertyChanged(); } }
        }

        public DateTime? FilterActionDateTo
        {
            get => CurrentFilter.ActionDateTo;
            set { if (CurrentFilter.ActionDateTo != value) { CurrentFilter.ActionDateTo = value; OnPropertyChanged(); } }
        }

        public bool FilterPotentialDuplicates
        {
            get => CurrentFilter.PotentialDuplicates == true;
            set { var v = (bool?)value; if (CurrentFilter.PotentialDuplicates != v) { CurrentFilter.PotentialDuplicates = v; OnPropertyChanged(); } }
        }

        // New filters
        public bool FilterUnmatched
        {
            get => CurrentFilter.Unmatched == true;
            set { var v = (bool?)value; if (CurrentFilter.Unmatched != v) { CurrentFilter.Unmatched = v; OnPropertyChanged(); } }
        }

        public bool FilterNewLines
        {
            get => CurrentFilter.NewLines == true;
            set { var v = (bool?)value; if (CurrentFilter.NewLines != v) { CurrentFilter.NewLines = v; OnPropertyChanged(); } }
        }

        public string FilterAssigneeId
        {
            get => CurrentFilter.AssigneeId;
            set { if (CurrentFilter.AssigneeId != value) { CurrentFilter.AssigneeId = value; OnPropertyChanged(); } }
        }

        public string FilterAccountId
        {
            get => CurrentFilter.AccountId;
            set { if (CurrentFilter.AccountId != value) { CurrentFilter.AccountId = value; OnPropertyChanged(); } }
        }

        public string FilterStatus
        {
            get => CurrentFilter.Status;
            set { if (CurrentFilter.Status != value) { CurrentFilter.Status = value; OnPropertyChanged(); } }
        }

        /// <summary>
        /// Apply filters held in CurrentFilter to the provided dataset.
        /// If excludeTransactionType is true, the TransactionTypeId step is skipped (used to refresh options).
        /// </summary>
        public List<ReconciliationViewData> ApplyFilters(IEnumerable<ReconciliationViewData> source, bool excludeTransactionType = false)
        {
            if (source == null) return new List<ReconciliationViewData>();
            var f = CurrentFilter ?? new FilterState();
            var q = source;

            // Account
            if (!string.IsNullOrWhiteSpace(f.AccountId))
            {
                var id = f.AccountId.Trim();
                q = q.Where(x => string.Equals(x.Account_ID?.Trim(), id, System.StringComparison.OrdinalIgnoreCase));
            }

            // Currency contains
            if (!string.IsNullOrWhiteSpace(f.Currency))
            {
                var c = f.Currency;
                q = q.Where(x => x.CCY != null && x.CCY.IndexOf(c, System.StringComparison.OrdinalIgnoreCase) >= 0);
            }

            // Amount range
            if (f.MinAmount.HasValue) q = q.Where(x => x.SignedAmount >= f.MinAmount.Value);
            if (f.MaxAmount.HasValue) q = q.Where(x => x.SignedAmount <= f.MaxAmount.Value);

            // Date range (operation)
            if (f.FromDate.HasValue) q = q.Where(x => x.Operation_Date.HasValue && x.Operation_Date.Value >= f.FromDate.Value);
            if (f.ToDate.HasValue) q = q.Where(x => x.Operation_Date.HasValue && x.Operation_Date.Value <= f.ToDate.Value);

            // Reference contains
            if (!string.IsNullOrWhiteSpace(f.ReconciliationNum))
                q = q.Where(x => (x.Reconciliation_Num ?? string.Empty).IndexOf(f.ReconciliationNum, System.StringComparison.OrdinalIgnoreCase) >= 0);
            if (!string.IsNullOrWhiteSpace(f.RawLabel))
                q = q.Where(x => (x.RawLabel ?? string.Empty).IndexOf(f.RawLabel, System.StringComparison.OrdinalIgnoreCase) >= 0);
            if (!string.IsNullOrWhiteSpace(f.EventNum))
                q = q.Where(x => (x.Event_Num ?? string.Empty).IndexOf(f.EventNum, System.StringComparison.OrdinalIgnoreCase) >= 0);

            // Comments contains (client-side filter on view data)
            if (!string.IsNullOrWhiteSpace(f.Comments))
                q = q.Where(x => (x.Comments ?? string.Empty).IndexOf(f.Comments, System.StringComparison.OrdinalIgnoreCase) >= 0);

            // Guarantee status contains
            if (!string.IsNullOrWhiteSpace(f.GuaranteeStatus))
            {
                var gs = f.GuaranteeStatus.Trim();
                q = q.Where(x => (x.GUARANTEE_STATUS ?? string.Empty).IndexOf(gs, System.StringComparison.OrdinalIgnoreCase) >= 0);
            }

            // DW ids
            if (!string.IsNullOrWhiteSpace(f.DwGuaranteeId))
            {
                var id = f.DwGuaranteeId;
                q = q.Where(x => (x.DWINGS_GuaranteeID ?? string.Empty).IndexOf(id, System.StringComparison.OrdinalIgnoreCase) >= 0
                                 || (x.GUARANTEE_ID ?? string.Empty).IndexOf(id, System.StringComparison.OrdinalIgnoreCase) >= 0);
            }
            if (!string.IsNullOrWhiteSpace(f.DwCommissionId))
            {
                var id = f.DwCommissionId;
                q = q.Where(x => (x.DWINGS_CommissionID ?? string.Empty).IndexOf(id, System.StringComparison.OrdinalIgnoreCase) >= 0
                                 || (x.COMMISSION_ID ?? string.Empty).IndexOf(id, System.StringComparison.OrdinalIgnoreCase) >= 0);
            }

            // Deleted date exact day
            if (f.DeletedDate.HasValue)
            {
                var day = f.DeletedDate.Value.Date;
                var next = day.AddDays(1);
                q = q.Where(a => a.DeleteDate.HasValue && a.DeleteDate.Value >= day && a.DeleteDate.Value < next);
            }

            // Status (Live/Archived)
            if (!string.IsNullOrWhiteSpace(f.Status) && !string.Equals(f.Status, "All", System.StringComparison.OrdinalIgnoreCase))
            {
                if (string.Equals(f.Status, "Live", System.StringComparison.OrdinalIgnoreCase))
                    q = q.Where(a => !a.DeleteDate.HasValue);
                else if (string.Equals(f.Status, "Archived", System.StringComparison.OrdinalIgnoreCase))
                    q = q.Where(a => a.DeleteDate.HasValue);
            }

            // Transaction type by enum id (Category)
            if (!excludeTransactionType && f.TransactionTypeId.HasValue)
            {
                var t = f.TransactionTypeId.Value;
                q = q.Where(x => x.Category.HasValue && x.Category.Value == t);
            }

            // Referential filters
            if (f.ActionId.HasValue) q = q.Where(x => x.Action == f.ActionId);
            if (f.KpiId.HasValue) q = q.Where(x => x.KPI == f.KpiId);
            if (f.IncidentTypeId.HasValue) q = q.Where(x => x.IncidentType == f.IncidentTypeId);
            if (!string.IsNullOrWhiteSpace(f.AssigneeId)) q = q.Where(x => string.Equals(x.Assignee, f.AssigneeId, System.StringComparison.OrdinalIgnoreCase));

            // Potential Duplicates
            if (f.PotentialDuplicates == true) q = q.Where(x => x.IsPotentialDuplicate);

            // Unmatched: no invoice linked (DWINGS invoice id AND InternalInvoiceReference are blank)
            if (f.Unmatched == true)
            {
                q = q.Where(x => string.IsNullOrWhiteSpace(x.DWINGS_InvoiceID)
                                 && string.IsNullOrWhiteSpace(x.InternalInvoiceReference));
            }

            // New lines: appeared in Ambre today (based on AMBRE CreationDate)
            if (f.NewLines == true)
            {
                var today = DateTime.Today;
                q = q.Where(x => x.CreationDate.HasValue && x.CreationDate.Value.Date == today);
            }

            // Action Done and Action Date
            if (f.ActionDone.HasValue)
            {
                if (f.ActionDone.Value) q = q.Where(x => x.ActionStatus == true);
                else q = q.Where(x => x.ActionStatus == false);
            }
            if (f.ActionDateFrom.HasValue) q = q.Where(x => x.ActionDate.HasValue && x.ActionDate.Value >= f.ActionDateFrom.Value);
            if (f.ActionDateTo.HasValue) q = q.Where(x => x.ActionDate.HasValue && x.ActionDate.Value <= f.ActionDateTo.Value);

            return q.ToList();
        }

        // TransactionType options for UI binding
        private ObservableCollection<OptionItem> _transactionTypeOptions = new ObservableCollection<OptionItem>();
        public ObservableCollection<OptionItem> TransactionTypeOptions
        {
            get => _transactionTypeOptions;
            private set { _transactionTypeOptions = value ?? new ObservableCollection<OptionItem>(); OnPropertyChanged(nameof(TransactionTypeOptions)); }
        }

        public void LoadTransactionTypeOptions()
        {
            try
            {
                var list = new List<OptionItem> { new OptionItem { Id = -1, Name = string.Empty } };
                foreach (var v in Enum.GetValues(typeof(TransactionType)).Cast<TransactionType>())
                {
                    list.Add(new OptionItem { Id = (int)v, Name = v.ToString().Replace('_', ' ') });
                }
                TransactionTypeOptions = new ObservableCollection<OptionItem>(list);
            }
            catch { }
        }

        public void UpdateTransactionTypeOptionsForData(IEnumerable<ReconciliationViewData> data)
        {
            try
            {
                if (data == null) return;
                var ids = data.Where(x => x?.Category != null)
                              .Select(x => x.Category.Value)
                              .Distinct()
                              .OrderBy(i => i)
                              .ToList();
                var target = new List<OptionItem> { new OptionItem { Id = -1, Name = string.Empty } };
                foreach (var id in ids)
                {
                    var name = Enum.GetName(typeof(TransactionType), id)?.Replace('_', ' ') ?? id.ToString();
                    target.Add(new OptionItem { Id = id, Name = name });
                }
                var same = (TransactionTypeOptions?.Count ?? 0) == target.Count
                           && TransactionTypeOptions.Zip(target, (a, b) => a.Id == b.Id && string.Equals(a.Name, b.Name, StringComparison.Ordinal)).All(eq => eq);
                if (!same)
                    TransactionTypeOptions = new ObservableCollection<OptionItem>(target);

                // Coerce selection to All if not available
                if (FilterTransactionTypeId.HasValue && !ids.Contains(FilterTransactionTypeId.Value))
                    FilterTransactionTypeId = null;
            }
            catch { }
        }

        public event PropertyChangedEventHandler PropertyChanged;
        private void OnPropertyChanged([CallerMemberName] string name = null) => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
    }
}
