using System;
using System.Collections.ObjectModel;
using System.Windows;

namespace RecoTool.Windows
{
    public partial class BulkEditWindow : Window
    {
        public class BulkEditViewModel : System.ComponentModel.INotifyPropertyChanged
        {
            public ObservableCollection<ReconciliationView.OptionItem> ActionOptions { get; }
            public ObservableCollection<ReconciliationView.OptionItem> KpiOptions { get; }
            public ObservableCollection<ReconciliationView.OptionItem> IncidentTypeOptions { get; }

            private bool _applyAction;
            public bool ApplyAction { get => _applyAction; set { _applyAction = value; OnPropertyChanged(nameof(ApplyAction)); } }

            private bool _applyKpi;
            public bool ApplyKpi { get => _applyKpi; set { _applyKpi = value; OnPropertyChanged(nameof(ApplyKpi)); } }

            private bool _applyIncidentType;
            public bool ApplyIncidentType { get => _applyIncidentType; set { _applyIncidentType = value; OnPropertyChanged(nameof(ApplyIncidentType)); } }

            private int? _selectedActionId;
            public int? SelectedActionId { get => _selectedActionId; set { _selectedActionId = value; OnPropertyChanged(nameof(SelectedActionId)); } }

            private int? _selectedKpiId;
            public int? SelectedKpiId { get => _selectedKpiId; set { _selectedKpiId = value; OnPropertyChanged(nameof(SelectedKpiId)); } }

            private int? _selectedIncidentTypeId;
            public int? SelectedIncidentTypeId { get => _selectedIncidentTypeId; set { _selectedIncidentTypeId = value; OnPropertyChanged(nameof(SelectedIncidentTypeId)); } }

            public BulkEditViewModel(
                ObservableCollection<ReconciliationView.OptionItem> actionOptions,
                ObservableCollection<ReconciliationView.OptionItem> kpiOptions,
                ObservableCollection<ReconciliationView.OptionItem> incidentTypeOptions)
            {
                ActionOptions = actionOptions ?? new ObservableCollection<ReconciliationView.OptionItem>();
                KpiOptions = kpiOptions ?? new ObservableCollection<ReconciliationView.OptionItem>();
                IncidentTypeOptions = incidentTypeOptions ?? new ObservableCollection<ReconciliationView.OptionItem>();
            }

            public event System.ComponentModel.PropertyChangedEventHandler PropertyChanged;
            private void OnPropertyChanged(string name) => PropertyChanged?.Invoke(this, new System.ComponentModel.PropertyChangedEventArgs(name));
        }

        public BulkEditViewModel ViewModel { get; }

        public BulkEditWindow(
            ObservableCollection<ReconciliationView.OptionItem> actionOptions,
            ObservableCollection<ReconciliationView.OptionItem> kpiOptions,
            ObservableCollection<ReconciliationView.OptionItem> incidentTypeOptions)
        {
            InitializeComponent();
            ViewModel = new BulkEditViewModel(actionOptions, kpiOptions, incidentTypeOptions);
            DataContext = ViewModel;
        }

        private void OkButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                // No additional validation required; unchecked fields will be ignored by caller
                this.DialogResult = true;
                this.Close();
            }
            catch
            {
                this.DialogResult = false;
                this.Close();
            }
        }

        private void CancelButton_Click(object sender, RoutedEventArgs e)
        {
            this.DialogResult = false;
            this.Close();
        }
    }
}
