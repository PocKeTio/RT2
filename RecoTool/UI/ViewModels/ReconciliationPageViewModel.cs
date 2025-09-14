using System;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using RecoTool.Services;

namespace RecoTool.UI.ViewModels
{
    /// <summary>
    /// MVVM skeleton for ReconciliationPage. Not yet wired; used to gradually migrate logic out of code-behind.
    /// </summary>
    public sealed class ReconciliationPageViewModel : INotifyPropertyChanged
    {
        private readonly ReconciliationService _reconciliationService;
        private readonly OfflineFirstService _offlineFirstService;

        public ReconciliationPageViewModel(ReconciliationService reconciliationService, OfflineFirstService offlineFirstService)
        {
            _reconciliationService = reconciliationService;
            _offlineFirstService = offlineFirstService;
        }

        private string _selectedAccount;
        public string SelectedAccount
        {
            get => _selectedAccount;
            set { _selectedAccount = value; OnPropertyChanged(); }
        }

        private string _selectedStatus;
        public string SelectedStatus
        {
            get => _selectedStatus;
            set { _selectedStatus = value; OnPropertyChanged(); }
        }

        public async Task InitializeAsync(CancellationToken token = default)
        {
            await Task.CompletedTask;
        }

        public event PropertyChangedEventHandler PropertyChanged;
        private void OnPropertyChanged([CallerMemberName] string name = null) => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
    }
}
