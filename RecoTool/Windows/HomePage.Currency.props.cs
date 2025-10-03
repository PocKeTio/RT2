using System.Collections.Generic;
using LiveCharts;

namespace RecoTool.Windows
{
    public partial class HomePage
    {
        public SeriesCollection ReceivablePivotByCurrencySeries
        {
            get => _receivablePivotByCurrencySeries;
            set
            {
                _receivablePivotByCurrencySeries = value;
                OnPropertyChanged();
            }
        }

        public List<string> ReceivablePivotByCurrencyLabels
        {
            get => _receivablePivotByCurrencyLabels;
            set
            {
                _receivablePivotByCurrencyLabels = value;
                OnPropertyChanged();
            }
        }
    }
}
