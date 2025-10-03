using System;
using System.ComponentModel;

namespace RecoTool.Windows
{
    public partial class HomePage
    {
        protected override void OnInitialized(EventArgs e)
        {
            base.OnInitialized(e);
            try { this.PropertyChanged += HomePage_OnAnyPropertyChanged; } catch { }
            try { UpdateReceivablePivotByCurrencyChart(); } catch { }
        }

        private void HomePage_OnAnyPropertyChanged(object sender, PropertyChangedEventArgs e)
        {
            try
            {
                if (e == null || string.IsNullOrEmpty(e.PropertyName))
                {
                    UpdateReceivablePivotByCurrencyChart();
                    return;
                }
                if (e.PropertyName == nameof(CurrencyDistributionSeries)
                    || e.PropertyName == nameof(ReceivablePivotByActionSeries)
                    || e.PropertyName == nameof(KpiRiskSeries))
                {
                    UpdateReceivablePivotByCurrencyChart();
                }
            }
            catch { }
        }
    }
}
