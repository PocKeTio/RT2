using System;
using System.Windows;

namespace RecoTool.Windows
{
    public partial class PreviewTextDialog : Window
    {
        public PreviewTextDialog()
        {
            InitializeComponent();
        }

        public void SetTitle(string title)
        {
            try { TitleBlock.Text = title ?? string.Empty; } catch { }
        }

        public void SetContent(string text)
        {
            try
            {
                ContentTextBox.Text = text ?? string.Empty;
                ContentTextBox.CaretIndex = 0;
                ContentTextBox.ScrollToHome();
            }
            catch { }
        }

        private void Close_Click(object sender, RoutedEventArgs e)
        {
            try { this.Close(); } catch { }
        }
    }
}
