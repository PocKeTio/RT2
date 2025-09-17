using System;
using System.Windows;

namespace RecoTool.Windows
{
    public partial class CommentsDialog : Window
    {
        public CommentsDialog()
        {
            InitializeComponent();
        }

        public void SetConversationText(string comments)
        {
            try
            {
                ConversationTextBox.Text = comments ?? string.Empty;
                ConversationTextBox.CaretIndex = ConversationTextBox.Text.Length;
                ConversationTextBox.ScrollToEnd();
            }
            catch { }
        }

        public string GetNewCommentText()
        {
            try { return NewCommentTextBox.Text; } catch { return null; }
        }

        private void AddButton_Click(object sender, RoutedEventArgs e)
        {
            this.DialogResult = true;
            this.Close();
        }
    }
}
