using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows;
using RecoTool.Services.Rules;

namespace RecoTool.Windows
{
    public partial class RuleDebugWindow : Window
    {
        public RuleDebugWindow()
        {
            InitializeComponent();
        }

        public void SetDebugInfo(string lineInfo, string contextInfo, List<RuleDebugItem> rules)
        {
            LineInfoText.Text = lineInfo;
            ContextInfoText.Text = contextInfo;
            RulesDataGrid.ItemsSource = rules;
        }

        private void Close_Click(object sender, RoutedEventArgs e)
        {
            Close();
        }
    }

    /// <summary>
    /// Represents a rule with its evaluation result for debug display
    /// </summary>
    public class RuleDebugItem
    {
        public int DisplayOrder { get; set; }
        public int RuleId { get; set; }
        public string RuleName { get; set; }
        public bool IsEnabled { get; set; }
        public bool IsMatch { get; set; }
        public string MatchStatus { get; set; }
        public string OutputAction { get; set; }
        public string OutputKPI { get; set; }
        public List<ConditionDebugItem> Conditions { get; set; } = new List<ConditionDebugItem>();
    }

    /// <summary>
    /// Represents a single condition with its evaluation result
    /// </summary>
    public class ConditionDebugItem
    {
        public string Field { get; set; }
        public string Expected { get; set; }
        public string Actual { get; set; }
        public bool IsMet { get; set; }
        public string Status { get; set; } // "✓" or "✗"
    }
}
