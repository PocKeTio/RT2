using System;

namespace RecoTool.Models
{
    /// <summary>
    /// Shared/global ToDo list item stored in the referential database (T_Ref_TodoList).
    /// </summary>
    public class TodoListItem
    {
        public int TDL_id { get; set; }
        public string TDL_Name { get; set; }
        public string TDL_FilterName { get; set; }
        public string TDL_ViewName { get; set; }
        public string TDL_Account { get; set; } // "Pivot" | "Receivable" | explicit Account_ID
        public int? TDL_Order { get; set; }
        public bool TDL_Active { get; set; }
        public string TDL_CountryId { get; set; } // optional scoping; null => all countries
    }
}
