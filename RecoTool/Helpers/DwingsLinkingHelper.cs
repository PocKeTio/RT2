using System;
using System.Text.RegularExpressions;

namespace RecoTool.Helpers
{
    public static class DwingsLinkingHelper
    {
        // BGPMT token: e.g., BGPMTxxxxxxxx (8-20 alnum after BGPMT)
        public static string ExtractBgpmtToken(string s)
        {
            if (string.IsNullOrWhiteSpace(s)) return null;
            var m = Regex.Match(s, @"\bBGPMT[A-Za-z0-9]{8,20}\b");
            return m.Success ? m.Value : null;
        }

        // BGI invoice id: BGI + 13 digits (year+month+7 digits)
        public static string ExtractBgiToken(string s)
        {
            if (string.IsNullOrWhiteSpace(s)) return null;
            var m = Regex.Match(s, @"\bBGI\d{13}\b");
            return m.Success ? m.Value : null;
        }

        // Guarantee ID: G####AA######### (4 digits, 2 letters, 9 digits)
        public static string ExtractGuaranteeId(string s)
        {
            if (string.IsNullOrWhiteSpace(s)) return null;
            var m = Regex.Match(s, @"\bG\d{4}[A-Za-z]{2}\d{9}\b");
            return m.Success ? m.Value : null;
        }
    }
}
