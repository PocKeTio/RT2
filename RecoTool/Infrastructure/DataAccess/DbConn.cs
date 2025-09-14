using System;

namespace RecoTool.Services
{
    // Centralized ACE OLE DB connection string helpers
    public static class DbConn
    {
        public static string AceConn(string path)
            => $"Provider=Microsoft.ACE.OLEDB.16.0;Data Source={path};";

        public static string AceConnNetwork(string path)
            => $"Provider=Microsoft.ACE.OLEDB.16.0;Data Source={path};Jet OLEDB:Database Locking Mode=1;Mode=Share Deny None;";
    }
}
