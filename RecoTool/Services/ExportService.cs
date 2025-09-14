using System;
using System.Data;
using System.Data.Common;
using System.Data.OleDb;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Threading;
using System.Collections.Generic;

namespace RecoTool.Services
{
    public class ExportContext
    {
        public string CountryId { get; set; }
        public string AccountId { get; set; }
        public DateTime? FromDate { get; set; }
        public DateTime? ToDate { get; set; }
        public string UserId { get; set; }
        public string OutputDirectory { get; set; }
    }

    public class ExportService
    {
        private readonly ReconciliationService _reconciliationService;
        private readonly ReferentialService _referentialService;

        public ExportService(ReconciliationService reconciliationService, ReferentialService referentialService)
        {
            _reconciliationService = reconciliationService ?? throw new ArgumentNullException(nameof(reconciliationService));
            _referentialService = referentialService ?? throw new ArgumentNullException(nameof(referentialService));
        }

        public async Task<string> ExportFromParamAsync(string paramKey, ExportContext ctx, CancellationToken cancellationToken = default)
        {
            if (ctx == null) throw new ArgumentNullException(nameof(ctx));

            cancellationToken.ThrowIfCancellationRequested();
            var sql = await _referentialService.GetParamValueAsync(paramKey, cancellationToken).ConfigureAwait(false);
            if (string.IsNullOrWhiteSpace(sql))
                throw new InvalidOperationException($"Paramètre {paramKey} introuvable ou vide dans T_param");

            var wantedParams = DetectSqlParams(sql);
            var sqlParams = BuildSqlParameters(wantedParams, ctx.CountryId, ctx.AccountId, ctx.FromDate, ctx.ToDate, ctx.UserId);

            cancellationToken.ThrowIfCancellationRequested();
            var table = await ExecuteExportAsync(sql, sqlParams, cancellationToken).ConfigureAwait(false);

            var outputDir = ctx.OutputDirectory;
            if (string.IsNullOrWhiteSpace(outputDir))
            {
                var docs = Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments);
                outputDir = Path.Combine(docs, "RecoExports");
            }
            Directory.CreateDirectory(outputDir);

            var fileName = $"{paramKey}_{Sanitize(ctx.CountryId)}_{Sanitize(ctx.AccountId)}_{DateTime.UtcNow:yyyyMMddHHmmss}.csv";
            var tempPath = Path.Combine(outputDir, fileName + ".tmp");
            var finalPath = Path.Combine(outputDir, fileName);

            await ExportToCsvAsync(table, tempPath, cancellationToken).ConfigureAwait(false);
            if (File.Exists(finalPath)) File.Delete(finalPath);
            File.Move(tempPath, finalPath);
            return finalPath;
        }

        private static string Sanitize(string s)
        {
            if (string.IsNullOrEmpty(s)) return "NA";
            var invalid = Path.GetInvalidFileNameChars();
            return new string(s.Where(c => !invalid.Contains(c)).ToArray());
        }

        private static string[] DetectSqlParams(string sql)
        {
            // Match @ParamName tokens
            var matches = Regex.Matches(sql, @"@([A-Za-z_][A-Za-z0-9_]*)");
            return matches.Cast<Match>().Select(m => m.Groups[1].Value).Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
        }

        private static IEnumerable<DbParameter> BuildSqlParameters(IEnumerable<string> paramNames, string countryId, string accountId, DateTime? fromDate, DateTime? toDate, string userId)
        {
            var list = new List<DbParameter>();
            if (paramNames == null) return list;

            foreach (var name in paramNames)
            {
                OleDbParameter p;
                switch (name.ToLowerInvariant())
                {
                    case "countryid":
                        p = new OleDbParameter("@" + name, OleDbType.VarWChar) { Value = (object)(countryId ?? string.Empty) ?? DBNull.Value }; break;
                    case "accountid":
                        p = new OleDbParameter("@" + name, OleDbType.VarWChar) { Value = (object)(accountId ?? string.Empty) ?? DBNull.Value }; break;
                    case "fromdate":
                        p = new OleDbParameter("@" + name, OleDbType.Date) { Value = fromDate.HasValue ? (object)fromDate.Value.Date : DBNull.Value }; break;
                    case "todate":
                        p = new OleDbParameter("@" + name, OleDbType.Date) { Value = toDate.HasValue ? (object)toDate.Value.Date : DBNull.Value }; break;
                    case "userid":
                        // fallback to reconciliation service user if not specified
                        p = new OleDbParameter("@" + name, OleDbType.VarWChar) { Value = (object)(userId ?? string.Empty) ?? DBNull.Value }; break;
                    default:
                        p = new OleDbParameter("@" + name, OleDbType.VarWChar) { Value = DBNull.Value }; break;
                }
                list.Add(p);
            }
            return list;
        }

        private async Task<DataTable> ExecuteExportAsync(string sql, IEnumerable<DbParameter> parameters, CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(sql)) throw new ArgumentNullException(nameof(sql));
            // Use main data connection string from reconciliation service
            var connectionString = _reconciliationService.MainConnectionString;
            using (var connection = new OleDbConnection(connectionString))
            {
                cancellationToken.ThrowIfCancellationRequested();
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                using (var cmd = new OleDbCommand(sql, connection))
                {
                    if (parameters != null)
                    {
                        foreach (var p in parameters)
                        {
                            if (p is OleDbParameter op)
                            {
                                var clone = new OleDbParameter(op.ParameterName, op.OleDbType) { Value = op.Value ?? DBNull.Value };
                                cmd.Parameters.Add(clone);
                            }
                            else
                            {
                                var clone = new OleDbParameter
                                {
                                    ParameterName = p.ParameterName,
                                    Value = p.Value ?? DBNull.Value
                                };
                                cmd.Parameters.Add(clone);
                            }
                        }
                    }

                    using (var adapter = new OleDbDataAdapter(cmd))
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        var table = new DataTable();
                        adapter.Fill(table);
                        return table;
                    }
                }
            }
        }

        private static async Task ExportToCsvAsync(DataTable table, string path, CancellationToken cancellationToken = default)
        {
            using var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None);
            using var sw = new StreamWriter(fs, new UTF8Encoding(encoderShouldEmitUTF8Identifier: true));

            // Header
            for (int i = 0; i < table.Columns.Count; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (i > 0) await sw.WriteAsync(",");
                await sw.WriteAsync(EscapeCsv(table.Columns[i].ColumnName));
            }
            await sw.WriteLineAsync();

            // Rows
            foreach (DataRow row in table.Rows)
            {
                cancellationToken.ThrowIfCancellationRequested();
                for (int i = 0; i < table.Columns.Count; i++)
                {
                    if (i > 0) await sw.WriteAsync(",");
                    var val = row[i];
                    await sw.WriteAsync(EscapeCsv(FormatValue(val)));
                }
                await sw.WriteLineAsync();
            }
        }

        private static string FormatValue(object? val)
        {
            if (val == null || val is DBNull) return string.Empty;
            switch (val)
            {
                case DateTime dt:
                    return dt.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
                case decimal dec:
                    return dec.ToString(CultureInfo.InvariantCulture);
                case double dbl:
                    return dbl.ToString(CultureInfo.InvariantCulture);
                case float fl:
                    return fl.ToString(CultureInfo.InvariantCulture);
                default:
                    return Convert.ToString(val, CultureInfo.InvariantCulture) ?? string.Empty;
            }
        }

        private static string EscapeCsv(string input)
        {
            if (input == null) return string.Empty;
            var needsQuotes = input.Contains(',') || input.Contains('"') || input.Contains('\n') || input.Contains('\r');
            var s = input.Replace("\"", "\"\"");
            return needsQuotes ? $"\"{s}\"" : s;
        }
    }
}

