using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using RecoTool.Services.DTOs;

namespace RecoTool.Services
{
    /// <summary>
    /// Encapsulates DWINGS local database access with shared, coalesced in-memory caches.
    /// Extracted from ReconciliationService to reduce responsibility and improve reuse.
    /// </summary>
    public class DwingsService
    {
        private readonly OfflineFirstService _offlineFirstService;

        public DwingsService(OfflineFirstService offlineFirstService)
        {
            _offlineFirstService = offlineFirstService;
        }

        private List<DwingsInvoiceDto> _invoicesCache;
        private List<DwingsGuaranteeDto> _guaranteesCache;
        private string _dwPath;
        private volatile bool _invalidated;

        // Shared per-DW path cache across all service instances
        private static readonly ConcurrentDictionary<string, Lazy<Task<(List<DwingsInvoiceDto> invoices, List<DwingsGuaranteeDto> guarantees)>>> _sharedDwCache
            = new ConcurrentDictionary<string, Lazy<Task<(List<DwingsInvoiceDto>, List<DwingsGuaranteeDto>)>>>();

        public static void InvalidateSharedCacheForPath(string dwPath)
        {
            if (string.IsNullOrWhiteSpace(dwPath)) return;
            try { _sharedDwCache.TryRemove(dwPath, out _); } catch { }
        }

        public async Task PrimeCachesAsync()
        {
            var dwPath = _offlineFirstService?.GetLocalDWDatabasePath();
            if (string.IsNullOrWhiteSpace(dwPath) || !File.Exists(dwPath))
            {
                _invoicesCache = new List<DwingsInvoiceDto>();
                _guaranteesCache = new List<DwingsGuaranteeDto>();
                _dwPath = null;
                _invalidated = false;
                return;
            }

            bool needReload = _invalidated
                              || _invoicesCache == null || _guaranteesCache == null
                              || !string.Equals(_dwPath, dwPath, StringComparison.OrdinalIgnoreCase);
            if (!needReload) return;

            if (_invalidated && !string.IsNullOrWhiteSpace(_dwPath))
            {
                _sharedDwCache.TryRemove(_dwPath, out _);
            }

            var loader = _sharedDwCache.GetOrAdd(dwPath,
                new Lazy<Task<(List<DwingsInvoiceDto>, List<DwingsGuaranteeDto>)>>(
                    () => LoadDwingsAsync(dwPath),
                    System.Threading.LazyThreadSafetyMode.ExecutionAndPublication));

            var tuple = await loader.Value.ConfigureAwait(false);
            _invoicesCache = tuple.Item1;
            _guaranteesCache = tuple.Item2;
            _dwPath = dwPath;
            _invalidated = false;
        }

        public void InvalidateCaches()
        {
            _invalidated = true;
            if (!string.IsNullOrWhiteSpace(_dwPath))
            {
                _sharedDwCache.TryRemove(_dwPath, out _);
            }
        }

        public async Task<IReadOnlyList<DwingsInvoiceDto>> GetInvoicesAsync()
        {
            await PrimeCachesAsync().ConfigureAwait(false);
            return _invoicesCache ?? new List<DwingsInvoiceDto>();
        }

        public async Task<IReadOnlyList<DwingsGuaranteeDto>> GetGuaranteesAsync()
        {
            await PrimeCachesAsync().ConfigureAwait(false);
            return _guaranteesCache ?? new List<DwingsGuaranteeDto>();
        }

        private static async Task<(List<DwingsInvoiceDto> invoices, List<DwingsGuaranteeDto> guarantees)> LoadDwingsAsync(string dwPath)
        {
            var dwCs = DbConn.AceConn(dwPath);
            var invoices = new List<DwingsInvoiceDto>();
            var guarantees = new List<DwingsGuaranteeDto>();

            using (var connection = new OleDbConnection(dwCs))
            {
                await connection.OpenAsync().ConfigureAwait(false);
                using (var cmd = new OleDbCommand(@"SELECT INVOICE_ID, T_INVOICE_STATUS, BILLING_AMOUNT, REQUESTED_AMOUNT, FINAL_AMOUNT, BILLING_CURRENCY, BGPMT, PAYMENT_METHOD, SENDER_REFERENCE, RECEIVER_REFERENCE, BUSINESS_CASE_REFERENCE, BUSINESS_CASE_ID, START_DATE, END_DATE, DEBTOR_PARTY_NAME, CREDITOR_PARTY_NAME FROM T_DW_Data", connection))
                using (var rd = await cmd.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    while (await rd.ReadAsync().ConfigureAwait(false))
                    {
                        invoices.Add(new DwingsInvoiceDto
                        {
                            INVOICE_ID = rd["INVOICE_ID"]?.ToString(),
                            T_INVOICE_STATUS = rd["T_INVOICE_STATUS"]?.ToString(),
                            BILLING_AMOUNT = TryToDecimal(rd["BILLING_AMOUNT"]),
                            REQUESTED_AMOUNT = TryToDecimal(rd["REQUESTED_AMOUNT"]),
                            FINAL_AMOUNT = TryToDecimal(rd["FINAL_AMOUNT"]),
                            BILLING_CURRENCY = rd["BILLING_CURRENCY"]?.ToString(),
                            BGPMT = rd["BGPMT"]?.ToString(),
                            PAYMENT_METHOD = rd["PAYMENT_METHOD"]?.ToString(),
                            SENDER_REFERENCE = rd["SENDER_REFERENCE"]?.ToString(),
                            RECEIVER_REFERENCE = rd["RECEIVER_REFERENCE"]?.ToString(),
                            BUSINESS_CASE_REFERENCE = rd["BUSINESS_CASE_REFERENCE"]?.ToString(),
                            BUSINESS_CASE_ID = rd["BUSINESS_CASE_ID"]?.ToString(),
                            START_DATE = TryToDate(rd["START_DATE"]),
                            END_DATE = TryToDate(rd["END_DATE"]),
                            DEBTOR_PARTY_NAME = rd["DEBTOR_PARTY_NAME"]?.ToString(),
                            CREDITOR_PARTY_NAME = rd["CREDITOR_PARTY_NAME"]?.ToString(),
                        });
                    }
                }

                using (var cmdG = new OleDbCommand(@"SELECT 
                            GUARANTEE_ID,
                            GUARANTEE_STATUS,
                            OUTSTANDING_AMOUNT,
                            CURRENCYNAME,
                            NAME1,
                            NAME2,
                            GUARANTEE_TYPE,
                            NATURE,
                            EVENT_STATUS,
                            EVENT_EFFECTIVEDATE,
                            ISSUEDATE,
                            OFFICIALREF,
                            UNDERTAKINGEVENT,
                            PROCESS,
                            EXPIRYDATETYPE,
                            EXPIRYDATE,
                            PARTY_ID,
                            PARTY_REF,
                            SECONDARY_OBLIGOR,
                            SECONDARY_OBLIGOR_NATURE,
                            ROLE,
                            COUNTRY,
                            CENTRAL_PARTY_CODE,
                            GROUPE,
                            PREMIUM,
                            BRANCH_CODE,
                            BRANCH_NAME,
                            OUTSTANDING_AMOUNT_IN_BOOKING_CURRENCY,
                            CANCELLATIONDATE,
                            CONTROLER,
                            AUTOMATICBOOKOFF,
                            NATUREOFDEAL
                        FROM T_DW_Guarantee", connection))
                using (var rdG = await cmdG.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    while (await rdG.ReadAsync().ConfigureAwait(false))
                    {
                        guarantees.Add(new DwingsGuaranteeDto
                        {
                            GUARANTEE_ID = rdG["GUARANTEE_ID"]?.ToString(),
                            GUARANTEE_STATUS = rdG["GUARANTEE_STATUS"]?.ToString(),
                            OUTSTANDING_AMOUNT = TryToDecimal(rdG["OUTSTANDING_AMOUNT"]),
                            CURRENCYNAME = rdG["CURRENCYNAME"]?.ToString(),
                            NAME1 = rdG["NAME1"]?.ToString(),
                            NAME2 = rdG["NAME2"]?.ToString(),
                            GUARANTEE_TYPE = rdG["GUARANTEE_TYPE"]?.ToString(),
                            NATURE = rdG["NATURE"]?.ToString(),
                            EVENT_STATUS = rdG["EVENT_STATUS"]?.ToString(),
                            EVENT_EFFECTIVEDATE = TryToDate(rdG["EVENT_EFFECTIVEDATE"]),
                            ISSUEDATE = TryToDate(rdG["ISSUEDATE"]),
                            OFFICIALREF = rdG["OFFICIALREF"]?.ToString(),
                            UNDERTAKINGEVENT = rdG["UNDERTAKINGEVENT"]?.ToString(),
                            PROCESS = rdG["PROCESS"]?.ToString(),
                            EXPIRYDATETYPE = rdG["EXPIRYDATETYPE"]?.ToString(),
                            EXPIRYDATE = TryToDate(rdG["EXPIRYDATE"]),
                            PARTY_ID = rdG["PARTY_ID"]?.ToString(),
                            PARTY_REF = rdG["PARTY_REF"]?.ToString(),
                            SECONDARY_OBLIGOR = rdG["SECONDARY_OBLIGOR"]?.ToString(),
                            SECONDARY_OBLIGOR_NATURE = rdG["SECONDARY_OBLIGOR_NATURE"]?.ToString(),
                            ROLE = rdG["ROLE"]?.ToString(),
                            COUNTRY = rdG["COUNTRY"]?.ToString(),
                            CENTRAL_PARTY_CODE = rdG["CENTRAL_PARTY_CODE"]?.ToString(),
                            GROUPE = rdG["GROUPE"]?.ToString(),
                            PREMIUM = rdG["PREMIUM"]?.ToString(),
                            BRANCH_CODE = rdG["BRANCH_CODE"]?.ToString(),
                            BRANCH_NAME = rdG["BRANCH_NAME"]?.ToString(),
                            OUTSTANDING_AMOUNT_IN_BOOKING_CURRENCY = rdG["OUTSTANDING_AMOUNT_IN_BOOKING_CURRENCY"]?.ToString(),
                            CANCELLATIONDATE = TryToDate(rdG["CANCELLATIONDATE"]),
                            CONTROLER = rdG["CONTROLER"]?.ToString(),
                            AUTOMATICBOOKOFF = rdG["AUTOMATICBOOKOFF"]?.ToString(),
                            NATUREOFDEAL = rdG["NATUREOFDEAL"]?.ToString(),
                        });
                    }
                }
            }

            return (invoices, guarantees);
        }

        private static decimal? TryToDecimal(object o)
        {
            if (o == null || o == DBNull.Value) return null;
            try { return Convert.ToDecimal(o, CultureInfo.InvariantCulture); } catch { return null; }
        }

        private static DateTime? TryToDate(object o)
        {
            if (o == null || o == DBNull.Value) return null;
            try { return Convert.ToDateTime(o, CultureInfo.InvariantCulture); } catch { return null; }
        }
    }
}
