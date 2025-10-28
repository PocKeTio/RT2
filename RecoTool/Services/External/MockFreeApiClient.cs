using System;
using System.Threading.Tasks;

namespace RecoTool.Services.External
{
    // Initial mock: synthesizes a payload string that may contain recognizable tokens
    // In production this will call the real Free API endpoint.
    public sealed class MockFreeApiClient : IFreeApiClient
    {
        private static readonly Random _rng = new Random();

        public Task<string> SearchAsync(DateTime day, string reference, string cntServiceCode)
        {
            // Generate a deterministic-ish mock payload including the inputs so regex can extract tokens
            var dayStr = day.ToString("yyyy-MM-dd");
            var svc = string.IsNullOrWhiteSpace(cntServiceCode) ? "N/A" : cntServiceCode.Trim();
            var refStr = reference ?? string.Empty;

            // Sometimes include recognizable patterns for tests
            string maybeBgpmt = _rng.Next(0, 3) == 0 ? $" BGPMT:{_rng.Next(100000, 999999)}" : string.Empty;
            string maybeBgi = _rng.Next(0, 3) == 0 ? $" BGI:INV{_rng.Next(100000, 999999)}" : string.Empty;
            string maybeGid = _rng.Next(0, 3) == 0 ? $" GUARANTEE_ID:G{_rng.Next(100000, 999999)}" : string.Empty;

            var payload = $"FREE_API_RESULT | day={dayStr} | service={svc} | ref={refStr};{maybeBgpmt}{maybeBgi}{maybeGid}";
            return Task.FromResult(payload);
        }
    }
}
