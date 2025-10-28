using System;
using System.Threading.Tasks;

namespace RecoTool.Services.External
{
    // Mock implementation of the real Free API client
    public sealed class MockFreeApiClient : IFreeApiClient
    {
        private static readonly Random _rng = new Random();
        private bool _isAuth;

        public Task<bool> AuthenticateAsync()
        {
            // Simulate a quick auth success
            _isAuth = true;
            return Task.FromResult(true);
        }

        public bool IsAuthenticated => _isAuth;

        public Task<string> SearchAsync(DateTime day, string reference, string cntServiceCode)
        {
            var dayStr = day.ToString("yyyy-MM-dd");
            var svc = string.IsNullOrWhiteSpace(cntServiceCode) ? "N/A" : cntServiceCode.Trim();
            var refStr = reference ?? string.Empty;

            string maybeBgpmt = _rng.Next(0, 3) == 0 ? $" BGPMT:{_rng.Next(100000, 999999)}" : string.Empty;
            string maybeBgi = _rng.Next(0, 3) == 0 ? $" BGI:INV{_rng.Next(100000, 999999)}" : string.Empty;
            string maybeGid = _rng.Next(0, 3) == 0 ? $" GUARANTEE_ID:G{_rng.Next(100000, 999999)}" : string.Empty;

            var payload = $"FREE_API_RESULT | day={dayStr} | service={svc} | ref={refStr};{maybeBgpmt}{maybeBgi}{maybeGid}";
            return Task.FromResult(payload);
        }
    }
}
