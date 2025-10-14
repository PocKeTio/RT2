using System;
using System.Windows;
using Microsoft.Extensions.DependencyInjection;
using RecoTool.Services;
using RecoTool.Services.Policies;
using RecoTool.Domain.Repositories;
using RecoTool.Infrastructure.Repositories;
using RecoTool.Windows;

namespace RecoTool
{
    public partial class App : Application
    {
        public static IServiceProvider ServiceProvider { get; private set; }

        [STAThread]
        public static void Main()
        {
            var app = new App();
            app.InitializeComponent();
            app.Run();
        }

        protected override void OnStartup(StartupEventArgs e)
        {
            base.OnStartup(e);

            var services = new ServiceCollection();

            // Notre service offline-first (singleton pour tout l'app)
            services.AddSingleton<OfflineFirstService>();

            // Sync policy: centralize when background pushes and syncs are allowed
            services.AddSingleton<ISyncPolicy, SyncPolicy>();

            // Services métiers
            services.AddTransient<AmbreImportService>();
            services.AddTransient<ReconciliationService>(sp =>
            {
                var offline = sp.GetRequiredService<OfflineFirstService>();
                // Récupère la chaîne de connexion locale courante (nécessite que le pays courant soit déjà défini)
                var connStr = offline.GetCurrentLocalConnectionString();
                var currentUser = Environment.UserName ?? "Unknown";
                var countries = offline.Countries;
                return new ReconciliationService(connStr, currentUser, countries, offline);
            });
            // Lookup/Referential/Options services
            services.AddTransient<LookupService>(sp => new LookupService(sp.GetRequiredService<OfflineFirstService>()));
            services.AddTransient<ReferentialService>(sp =>
            {
                var offline = sp.GetRequiredService<OfflineFirstService>();
                var recoSvc = sp.GetRequiredService<ReconciliationService>();
                return new ReferentialService(offline, recoSvc?.CurrentUser);
            });
            services.AddTransient<OptionsService>(sp => new OptionsService(
                sp.GetRequiredService<ReconciliationService>(),
                sp.GetRequiredService<ReferentialService>(),
                sp.GetRequiredService<LookupService>()));

            // Repositories (transition: wraps existing services)
            services.AddTransient<IReconciliationRepository, ReconciliationRepository>();

            // Referential cache service (singleton for app-wide caching)
            services.AddSingleton<ReferentialCacheService>();

            // Fenêtres principales
            services.AddTransient<MainWindow>();
            services.AddTransient<ImportAmbreWindow>();
            services.AddTransient<ReconciliationPage>(sp =>
            {
                var recoSvc = sp.GetRequiredService<ReconciliationService>();
                var offline = sp.GetRequiredService<OfflineFirstService>();
                var repo = sp.GetRequiredService<IReconciliationRepository>();
                return new ReconciliationPage(recoSvc, offline, repo);
            });
            services.AddTransient<ReconciliationView>();
            services.AddTransient<ReportsWindow>();

            ServiceProvider = services.BuildServiceProvider();

            // Ensure OfflineFirstService is fully initialized BEFORE constructing any services/windows
            try
            {
                var offline = ServiceProvider.GetRequiredService<OfflineFirstService>();
                var policy = ServiceProvider.GetRequiredService<ISyncPolicy>();
                // Propagate policy to service (background pushes disabled by default)
                try { offline.AllowBackgroundPushes = policy.AllowBackgroundPushes; } catch { }
                // Complete referential load
                offline.LoadReferentialsAsync().GetAwaiter().GetResult();

                var currentCountry = offline.CurrentCountryId;

                // Ensure Control DB schema exists early (idempotent)
                try
                {
                    offline.EnsureControlSchemaAsync().GetAwaiter().GetResult();
                }
                catch (Exception ex2)
                {
                    System.Diagnostics.Debug.WriteLine($"[Startup] EnsureControlSchema warning: {ex2.Message}");
                }
            }
            catch (Exception ex)
            {
                // Do not block app start, but log for diagnostics
                System.Diagnostics.Debug.WriteLine($"[Startup] OfflineFirst initialization warning: {ex.Message}");
            }

            var main = ServiceProvider.GetRequiredService<MainWindow>();
            main.Show();
        }

        protected override void OnExit(ExitEventArgs e)
        {
            // Disposez les singletons qui implémentent IDisposable
            if (ServiceProvider is IDisposable disp)
                disp.Dispose();

            base.OnExit(e);
        }
    }
}
