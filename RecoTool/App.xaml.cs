using System;
using System.Windows;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using RecoTool.Services;
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

            // Fenêtres principales
            services.AddTransient<MainWindow>();
            services.AddTransient<ImportAmbreWindow>();
            services.AddTransient<ReconciliationPage>();
            services.AddTransient<ReconciliationView>();
            services.AddTransient<ReportsWindow>();

            ServiceProvider = services.BuildServiceProvider();

            // Ensure OfflineFirstService is fully initialized BEFORE constructing any services/windows
            try
            {
                var offline = ServiceProvider.GetRequiredService<OfflineFirstService>();
                // Complete referential load
                offline.LoadReferentialsAsync().GetAwaiter().GetResult();

                // Ensure a current country is set (prefer LastCountryUsed from T_Param)
                var currentCountry = offline.CurrentCountryId;
                if (string.IsNullOrWhiteSpace(currentCountry))
                {
                    var last = offline.GetParameter("LastCountryUsed");
                    if (!string.IsNullOrWhiteSpace(last))
                    {
                        offline.SetCurrentCountryAsync(last).GetAwaiter().GetResult();
                    }
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
