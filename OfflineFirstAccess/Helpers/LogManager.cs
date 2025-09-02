using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Globalization;

namespace OfflineFirstAccess.Helpers
{
    /// <summary>
    /// Gestionnaire de logs centralisé avec support asynchrone
    /// </summary>
    public static class LogManager
    {
        private static string _logDirectory;
        private static int _retentionDays;
        private static readonly ConcurrentQueue<LogEntry> _logQueue = new ConcurrentQueue<LogEntry>();
        private static readonly SemaphoreSlim _logSemaphore = new SemaphoreSlim(1, 1);
        private static Timer _flushTimer;
        private static bool _isInitialized = false;

        /// <summary>
        /// Initialise le gestionnaire de logs
        /// </summary>
        public static void Initialize(string logDirectory, int retentionDays = 30)
        {
            if (_isInitialized)
                return;

            _logDirectory = logDirectory;
            _retentionDays = retentionDays;

            // Créer le répertoire s'il n'existe pas
            if (!Directory.Exists(_logDirectory))
            {
                Directory.CreateDirectory(_logDirectory);
            }

            // Démarrer le timer pour vider la queue régulièrement
            _flushTimer = new Timer(async _ => await FlushLogsAsync(), null, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5));

            _isInitialized = true;
        }

        /// <summary>
        /// Enregistre un message d'information
        /// </summary>
        public static void Info(string message)
        {
            Log(LogLevel.Info, message, null);
        }

        /// <summary>
        /// Enregistre un message de debug
        /// </summary>
        public static void Debug(string message)
        {
            Log(LogLevel.Debug, message, null);
        }

        /// <summary>
        /// Enregistre un message d'avertissement
        /// </summary>
        public static void Warning(string message, Exception ex = null)
        {
            Log(LogLevel.Warning, message, ex);
        }

        /// <summary>
        /// Enregistre un message d'erreur
        /// </summary>
        public static void Error(string message, Exception ex)
        {
            Log(LogLevel.Error, message, ex);
        }

        /// <summary>
        /// Enregistre un message dans la queue
        /// </summary>
        private static void Log(LogLevel level, string message, Exception exception)
        {
            if (!_isInitialized)
            {
                // Si non initialisé, écrire dans la console
                Console.WriteLine($"[{level}] {message}");
                if (exception != null)
                    Console.WriteLine(exception.ToString());
                return;
            }

            var entry = new LogEntry
            {
                Timestamp = DateTime.UtcNow,
                Level = level,
                Message = message,
                Exception = exception,
                ThreadId = Thread.CurrentThread.ManagedThreadId
            };

            _logQueue.Enqueue(entry);

            // Si c'est une erreur, forcer l'écriture immédiate
            if (level == LogLevel.Error)
            {
                Task.Run(async () => await FlushLogsAsync());
            }
        }

        /// <summary>
        /// Vide la queue de logs dans le fichier
        /// </summary>
        private static async Task FlushLogsAsync()
        {
            if (_logQueue.IsEmpty)
                return;

            await _logSemaphore.WaitAsync();
            try
            {
                string logFile = Path.Combine(_logDirectory, $"OfflineFirstAccess_{DateTime.UtcNow.ToString("yyyyMMdd", CultureInfo.InvariantCulture)}.log");

                using (var writer = new StreamWriter(logFile, append: true))
                {
                    while (_logQueue.TryDequeue(out var entry))
                    {
                        string logLine = FormatLogEntry(entry);
                        await writer.WriteLineAsync(logLine);

                        // Écrire aussi dans la console en mode debug
#if DEBUG
                        Console.WriteLine(logLine);
#endif
                    }
                }

                // Nettoyer les vieux logs (garder seulement 30 jours)
                CleanupOldLogs();
            }
            catch (Exception ex)
            {
                // En cas d'erreur, écrire dans la console
                Console.WriteLine($"Erreur lors de l'écriture des logs : {ex.Message}");
            }
            finally
            {
                _logSemaphore.Release();
            }
        }

        /// <summary>
        /// Formate une entrée de log
        /// </summary>
        private static string FormatLogEntry(LogEntry entry)
        {
            string baseMessage = string.Format(CultureInfo.InvariantCulture, "{0:yyyy-MM-dd HH:mm:ss.fff} [{1,-7}] [Thread:{2:D3}] {3}", entry.Timestamp, entry.Level, entry.ThreadId, entry.Message);

            if (entry.Exception != null)
            {
                baseMessage += Environment.NewLine + "Exception: " + entry.Exception.ToString();
            }

            return baseMessage;
        }

        /// <summary>
        /// Nettoie les anciens fichiers de log
        /// </summary>
        private static void CleanupOldLogs()
        {
            try
            {
                var cutoffDate = DateTime.UtcNow.AddDays(-30);
                var files = Directory.GetFiles(_logDirectory, "OfflineFirstAccess_*.log");

                foreach (var file in files)
                {
                    var fileInfo = new FileInfo(file);
                    if (fileInfo.CreationTime < cutoffDate)
                    {
                        File.Delete(file);
                    }
                }
            }
            catch
            {
                // Ignorer les erreurs de nettoyage
            }
        }

        /// <summary>
        /// Arrête le gestionnaire de logs
        /// </summary>
        public static async Task ShutdownAsync()
        {
            if (!_isInitialized)
                return;

            _flushTimer?.Dispose();
            await FlushLogsAsync();
            _isInitialized = false;
        }

        public static void Warn(string v)
        {
            Log(LogLevel.Warning, v, null);
        }

        /// <summary>
        /// Niveau de log
        /// </summary>
        private enum LogLevel
        {
            Debug,
            Info,
            Warning,
            Error
        }

        /// <summary>
        /// Entrée de log
        /// </summary>
        private class LogEntry
        {
            public DateTime Timestamp { get; set; }
            public LogLevel Level { get; set; }
            public string Message { get; set; }
            public Exception Exception { get; set; }
            public int ThreadId { get; set; }
        }
    }
}