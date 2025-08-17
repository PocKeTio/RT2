using System;
using System.Threading.Tasks;
using OfflineFirstAccess.Helpers;

namespace OfflineFirstAccess.Helpers
{
    /// <summary>
    /// Helper pour gérer les retry avec backoff exponentiel
    /// </summary>
    public static class RetryHelper
    {
        private static readonly Random _random = new Random();

        /// <summary>
        /// Exécute une opération avec retry automatique en cas d'échec
        /// </summary>
        public static async Task<T> ExecuteWithRetryAsync<T>(
            Func<Task<T>> operation,
            int maxRetries = 3,
            int baseDelayMs = 1000,
            Func<Exception, bool> shouldRetry = null)
        {
            int retryCount = 0;
            Exception lastException = null;

            while (retryCount <= maxRetries)
            {
                try
                {
                    return await operation();
                }
                catch (Exception ex)
                {
                    lastException = ex;

                    // Vérifier si on doit réessayer
                    if (shouldRetry != null && !shouldRetry(ex))
                    {
                        throw;
                    }

                    // Si on a atteint le nombre max de tentatives, abandonner
                    if (retryCount >= maxRetries)
                    {
                        LogManager.Error($"Échec après {maxRetries} tentatives", ex);
                        throw;
                    }

                    // Calculer le délai avec backoff exponentiel
                    var backoff = TimeSpan.FromMilliseconds(baseDelayMs * (int)Math.Pow(2, retryCount));
                    var jitter = TimeSpan.FromMilliseconds(_random.Next(0, 100)); // Jitter to prevent thundering herd
                    var delay = backoff + jitter;

                    LogManager.Warning($"Tentative {retryCount + 1}/{maxRetries} échouée, nouvelle tentative dans {delay.TotalMilliseconds}ms", ex);

                    await Task.Delay(delay);
                    retryCount++;
                }
            }

            throw lastException ?? new InvalidOperationException("Échec de l'opération");
        }

        /// <summary>
        /// Version pour les opérations sans valeur de retour
        /// </summary>
        public static async Task ExecuteWithRetryAsync(
            Func<Task> operation,
            int maxRetries = 3,
            int baseDelayMs = 1000,
            Func<Exception, bool> shouldRetry = null)
        {
            await ExecuteWithRetryAsync(async () =>
            {
                await operation();
                return true;
            }, maxRetries, baseDelayMs, shouldRetry);
        }

        /// <summary>
        /// Détermine si une exception est récupérable
        /// </summary>
        public static bool IsRetriableException(Exception ex)
        {
            // Timeout
            if (ex is TimeoutException)
                return true;

            // Erreurs réseau
            if (ex.Message.Contains("network") ||
                ex.Message.Contains("connection") ||
                ex.Message.Contains("timeout"))
                return true;

            // Erreurs de base de données spécifiques
            if (ex is System.Data.OleDb.OleDbException oleDbEx)
            {
                // Codes d'erreur récupérables
                var retriableCodes = new[] {
                    -2147467259, // Timeout
                    -2147217843, // Deadlock
                    -2147217900  // Connection broken
                };

                foreach (var code in retriableCodes)
                {
                    if (oleDbEx.ErrorCode == code)
                        return true;
                }
            }

            return false;
        }
    }
}