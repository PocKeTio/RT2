using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using OfflineFirstAccess.Helpers;

namespace RecoTool.Services
{
    /// <summary>
    /// Simple singleton background task queue to serialize fire-and-forget operations.
    /// </summary>
    public sealed class BackgroundTaskQueue : IDisposable
    {
        private static readonly Lazy<BackgroundTaskQueue> _instance =
            new Lazy<BackgroundTaskQueue>(() => new BackgroundTaskQueue());
        public static BackgroundTaskQueue Instance => _instance.Value;

        private readonly ConcurrentQueue<Func<Task>> _queue = new ConcurrentQueue<Func<Task>>();
        private readonly SemaphoreSlim _signal = new SemaphoreSlim(0);
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private readonly Task _worker;

        private BackgroundTaskQueue()
        {
            _worker = Task.Run(ProcessQueueAsync);
        }

        public void Enqueue(Func<Task> workItem)
        {
            if (workItem == null) throw new ArgumentNullException(nameof(workItem));
            _queue.Enqueue(workItem);
            _signal.Release();
        }

        private async Task ProcessQueueAsync()
        {
            while (!_cts.IsCancellationRequested)
            {
                try
                {
                    await _signal.WaitAsync(_cts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                if (_queue.TryDequeue(out var work))
                {
                    try
                    {
                        await work().ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        try { LogManager.Error("[BG-QUEUE] Task failed", ex); } catch { }
                    }
                }
            }
        }

        public void Dispose()
        {
            _cts.Cancel();
            try { _signal.Release(); } catch { }
            try { _worker.Wait(TimeSpan.FromSeconds(1)); } catch { }
            _signal.Dispose();
            _cts.Dispose();
        }
    }
}
