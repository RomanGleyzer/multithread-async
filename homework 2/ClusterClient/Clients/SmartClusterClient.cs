using System;
using System.Threading;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients;

public class SmartClusterClient(string[] replicaAddresses) : ClusterClientBase(replicaAddresses)
{
    public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
    {
        if (ReplicaAddresses == null || ReplicaAddresses.Length == 0)
            throw new InvalidOperationException("Реплика адресов не указана");

        var perReplicaTimeout = TimeSpan.FromTicks(timeout.Ticks / ReplicaAddresses.Length);
        var firstSuccess = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

        var remaining = ReplicaAddresses.Length;
        Exception lastError = null;

        foreach (var uri in ReplicaAddresses)
        {
            var webRequest = CreateRequest(uri + "?query=" + query);

            Log.InfoFormat($"Обработка {webRequest.RequestUri}");

            var task = ProcessRequestAsync(webRequest);

            _ = task.ContinueWith(t =>
            {
                if (t.Status == TaskStatus.RanToCompletion)
                {
                    firstSuccess.TrySetResult(t.Result);
                    return;
                }

                var ex = t.Exception?.GetBaseException() ?? new TaskCanceledException(t);
                Interlocked.Exchange(ref lastError, ex);

                if (Interlocked.Decrement(ref remaining) == 0)
                    firstSuccess.TrySetException(Volatile.Read(ref lastError) ??
                                                 new Exception(
                                                     "Не удалось получить успешный ответ ни от одной реплики"));
            }, TaskContinuationOptions.ExecuteSynchronously);

            var completed = await Task.WhenAny(firstSuccess.Task, Task.Delay(perReplicaTimeout));
            if (completed == firstSuccess.Task)
                return await firstSuccess.Task;
        }

        if (firstSuccess.Task.IsCompleted)
            return await firstSuccess.Task;

        throw new TimeoutException();
    }

    protected override ILog Log => LogManager.GetLogger(typeof(SmartClusterClient));
}