using System;
using System.Diagnostics;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients;

public class RoundRobinClusterClient(string[] replicaAddresses) : ClusterClientBase(replicaAddresses)
{
    public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
    {
        if (ReplicaAddresses == null || ReplicaAddresses.Length == 0)
            throw new InvalidOperationException("Реплика адресов не указана");

        var sw = Stopwatch.StartNew();
        Exception lastError = null;

        for (var i = 0; i < ReplicaAddresses.Length; i++)
        {
            var remaining = timeout - sw.Elapsed;
            if (remaining <= TimeSpan.Zero)
                break;

            var remainingReplicas = ReplicaAddresses.Length - i;
            var slice = TimeSpan.FromTicks(remaining.Ticks / remainingReplicas);

            var uri = ReplicaAddresses[i];
            var webRequest = CreateRequest(uri + "?query=" + query);

            Log.InfoFormat($"Обработка {webRequest.RequestUri}");

            var resultTask = ProcessRequestAsync(webRequest);

            var completed = await Task.WhenAny(resultTask, Task.Delay(slice));
            if (completed != resultTask)
            {
                _ = resultTask.ContinueWith(t => _ = t.Exception, TaskContinuationOptions.OnlyOnFaulted);
                continue;
            }

            try
            {
                return await resultTask;
            }
            catch (Exception e)
            {
                lastError = e;
            }
        }

        if (lastError != null)
            throw lastError;

        throw new TimeoutException();
    }

    protected override ILog Log => LogManager.GetLogger(typeof(RoundRobinClusterClient));
}