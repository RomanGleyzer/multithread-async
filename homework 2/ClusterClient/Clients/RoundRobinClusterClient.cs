using System;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients;

public class RoundRobinClusterClient(string[] replicaAddresses) : ClusterClientBase(replicaAddresses)
{
    public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
    {
        if (ReplicaAddresses == null || ReplicaAddresses.Length == 0)
            throw new InvalidOperationException("Реплика адресов не указана");

        var perReplicaTimeout = TimeSpan.FromTicks(timeout.Ticks / ReplicaAddresses.Length);

        Exception lastError = null;

        foreach (var uri in ReplicaAddresses)
        {
            var webRequest = CreateRequest(uri + "?query=" + query);

            Log.InfoFormat($"Обработка {webRequest.RequestUri}");

            var resultTask = ProcessRequestAsync(webRequest);
            
            await Task.WhenAny(resultTask, Task.Delay(perReplicaTimeout));

            if (!resultTask.IsCompleted)
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