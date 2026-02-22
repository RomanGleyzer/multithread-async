using System;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients;

public class ParallelClusterClient(string[] replicaAddresses) : ClusterClientBase(replicaAddresses)
{
    public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
    {
        if (ReplicaAddresses == null || ReplicaAddresses.Length == 0)
            throw new InvalidOperationException("Реплика адресов не указана");

        var tasks = new Task<string>[ReplicaAddresses.Length];

        for (var i = 0; i < ReplicaAddresses.Length; i++)
        {
            var uri = ReplicaAddresses[i];
            var webRequest = CreateRequest(uri + "?query=" + query);

            Log.InfoFormat($"Обработка {webRequest.RequestUri}");

            tasks[i] = ProcessRequestAsync(webRequest);
        }

        var anyReplicaTask = Task.WhenAny(tasks);
        var timeoutTask = Task.Delay(timeout);

        var completed = await Task.WhenAny(anyReplicaTask, timeoutTask);
        if (ReferenceEquals(completed, timeoutTask))
            throw new TimeoutException();

        return await anyReplicaTask.Result;
    }

    protected override ILog Log => LogManager.GetLogger(typeof(ParallelClusterClient));
}