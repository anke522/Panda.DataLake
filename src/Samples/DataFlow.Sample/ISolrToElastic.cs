using System.Threading.Tasks;

namespace DataFlow.Sample
{
    public interface ISolrToElastic
    {
       Task ExecutorAsync(string indexName);

       Task BullExecutorAsync(string indexName,int start, int endNum, int batchSize);

        Task ParallelExecutorAsync(string indexName);
    }
}