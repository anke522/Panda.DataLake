using System.Threading.Tasks;

namespace DataFlow.Sample
{
    public interface ISolrToElastic
    {
       Task ExecutorAsync(string indexName);

       Task ParallelExecutorAsync(string indexName);
    }
}