using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using System.Xml.Linq;
using ElasticSearch.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Nest;
using Serilog;
using SolrNet;
using SolrNet.Commands.Parameters;
using SolrNet.Impl;
using SolrNet.Microsoft.DependencyInjection;
using SolrNet.Schema;
using Serilog.Extensions.Logging;

namespace DataFlow.Sample
{
    class Program
    {
        static void Main(string[] args)
        {

            Console.WriteLine("Please enter solr core name!");
            var index= Console.ReadLine();
            var prefixIndexName = index;

            Console.WriteLine($"You enter solr core name is :{index}");

            IServiceCollection serviceCollection =new ServiceCollection();

            var credentials = System.Text.Encoding.ASCII.GetBytes("test:123");

            var credentialsBase64 = Convert.ToBase64String(credentials);


            serviceCollection.AddSolrNet($"http://192.168.30.3:9988/solr/{prefixIndexName.ToUpper()}",
                options =>
                {
                    options.HttpClient.DefaultRequestHeaders.Authorization =
                        new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", credentialsBase64);
                });

            serviceCollection.AddElasticSearch("http://127.0.0.1:9200");

            Log.Logger = new LoggerConfiguration()
                .WriteTo.Console()
                .CreateLogger();

            serviceCollection.AddLogging(loggingBuilder => loggingBuilder.AddSerilog(dispose: true));
            serviceCollection.AddScoped(typeof(ISolrToElastic), typeof(SolrToElastic));

            IServiceProvider serviceProvider= serviceCollection.BuildServiceProvider();

            ISolrToElastic solrToElastic = serviceProvider.GetService<ISolrToElastic>();


            solrToElastic.ParallelExecutorAsync(prefixIndexName).GetAwaiter().GetResult();

            Console.Read();
        }

    }
}
