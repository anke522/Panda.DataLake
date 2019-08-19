using System;
using ElasticSearch.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Serilog;
using SolrNet;

namespace DataFlow.Sample
{
    class Program
    {
        static void Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
                .Enrich.With(new ThreadIdEnricher())
                .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] ({ThreadId}) {Message:lj}{NewLine}{Exception}")
                .CreateLogger();

            Log.Information("Please enter solr core name!");

            var index= Console.ReadLine();
            var prefixIndexName = index;

            Log.Information($"You enter solr core name is :{index}");

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

            serviceCollection.AddLogging(loggingBuilder => loggingBuilder.AddSerilog(dispose: true));
            serviceCollection.AddScoped(typeof(ISolrToElastic), typeof(SolrToElastic));

            var serviceProvider= serviceCollection.BuildServiceProvider();

            var solrToElastic = serviceProvider.GetService<ISolrToElastic>();

            solrToElastic.ExecutorAsync(prefixIndexName).GetAwaiter().GetResult();

            Console.Read();
        }

    }
}
