using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Xml.Linq;
using Microsoft.Extensions.DependencyInjection;
using Nest;
using SolrNet;
using SolrNet.Commands.Parameters;
using SolrNet.Schema;

namespace DataFlow.Sample
{
    class Program
    {
        static void Main(string[] args)
        {
            IServiceCollection serviceCollection =new ServiceCollection();

            serviceCollection.AddSolrNet("http://localhost:9996/solr/SYRK");

            IServiceProvider serviceProvider= serviceCollection.BuildServiceProvider();

            ISolrOperations<Dictionary<string, object>> solrOperations = serviceProvider.GetService<ISolrOperations<Dictionary<string,object>>>();

            var next = true;


            IElasticClient elasticClient = new ElasticClient(new Uri("http://127.0.0.1:9200"));

            var response = elasticClient.Indices.Exists(index: "ebss_syrk".ToLower());


            if (!response.Exists)
            {
                var solrSchema = solrOperations.GetSchema("schema.xml");


                var indexSettings = new IndexSettings()
                {
                    NumberOfReplicas = 0
                };
                var properties = new Properties();

             
                var filedTypes=  solrSchema.SolrFields.Select(f => f.Type.Name).Distinct();
                solrSchema.SolrFields.ForEach(sf =>
                {
                    switch (sf.Type.Name)
                    {
                        case "int":
                            properties.Add(sf.Name.ToLower(), new NumberProperty(NumberType.Integer));
                                break;
                        case "double":
                            properties.Add(sf.Name.ToLower(), new NumberProperty(NumberType.Double));
                            break;
                        case "float":
                            properties.Add(sf.Name.ToLower(), new NumberProperty(NumberType.Float));
                            break;
                        case "string":
                            properties.Add(sf.Name.ToLower(), new KeywordProperty());
                            break;
                        case "date":
                            properties.Add(sf.Name.ToLower(), new DateProperty());
                            break;
                        case "location_rpt":
                            properties.Add(sf.Name.ToLower(), new GeoPointProperty());
                            break;
                            default:                     
                                properties.Add(sf.Name.ToLower(), new TextProperty());
                            break;
                    }
                });

                var typeMappings = new TypeMapping()
                {
                    Properties = properties
                };

                elasticClient.Indices.Create("ebss_syrk", p =>
                    p.InitializeUsing(new IndexState()
                    {
                        Settings = indexSettings,
                        Mappings = typeMappings
                    }));
            }

            var stopwatch=new Stopwatch();
            stopwatch.Start();

            int start = 0;
            int batchSize = 5000;
            while (next)
            {
                var result = solrOperations.Query(SolrQuery.All, new QueryOptions
                {
                    StartOrCursor = new StartOrCursor.Start(start),
                    Rows = batchSize
                });

                result.ForEach(r =>
                {
                    if (r.ContainsKey("AC_GEO"))
                    {
                        var wkt = r["AC_GEO"].ToString().Split(" ");
                        r["AC_GEO"] = new GeoLocation(double.Parse(wkt[1]), double.Parse(wkt[0]));
                    }
                    else
                    {
                        Console.WriteLine("缺少经纬度");
                    }
                    

                });
               var bulkResponse= elasticClient.Bulk(p => p.IndexMany(result).Index("ebss_syrk"));

              var flushResponse= elasticClient.Indices.Flush();
                start += result.Count;
                next = result.NumFound > start;
                Console.WriteLine($"{start}/{ result.NumFound} ReaderOptions ended"  );
            }
            
            stopwatch.Stop();
            Console.WriteLine(stopwatch.ElapsedMilliseconds);

        
           
           Console.Read();
        }
    }
}
