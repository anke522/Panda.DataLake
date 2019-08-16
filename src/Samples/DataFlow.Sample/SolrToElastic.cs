using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Nest;
using SolrNet;
using SolrNet.Commands.Parameters;

namespace DataFlow.Sample
{
    public class SolrToElastic: ISolrToElastic
    {
        private readonly ISolrOperations<Dictionary<string, object>> _solrOperation;
        private readonly IElasticClient _elasticClient;
        private readonly ILogger<SolrToElastic> _logger;

        private  int _documentCount;
        public SolrToElastic(ISolrOperations<Dictionary<string, object>> solrOperations, IElasticClient elasticClient,ILogger<SolrToElastic> logger)
        {
            this._solrOperation = solrOperations;
            this._elasticClient = elasticClient;
            this._logger = logger;
        }

        public async Task ExecutorAsync(string indexName)
        {
            indexName=  await InitProcessor(indexName);

            var next = true;

            var stopwatch = new Stopwatch();
            stopwatch.Start();

            int start = 0;
            int batchSize = 10000;

            while (next)
            {
                var result =await Transform(start, batchSize,  indexName);
                start += result;
                next = _documentCount > start;
            }

            stopwatch.Stop();

            _logger.LogInformation($"total time: {stopwatch.ElapsedMilliseconds / (1000 * 60)}  mins ");
        }

        public async Task ParallelExecutorAsync(string indexName)
        {
            indexName = await InitProcessor(indexName);

            int batchSize = 5000;

            var length = (double)_documentCount / batchSize;

            var pureLength = Math.Ceiling(decimal.Parse(length.ToString())).ToString(CultureInfo.InvariantCulture);

            var parallelOptions = new ParallelOptions()
            {
                MaxDegreeOfParallelism = 6
            };

            var stopwatch = new Stopwatch();
            stopwatch.Start();

            Parallel.For( 0, int.Parse(pureLength), async (pipe)  =>
            {
                try
                {
                    await Transform(pipe, batchSize * pipe, indexName);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    throw;
                }
//                var stopwatchIn = new Stopwatch();
//                stopwatchIn.Start();
//
//                Console.WriteLine($"Start ReaderOptions ");
//                var result = _solrOperation.QueryAsync(SolrQuery.All, new QueryOptions
//                {
//                    StartOrCursor = new StartOrCursor.Start(pipe*batchSize),
//                    Rows = batchSize
//                }).GetAwaiter().GetResult();
//
//                stopwatchIn.Stop();
//                Console.WriteLine($" Reader Taked {stopwatchIn.ElapsedMilliseconds} Milliseconds");
//
//
//
//                result.ForEach(r =>
//                {
//                    if (r.ContainsKey("AC_GEO"))
//                    {
//                        var wkt = r["AC_GEO"].ToString().Split(" ");
//                        r["AC_GEO"] = new GeoLocation(double.Parse(wkt[1]), double.Parse(wkt[0]));
//                    }
//                    else
//                    {
//                        //                        if(r.ContainsKey("X")&& r.ContainsKey("Y"))
//                        //                        {
//                        //                            r["AC_GEO"] = new GeoLocation(double.Parse(r["Y"].ToString()), double.Parse(r["X"].ToString()));
//                        //                            //Console.WriteLine("以XY坐标合并经纬度");
//                        //                        }
//                        if (r.ContainsKey("AC_XCOORD") && r.ContainsKey("AC_YCOORD"))
//                        {
//                            r["AC_GEO"] = new GeoLocation(double.Parse(r["AC_YCOORD"].ToString()), double.Parse(r["AC_XCOORD"].ToString()));
//                            //Console.WriteLine("以XY坐标合并经纬度");
//                        }
//                        else
//                        {
//                            //  Console.WriteLine("缺少经纬度");
//                        }
//                    }
//
//                    if (r.ContainsKey("GEO"))
//                    {
//
//                        //                        var geoString = r["GEO"].ToString();
//                        //                        var geoWKT=  GeoWKTReader.Read();
//                        //                        r["GEO"] = geoWKT;
//                    }
//
//                    if (r.ContainsKey("X"))
//                    {
//                        r.Remove("X");
//                    }
//
//                    if (r.ContainsKey("Y"))
//                    {
//                        r.Remove("Y");
//                    }
//                    if (r.ContainsKey("_version_"))
//                    {
//                        r.Remove("_version_");
//                    }
//
//                    //return r;
//                });
//
//                stopwatchIn.Restart();
//                Console.WriteLine($"Index Start");
//
//                var bulkResponse = _elasticClient.Bulk(p => p.IndexMany(result).Index(indexName));
//
//                stopwatchIn.Stop();
//                Console.WriteLine($" Index Taked {stopwatchIn.ElapsedMilliseconds} Milliseconds");
//                if (bulkResponse.Errors)
//                {
//
//                }
            });

            stopwatch.Stop();

            _logger.LogInformation($"total time: {stopwatch.ElapsedMilliseconds / (1000 * 60)}  mins ");
          
        }

        private async Task<string> InitProcessor(string indexName)
        {
            indexName = "gdst_" + indexName.ToLower();

            _documentCount = (await _solrOperation.QueryAsync(SolrQuery.All, new QueryOptions()
            {
                StartOrCursor = new StartOrCursor.Start(0),
                Rows = 1
            })).NumFound;

            _logger.LogInformation($"{indexName} NumFound :{_documentCount}");

            var response = await _elasticClient.Indices.ExistsAsync(index: indexName.ToLower());

            var solrSchema = await _solrOperation.GetSchemaAsync("schema.xml");

            var filedTypes = solrSchema.SolrFields.Select(f => f.Type.Name).Distinct();
            _logger.LogInformation($"total fields: {solrSchema.SolrFields.Count} Types: {string.Join(",", filedTypes)}");

            if (!response.Exists)
            { 
                var indexSettings = new IndexSettings()
                {
                    NumberOfReplicas = 0
                };
                var properties = new Properties();

                solrSchema.SolrFields.ForEach(sf =>
                {
                    switch (sf.Type.Name)
                    {
                        case "int":
                            properties.Add(sf.Name, new NumberProperty(NumberType.Integer));
                            break;
                        case "double":
                            properties.Add(sf.Name, new NumberProperty(NumberType.Double));
                            break;
                        case "float":
                            properties.Add(sf.Name, new NumberProperty(NumberType.Float));
                            break;
                        case "string":
                            properties.Add(sf.Name, new KeywordProperty());
                            break;
                        case "date":
                            properties.Add(sf.Name, new DateProperty());
                            break;
                        case "location_rpt":
                            if (sf.Name == "GEO")
                            {
                                properties.Add(sf.Name, new GeoShapeProperty());
                            }
                            else
                            {
                                properties.Add(sf.Name, new GeoPointProperty());
                            }

                            break;
                        default:
                            properties.Add(sf.Name, new TextProperty());
                            break;
                    }
                });

                var typeMappings = new TypeMapping()
                {
                    Properties = properties
                };

                _elasticClient.Indices.Create(indexName, p =>
                    p.InitializeUsing(new IndexState()
                    {
                        Settings = indexSettings,
                        Mappings = typeMappings
                    }));
            }

            return indexName;
        }

        private async Task<int> Transform(int start, int batchSize,
            string indexName)
        {
            var stopwatchIn = new Stopwatch();
            stopwatchIn.Start();

            var result =await _solrOperation.QueryAsync(SolrQuery.All, new QueryOptions
            {
                StartOrCursor = new StartOrCursor.Start(start),
                Rows = batchSize
            });

            stopwatchIn.Stop();
            _logger.LogInformation($" Reader Taked {stopwatchIn.ElapsedMilliseconds} Milliseconds");

            result.ForEach(r =>
            {
                if (r.ContainsKey("AC_GEO"))
                {
                    var wkt = r["AC_GEO"].ToString().Split(" ");
                    r["AC_GEO"] = new GeoLocation(double.Parse(wkt[1]), double.Parse(wkt[0]));
                }
                else
                {
                    //                        if(r.ContainsKey("X")&& r.ContainsKey("Y"))
                    //                        {
                    //                            r["AC_GEO"] = new GeoLocation(double.Parse(r["Y"].ToString()), double.Parse(r["X"].ToString()));
                    //                            //Console.WriteLine("以XY坐标合并经纬度");
                    //                        }
                    if (r.ContainsKey("AC_XCOORD") && r.ContainsKey("AC_YCOORD"))
                    {
                        r["AC_GEO"] = new GeoLocation(double.Parse(r["AC_YCOORD"].ToString()),
                            double.Parse(r["AC_XCOORD"].ToString()));
                        //Console.WriteLine("以XY坐标合并经纬度");
                    }
                    else
                    {
                        //  Console.WriteLine("缺少经纬度");
                    }
                }

                if (r.ContainsKey("GEO"))
                {
                    //                        var geoString = r["GEO"].ToString();
                    //                        var geoWKT=  GeoWKTReader.Read();
                    //                        r["GEO"] = geoWKT;
                }

                if (r.ContainsKey("X"))
                {
                    r.Remove("X");
                }
                if (r.ContainsKey("Y"))
                {
                    r.Remove("Y");
                }
                if (r.ContainsKey("_version_"))
                {
                    r.Remove("_version_");
                }
                //return r;
            });

            stopwatchIn.Restart();
            var bulkResponse = _elasticClient.Bulk(p => p.IndexMany(result).Index(indexName));

            stopwatchIn.Stop();
            _logger.LogInformation($" Index Taked {stopwatchIn.ElapsedMilliseconds} Milliseconds");
            if (bulkResponse.Errors)
            {
            }

            var flushResponse = _elasticClient.Indices.Flush();
            return result.Count;
        }
    }
}