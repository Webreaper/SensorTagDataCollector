using System;
using System.Collections.Generic;
using System.Threading;
using System.Linq;
using Nest;
using System.Linq.Expressions;
using static SensorTagElastic.MainClass;

namespace SensorTagElastic
{
    public class ElasticUtils
    {
        public static ElasticClient getElasticClient(Uri url, string indexName, bool deleteIndex )
        {
            Utils.Log("Initialising ES client with index {0}...", indexName);

            var EsConfig = new ConnectionSettings(url);
            EsConfig.DefaultIndex(indexName );

            var esClient = new ElasticClient(EsConfig);

            if (deleteIndex)
            {
                Utils.Log("Deleting indices: {0}-*...", indexName);

                esClient.DeleteIndex(indexName + "-*");

                Utils.Log("Deletion complete.");
            }

//            esClient.CreateIndex(indexName, c => c
 //                   .Mappings(m => m
  //                            .Map<SensorReading>(mm => mm.AutoMap())
   //                     )
    //            );

            /*var putIndexTemplateResponse = esClient.PutIndexTemplate("SensorTagTemplate", t => t
                     .Template(indexName + "*")
                    .Mappings(m => m
                          .Map<SensorReading>(tm => tm
                            .Properties(p => p
                                .Keyword(k => k
                                    .Name("uuid")
                                )
                            )
                        )
                    )
                );
*/
            return esClient;
        }

        public static void BulkInsert<T>(ElasticClient esClient, string index, ICollection<T> readings) where T : class
        {
            const int pageSize = 1000;
            Utils.Log("Performing bulk insert of {0} {1} into {2}.", readings.Count, typeof(T).Name, index);

            var waitHandle = new CountdownEvent(1);
            int totalPages = readings.Count / pageSize;

            var bulkAll = esClient.BulkAll(readings, b => b
                                           .Index(index)
                                           .BackOffRetries(2)
                                           .BackOffTime("30s")
                                           .RefreshOnCompleted(true)
                                           .MaxDegreeOfParallelism(4)
                                           .Size(pageSize)
            );

            bulkAll.Subscribe(new BulkAllObserver(
                onNext: (b) => { Utils.Log(" Working - page {0} of {1}...", b.Page, totalPages); },
                onError: (e) => { throw e; },
                onCompleted: () => waitHandle.Signal()
            ));

            waitHandle.Wait();

            Utils.Log("Index complete: {0} documents indexed", readings.Count );
        }

        public static void createDateBasedAliases(ElasticClient esClient, string indexName)
        {
            Utils.Log("Creating aliases for index {0}-*...", indexName);

            //create the alias
            esClient.Alias(a => a
                .Add(add => add
                     .Index(indexName + "-*")
                     .Alias(indexName)
                )
            );
        }

        /// <summary>
        /// Scans all documents. Add this if we want field selectors:
        ///         .Query(q => q.Term(f => f.Property1, "1")
        ///       && q.Term(k => k.Property2, "0"
        ///
        /// </summary>
        /// <param name="esClient">Es client.</param>
        /// <param name="index">Index.</param>
        /// <param name="modifier">Modifier.</param>
        /// <typeparam name="T">The 1st type parameter.</typeparam>
        public static void ScanAllDocs<T>( ElasticClient esClient, string index, Expression<Func<T, object>> sortField, Action<T, string> modifier ) where T: class 
        {
            const string scrollTTLMins = "10m";
            const int scrollPageSize = 500;

            ISearchResponse<T> scanResults = esClient.Search<T>(s => s
              .From(0)
              .Size(scrollPageSize)
              .MatchAll()
              .Index(index)
              .Sort( sort => sort.Ascending( sortField)) 
              .Scroll(scrollTTLMins)
            );

            if( scanResults.Hits.Any() )
            {
                foreach (var hit in scanResults.Hits)
                {
                    modifier(hit.Source, hit.Id);
                }
            }

            var results = esClient.Scroll<T>(scrollTTLMins, scanResults.ScrollId);
            while (results.Hits.Any())
            {
                foreach (var hit in results.Hits)
                {
                    modifier( hit.Source, hit.Id );
                }

                results = esClient.Scroll<T>(scrollTTLMins, results.ScrollId);
            }
        }

        public static void DeleteDuplicates(ElasticClient client, string index)
        {
            Utils.Log("Loading all docs to check for duplicates...");

            var allDocs = new Dictionary<string, SensorReading>();

            ScanAllDocs<SensorReading>(client, index, x => x.timestamp, (doc, id) => { doc.Id = id; allDocs.Add(id, doc); });

            Utils.Log("{0} documents loaded from ES", allDocs.Count());
            try
            {
                var dupeDocs = allDocs.GroupBy(x => new { x.Value.device.uuid, x.Value.timestamp })
                                        .Select(x => x.Skip(1))
                                        .Where(x => x.Any())
                                        .SelectMany(x => x)
                                        .Select(x => x.Value)
                                        .ToList();

                if (dupeDocs.Any())
                {
                    Utils.Log("Deleting... {0} duplicate documents found in index.", dupeDocs.Count());

                    var bulkResponse = client.DeleteMany(dupeDocs.Select(x => new SensorReading { Id = x.Id }));

                    Utils.Log("Deletion complete");
                }
            }
            catch( Exception ex )
            {
                Utils.Log("Exception: " + ex);
            }

        }
    }
}
