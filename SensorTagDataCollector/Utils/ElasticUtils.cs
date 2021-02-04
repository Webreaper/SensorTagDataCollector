using System;
using System.Collections.Generic;
using System.Threading;
using System.Linq;
using Nest;
using System.Linq.Expressions;
using static SensorTagElastic.MainClass;

namespace SensorTagElastic
{
    public static class ElasticUtils
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

                esClient.Indices.Delete(indexName + "-*");

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

        public static void BulkInsert<T>( this ElasticClient client, string index, ICollection<T> items ) where T : class
        {
            const int pageSize = 1000;
            string typeName = typeof(T).Name;

            Utils.Log("Performing bulk insert of {0} {1} into {2}.", items.Count(), typeName, index);

            using (var waitHandle = new CountdownEvent(1))
            {
                int totalPages = items.Count / pageSize;

                var bulkAll = client.BulkAll(items, b => b
                                               .Index( index.ToLower() )
                                               .BackOffRetries(2)
                                               .BackOffTime("10s")
                                               .RefreshOnCompleted(true)
                                               .MaxDegreeOfParallelism(4)
                                               .Size(pageSize)
                );

                bulkAll.Subscribe(new BulkAllObserver(
                    onNext: (b) => { Utils.Log(" Working - page {0} of {1}...", b.Page, totalPages); },
                    onError: (e) => { 
                        Utils.Log("Exception: " + e.Message); 
                        waitHandle.Signal(); 
                        throw e; 
                    },
                    onCompleted: () =>
                    {
                        Utils.Log("Index complete: {0} documents indexed", items.Count());
                        waitHandle.Signal();
                    }
                ));

                waitHandle.Wait();
            }    
        }

        public static void createDateBasedAliases(this ElasticClient esClient, string indexName)
        {
            Utils.Log("Creating aliases for index {0}-*...", indexName);

            //create the alias
            esClient.Indices.BulkAlias( aliases => 
                aliases.Add(add => add
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
        public static void ScanAllDocs<T>( this ElasticClient esClient, string index, string sortField, Action<T, string> modifier, QueryContainer query ) where T: class 
        {
            const string scrollTTLMins = "10m";
            const int scrollPageSize = 500;

            if (query == null)
                query = new MatchAllQuery();

            var req = new SearchRequest(index)
            {
                From = 0,
                Size = scrollPageSize,
                Query = query,
                Sort = new List<ISort> { new FieldSort { Field = sortField, Order = SortOrder.Ascending } },
                Scroll = scrollTTLMins
            };

            ISearchResponse<T> scanResults = esClient.Search<T>(req);

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

        /// <summary>
        /// Find the timestamp of the most recent record for each device. 
        /// </summary>
        /// <returns>The high water mark.</returns>
        /// <param name="indexName">IndexName</param>
        /// <param name="query">Term Query to select records</param>
        public static T getHighWaterMark<T>(this ElasticClient client, string indexName, QueryContainer query) where T : class
        {
            if (query == null)
                query = new MatchAllQuery();

            var req = new SearchRequest(indexName)
            {
                From = 0,
                Size = 1,
                Query = query,
                Sort = new List<ISort> { new FieldSort { Field = "timestamp", Order = SortOrder.Descending } }
            };

            var searchResponse = client.Search<T>(req);

            if (searchResponse.IsValid)
            {
                if (searchResponse.Documents.Count() > 1)
                    throw new ArgumentOutOfRangeException("More than one record returned from high-watermark");

                var mostRecent = searchResponse.Documents.FirstOrDefault();

                return mostRecent;
            }

            if( searchResponse.OriginalException != null )
                Utils.Log("Exception querying ES for watermark: {0}", searchResponse.OriginalException.Message);
            else
                Utils.Log("Error querying ES for watermark: {0}", searchResponse);
            return null;
        }

        public static void DeleteDuplicates(this ElasticClient client, string index)
        {
            Utils.Log("Loading all docs to check for duplicates...");

            var allDocs = new Dictionary<string, SensorReading>();

            client.ScanAllDocs<SensorReading>(index, "timestamp", (doc, id) => { doc.Id = id; allDocs.Add(id, doc); }, null);

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
