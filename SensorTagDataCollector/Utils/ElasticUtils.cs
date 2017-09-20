using System;
using System.Collections.Generic;
using System.Threading;
using Nest;

namespace SensorTagElastic
{
    public class ElasticUtils
    {
        public static ElasticClient getElasticClient(Uri url, string indexName, bool deleteIndex )
        {
            Utils.Log("Initialising ES client...");

            var EsConfig = new ConnectionSettings(url);
            EsConfig.DefaultIndex(indexName );

            var esClient = new ElasticClient(EsConfig);

            if (deleteIndex)
            {
                Utils.Log("Deleting indices: {0}-*...", indexName);

                esClient.DeleteIndex(indexName + "-*");

                Utils.Log("Deletion complete.");
            }

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
    }
}
