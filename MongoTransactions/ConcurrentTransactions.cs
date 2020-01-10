using System;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace MongoTransactions
{
    public class ConcurrentTransactions
    {
        static ObjectId documentId = ObjectId.GenerateNewId();

        static FilterDefinitionBuilder<SampleDocument> filter = Builders<SampleDocument>.Filter;
        static UpdateDefinitionBuilder<SampleDocument> update = Builders<SampleDocument>.Update;

        public static async Task Run()
        {
            var client = new MongoClient();

            var collection = client.GetDatabase("txText")
                .GetCollection<SampleDocument>("sampleCollection");
            collection.InsertOne(new SampleDocument() { Id = documentId, Content = "initial value" });

            var l1 = Task.Run(async () => await UpdateLoop2("loop1", client));
            var l2 = Task.Run(async () => await UpdateLoop2("loop2", client));
            await Task.WhenAll(l1);
        }

        // Transaction Callback API
        static async Task UpdateLoop2(string loopId, MongoClient client)
        {
            int iteration = 1;
            while (true)
            {
                using (var session = await client.StartSessionAsync())
                {
                    var collection = client.GetDatabase("txText")
                        .GetCollection<SampleDocument>("sampleCollection");

                    var transactionResult = await session.WithTransactionAsync(async (s, ct) =>
                    {
                        Console.WriteLine($"{loopId} started transaction");

                        var lockId = ObjectId.GenerateNewId();
                        var result = await collection.FindOneAndUpdateAsync(
                            s,
                            filter.Eq("_id", documentId),
                            update.Set("lock", lockId),
                            new FindOneAndUpdateOptions<SampleDocument>
                            {
                                ReturnDocument = ReturnDocument.After
                            });
                        Console.WriteLine($"{loopId} completed FindOneAndUpdate with data: {result.Content}");

                        await Task.Delay(1000);
                        await collection.UpdateOneAsync(
                            s,
                            filter.Eq("_id", documentId),
                            update.Set(d => d.Content, $"({loopId}/{iteration})"));
                        return $"({ loopId}/{ iteration})";

                    }, new TransactionOptions());
                    Console.WriteLine($"{loopId} updated document value to {transactionResult}");
                    Console.WriteLine($"{loopId} committed transaction");
                }

                iteration++;
            }
        }

        // Transaction Core API
        static async Task UpdateLoop(string loopId, MongoClient client)
        {
            int iteration = 1;
            while (true)
            {
                try
                {
                    using (var session = await client.StartSessionAsync())
                    {
                        session.StartTransaction();

                        var collection = client.GetDatabase("txText")
                            .GetCollection<SampleDocument>("sampleCollection");

                        Console.WriteLine($"{loopId} started transaction");

                        var lockId = ObjectId.GenerateNewId();
                        var result = await collection.FindOneAndUpdateAsync(
                            session,
                            filter.Eq("_id", documentId),
                            update.Set("lock", lockId),
                            new FindOneAndUpdateOptions<SampleDocument>
                            {
                                ReturnDocument = ReturnDocument.After
                            });
                        Console.WriteLine($"{loopId} completed FindOneAndUpdate with data: {result.Content}");

                        await Task.Delay(1000);
                        await collection.UpdateOneAsync(session,
                            filter.Eq("_id", documentId),
                            update.Set(d => d.Content, $"({loopId}/{iteration})"));
                        Console.WriteLine($"{loopId} updated document value to ({loopId}/{iteration})");

                        await session.CommitTransactionAsync();
                        Console.WriteLine($"{loopId} committed transaction");
                    }

                    iteration++;
                }
                catch (MongoCommandException e)
                {
                    Console.WriteLine($"{loopId} failed loop due to an exception: {e.GetType().FullName}");
                }

                //await Task.Delay(100);
            }
        }
    }
}