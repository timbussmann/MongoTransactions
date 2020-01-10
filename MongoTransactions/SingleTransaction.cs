using System;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace MongoTransactions
{
    public class SingleTransaction
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

            using (var session = await client.StartSessionAsync())
            {
                session.StartTransaction();

                var result = collection.FindOneAndUpdate(
                    session,
                    filter.Eq("_id", documentId),
                    update.Set(f => f.Content, "inside transaction"),
                    new FindOneAndUpdateOptions<SampleDocument>() {ReturnDocument = ReturnDocument.After});
                Console.WriteLine("Updated document in transaction, new value: " + result.Content);

                var t = Task.Run(() =>
                {
                    // outside transaction:
                    var r = collection.FindOneAndUpdate(
                        filter.Eq("_id", documentId),
                        update.Set(f => f.Content, "outside transaction"),
                        new FindOneAndUpdateOptions<SampleDocument>()
                        {
                            ReturnDocument = ReturnDocument.After,
                            MaxTime = TimeSpan.FromSeconds(1)
                        });
                    Console.WriteLine("Updated document outside transaction, new value: " + result.Content);
                });

                await Task.Delay(5000);
                

                session.CommitTransaction();
                Console.WriteLine("committed transaction");

                await t;
            }
        }
    }
}