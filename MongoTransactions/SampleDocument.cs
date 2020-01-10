using MongoDB.Bson;

namespace MongoTransactions
{
    class SampleDocument
    {
        public ObjectId Id { get; set; }
        public string Content { get; set; }
        public string @lock { get; set; }
    }
}