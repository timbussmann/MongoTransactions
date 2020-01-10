using System.Threading.Tasks;

namespace MongoTransactions
{
    class Program
    {
        static async Task Main(string[] args)
        {
            await ConcurrentTransactions.Run();
            //await SingleTransaction.Run();
        }
    }
}
