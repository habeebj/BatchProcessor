using System.Diagnostics;
using System.Text.Json;
using BatchProcessor;
using BatchProcessor.Models;

var accountRequest = new List<AccountModel> {
    new("54", "1234"),
    new("54", "4532"),
    new("54", "0001"),
    new("54", "0002"),
    new("54", "0003"),
    new("54", "0004"),
    new("44", "000"),
    new("44", "000"),
    new("43", "000"),
    new("54", "0004"),
    new("44", "000"),
    new("44", "000"),
    new("43", "000"),
    new("54", "0004"),
    new("44", "000"),
    new("44", "000"),
    new("43", "000"),
    new("54", "0004"),
    new("44", "000"),
    new("43", "000"),
    new("43", "0012")
};

Func<AccountModel, string> groupingFunction = (account) => account.BankId switch
{
    "43" => "1",
    _ => "2"
};

var batchProcessor = new BulkProcessor<AccountModel, AccountResult>(async (accounts) =>
{
    var request = accounts.Select(x => new AccountResult(x.BankId, x.AccountNumber, "Success"));
    Console.WriteLine($"Sending... {accounts.Count()}");
    Console.WriteLine($"{JsonSerializer.Serialize(accounts)}");

    await Task.Delay(2000);

    return request;
}, groupingFunction, batchSize: 50, maxDegreeOfParallelism: 100);

var stopwatch = new Stopwatch();
stopwatch.Start();

var result = await batchProcessor.ProcessAsync(accountRequest, CancellationToken.None);

stopwatch.Stop();

foreach (var item in result)
{
    Console.WriteLine($"{item.BankId} {item.AccountNumber} {item.Status}");
}

Console.WriteLine($"Done");
Console.WriteLine($"{stopwatch.ElapsedMilliseconds}ms");