using System.Threading.Tasks.Dataflow;

namespace BatchProcessor;

public class BulkProcessor<TIn, TOut>
{
    private readonly int _batchSize;
    private readonly int _maxDegreeOfParallelism;
    private readonly Func<TIn, string> _groupingFunction;
    private readonly Func<IEnumerable<TIn>, Task<IEnumerable<TOut>>> _transform;
    private readonly Dictionary<string, BatchBlock<TIn>> _batchBlockStore = [];

    public BulkProcessor(
        Func<IEnumerable<TIn>, Task<IEnumerable<TOut>>> transform,
        Func<TIn, string>? groupingFunction = null,
        int batchSize = 5,
        int maxDegreeOfParallelism = 100)
    {
        _batchSize = batchSize;
        _transform = transform;
        _maxDegreeOfParallelism = maxDegreeOfParallelism;
        _groupingFunction = groupingFunction ??= (_) => string.Empty;
    }

    public Task<IEnumerable<TOut>> ProcessAsync(IEnumerable<TIn> input) => ProcessAsync(input.ToArray());

    public async Task<IEnumerable<TOut>> ProcessAsync(TIn[] input)
    {
        var transformBlock = new TransformBlock<IEnumerable<TIn>, IEnumerable<TOut>>(
            _transform, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = _maxDegreeOfParallelism });

        for (var i = 0; i < input.Length; i++)
        {
            var batchBlock = GetOrCreateBatchBlock(input[i], transformBlock);
            batchBlock.Post(input[i]);
        }

        foreach (var batchBlock in _batchBlockStore)
        {
            batchBlock.Value.Complete();
        }

        List<TOut> result = [];

        while (await transformBlock.OutputAvailableAsync())
        {
            var transformBlockResult = await transformBlock.ReceiveAsync();
            result.AddRange(transformBlockResult);
        }

        return result;
    }

    private BatchBlock<TIn> GetOrCreateBatchBlock(TIn account, TransformBlock<IEnumerable<TIn>, IEnumerable<TOut>> transformBlock)
    {
        var key = _groupingFunction(account);
        if (!_batchBlockStore.TryGetValue(key, out var batchBlock))
        {
            batchBlock = new BatchBlock<TIn>(_batchSize);
            _batchBlockStore.Add(key, batchBlock);
            batchBlock.LinkTo(transformBlock);
            batchBlock.Completion.ContinueWith(delegate { transformBlock.Complete(); });
        }

        return batchBlock;
    }
}