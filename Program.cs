using System.Buffers;
using System.Threading.Channels;

const int BufferSize = 10 * 1024 * 1024;

static async Task<long> SumArrays(ChannelReader<byte[]> channel)
{
    long sum = 0;
    await foreach (var segmentSum in channel.ReadAllAsync().Select(SumArray))
    {
        unchecked { sum += segmentSum; }
    }
    return sum;
}

static long SumArray(byte[] segment)
{
    if (segment.Length % 4 != 0)
    {
        throw new InvalidOperationException();
    }
    long sum = 0;
    for (int i = 0; i < segment.Length; i += 4)
    {
        unchecked { sum += BitConverter.ToUInt32(segment, i); }
    }
    ArrayPool<byte>.Shared.Return(segment);
    return sum;
}

var fileName = args[0];
ArgumentException.ThrowIfNullOrWhiteSpace(fileName);

var channel = Channel.CreateBounded<byte[]>(new BoundedChannelOptions(8) { SingleWriter = true, SingleReader = true });
var sumTask = Task.Run(async () => await SumArrays(channel.Reader));

using var file = File.OpenRead(fileName);
while (true)
{
    var arr = ArrayPool<byte>.Shared.Rent(BufferSize);
    var bytesRead = file.Read(arr, 0, BufferSize);
    if (bytesRead == 0) break;
    await channel.Writer.WriteAsync(arr); // Block on write
}
channel.Writer.Complete();

Console.WriteLine(await sumTask);

