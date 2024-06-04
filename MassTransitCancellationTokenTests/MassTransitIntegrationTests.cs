using MassTransit;
using MassTransit.Testing;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace MassTransitCancellationTokenTests;

public class DownloadCurrentWeekMessage { }
public class DownloadCurrentWeekMessageV2 { }

public class DownloadCurrentWeekService
{
    public async Task<bool> Download(CancellationToken cancellationToken)
    {
        for (int i = 0; i < 20; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await Task.Delay(500, cancellationToken); 
        }
        return true;
    }
}

public class DownloadCurrentWeekMessageConsumer : 
    IConsumer<DownloadCurrentWeekMessage>,
    IConsumer<DownloadCurrentWeekMessageV2>
{
    private readonly DownloadCurrentWeekService _downloadCurrentWeekService;

    public DownloadCurrentWeekMessageConsumer(DownloadCurrentWeekService downloadCurrentWeekService)
    {
        _downloadCurrentWeekService = downloadCurrentWeekService;
    }

    public Task Consume(ConsumeContext<DownloadCurrentWeekMessage> context)
    {
        throw new ArgumentException();
    }

    public async Task Consume(ConsumeContext<DownloadCurrentWeekMessageV2> context)
    {
        await _downloadCurrentWeekService.Download(context.CancellationToken);
    }
}

public class MassTransitIntegrationTests
{
    private ITestHarness _harness;

    [Fact]
    public async Task Test_Message_Consumed_By_Real_Consumer_With_Cancellation()
    {
        var provider = new ServiceCollection()
            .AddTransient<DownloadCurrentWeekService>()
            .AddMassTransitTestHarness(x =>
            {
                x.AddConsumer<DownloadCurrentWeekMessageConsumer>();

                x.UsingInMemory((context, cfg) =>
                {
                    cfg.ConfigureEndpoints(context);
                });
            })
            .BuildServiceProvider();

        _harness = provider.GetRequiredService<ITestHarness>();
        await _harness.Start();

        var cancellationTokenSource = new CancellationTokenSource();
        await _harness.Bus.Wait(x =>x.Publish(new DownloadCurrentWeekMessageV2(), cancellationTokenSource.Token));
        
        //wait for the consumer to start working.
        await Task.Delay(500);

        await cancellationTokenSource.CancelAsync();

        //wait so the service processing the cancellation
        await Task.Delay(2000);

        Assert.False(await _harness.Consumed.Any<DownloadCurrentWeekMessageV2>());
    }

    [Fact]
    public async Task Test_Message_Consumed_By_Real_Consumer()
    {
        var provider = new ServiceCollection()
            .AddTransient<DownloadCurrentWeekService>()
            .AddMassTransitTestHarness(x =>
            {
                x.AddConsumer<DownloadCurrentWeekMessageConsumer>();

                x.UsingInMemory((context, cfg) =>
                {
                    cfg.ConfigureEndpoints(context);
                });
            })
            .BuildServiceProvider();

        _harness = provider.GetRequiredService<ITestHarness>();
        await _harness.Start();

        await _harness.Bus.Wait(x => x.Publish(new DownloadCurrentWeekMessage()));
        Assert.False(await _harness.Consumed.Any<DownloadCurrentWeekMessage>());
    }
}