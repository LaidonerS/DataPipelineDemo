using DataPipeline.Core.Pipeline;
using Microsoft.Extensions.Hosting;

namespace DataPipeline.Api.Services;

public class PipelineWorker : BackgroundService
{
    private readonly ILogger<PipelineWorker> _logger;
    private readonly IServiceProvider _serviceProvider;
    private readonly TimeSpan _interval;

    public PipelineWorker(
        ILogger<PipelineWorker> logger,
        IServiceProvider serviceProvider)
    {
        _logger = logger;
        _serviceProvider = serviceProvider;

        // How often to run the pipeline
        _interval = TimeSpan.FromMinutes(1);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Pipeline worker started.");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var scope = _serviceProvider.CreateScope();
                var pipeline = scope.ServiceProvider.GetRequiredService<IDataPipeline>();

                var inserted = await pipeline.RunAsync(stoppingToken);

                if (inserted > 0)
                {
                    _logger.LogInformation(
                        "Pipeline worker ingested {Count} transactions at {Time}.",
                        inserted,
                        DateTimeOffset.Now);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while running pipeline from background worker.");
            }

            try
            {
                await Task.Delay(_interval, stoppingToken);
            }
            catch (TaskCanceledException)
            {
                // App is shutting down
            }
        }

        _logger.LogInformation("Pipeline worker stopping.");
    }
}
