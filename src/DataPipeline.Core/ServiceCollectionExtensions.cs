using DataPipeline.Core.Data;
using DataPipeline.Core.Pipeline;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DataPipeline.Core;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddDataPipelineCore(
        this IServiceCollection services,
        string connectionString,
        string? inputFolder = null)
    {
        services.AddDbContext<AppDbContext>(options =>
        {
            options.UseSqlite(connectionString);
        });

        services.AddScoped<IDataPipeline>(sp =>
        {
            var db = sp.GetRequiredService<AppDbContext>();
            var logger = sp.GetRequiredService<ILogger<CsvTransactionPipeline>>();
            return new CsvTransactionPipeline(db, logger, inputFolder);
        });

        return services;
    }
}
