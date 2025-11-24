using DataPipeline.Api.Services;
using DataPipeline.Core;
using DataPipeline.Core.Data;
using DataPipeline.Core.Pipeline;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

// Connection string: SQLite file in API project folder
var connectionString = builder.Configuration.GetConnectionString("DefaultConnection")
                       ?? "Data Source=transactions.db";

// Input folder for CSV files: ../data/input relative to repo
var inputFolder = Path.Combine(builder.Environment.ContentRootPath, "..", "..", "data", "input");
inputFolder = Path.GetFullPath(inputFolder);

// Core services: DbContext + pipeline
builder.Services.AddDataPipelineCore(connectionString, inputFolder);

// Background worker that runs the pipeline periodically
builder.Services.AddHostedService<PipelineWorker>();

// Minimal API extras
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

// Ensure database & Transactions table exist
using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
    db.Database.EnsureCreated();
}

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.MapGet("/", () => Results.Redirect("/swagger"));

// Manually trigger pipeline
app.MapPost("/pipeline/run", async (IDataPipeline pipeline, CancellationToken ct) =>
{
    var count = await pipeline.RunAsync(ct);
    return Results.Ok(new { inserted = count });
})
.WithName("RunPipeline")
.WithSummary("Run the CSV → DB data pipeline")
.WithDescription("Reads CSV files from data/input, validates rows, and stores them in SQLite.");

// List recent transactions
app.MapGet("/transactions", async (AppDbContext db, int take = 100, CancellationToken ct = default) =>
{
    var items = await db.Transactions
        .OrderByDescending(t => t.Timestamp)
        .Take(take)
        .ToListAsync(ct);

    return Results.Ok(items);
})
.WithName("GetTransactions")
.WithSummary("Get recent transactions");

// Daily summary
app.MapGet("/transactions/summary", async (AppDbContext db, CancellationToken ct = default) =>
{
    var summary = await db.Transactions
        .GroupBy(t => t.Timestamp.Date)
        .Select(g => new
        {
            Date = g.Key,
            Count = g.Count(),
            Total = g.Sum(t => t.Amount)
        })
        .OrderByDescending(x => x.Date)
        .ToListAsync(ct);

    return Results.Ok(summary);
})
.WithName("GetTransactionSummary")
.WithSummary("Get daily aggregated transaction summary");

app.Run();
