using System.Diagnostics;
using System.Globalization;
using DataPipeline.Core.Data;
using DataPipeline.Core.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace DataPipeline.Core.Pipeline;

public class CsvTransactionPipeline : IDataPipeline
{
    private readonly AppDbContext _db;
    private readonly string _inputFolder;
    private readonly ILogger<CsvTransactionPipeline> _logger;

    // Simple static FX rates to USD for demo purposes
    private static readonly Dictionary<string, decimal> FxToUsd = new(StringComparer.OrdinalIgnoreCase)
    {
        ["USD"] = 1.0m,
        ["EUR"] = 1.1m,
        ["NOK"] = 0.095m,
        ["THB"] = 0.028m
    };

    public CsvTransactionPipeline(
        AppDbContext db,
        ILogger<CsvTransactionPipeline> logger,
        string? inputFolder = null)
    {
        _db = db;
        _logger = logger;
        _inputFolder = inputFolder ?? Path.Combine(Directory.GetCurrentDirectory(), "data", "input");
    }

    public async Task<int> RunAsync(CancellationToken cancellationToken = default)
    {
        var stopwatch = Stopwatch.StartNew();

        if (!Directory.Exists(_inputFolder))
        {
            _logger.LogInformation("Pipeline: input folder '{Folder}' does not exist. Nothing to do.", _inputFolder);
            return 0;
        }

        var csvFiles = Directory.GetFiles(_inputFolder, "*.csv");
        if (csvFiles.Length == 0)
        {
            _logger.LogInformation("Pipeline: no CSV files found in '{Folder}'.", _inputFolder);
            return 0;
        }

        _logger.LogInformation("Pipeline: starting run. Found {FileCount} CSV file(s).", csvFiles.Length);

        var transactions = new List<Transaction>();
        var totalLines = 0;
        var skippedLines = 0;

        foreach (var file in csvFiles)
        {
            _logger.LogInformation("Pipeline: processing file {File}.", file);

            foreach (var line in File.ReadLines(file).Skip(1)) // skip header
            {
                if (string.IsNullOrWhiteSpace(line))
                    continue;

                totalLines++;

                var parts = line.Split(',');

                if (parts.Length < 5)
                {
                    skippedLines++;
                    continue; // ignore malformed rows
                }

                // Expected format:
                // Timestamp,Customer,Item,Amount,Currency
                if (!DateTime.TryParse(
                        parts[0],
                        CultureInfo.InvariantCulture,
                        DateTimeStyles.AssumeUniversal,
                        out var timestamp))
                {
                    skippedLines++;
                    continue;
                }

                if (!decimal.TryParse(
                        parts[3],
                        NumberStyles.Number,
                        CultureInfo.InvariantCulture,
                        out var amount))
                {
                    skippedLines++;
                    continue;
                }

                var currency = parts[4].Trim().ToUpperInvariant();

                // Transformations
                var rate = FxToUsd.TryGetValue(currency, out var r) ? r : 1.0m;
                var amountUsd = amount * rate;
                var isHighValue = amountUsd >= 100m;

                var transaction = new Transaction
                {
                    Timestamp = timestamp,
                    Customer = parts[1].Trim(),
                    Item = parts[2].Trim(),
                    Amount = amount,
                    Currency = currency,
                    AmountUsd = decimal.Round(amountUsd, 2),
                    IsHighValue = isHighValue
                };

                transactions.Add(transaction);
            }
        }

        if (transactions.Count == 0)
        {
            stopwatch.Stop();
            _logger.LogInformation(
                "Pipeline: finished with 0 new transactions. Lines read: {Lines}, lines skipped: {Skipped}. Took {Ms} ms.",
                totalLines,
                skippedLines,
                stopwatch.ElapsedMilliseconds);
            return 0;
        }

        await _db.Database.EnsureCreatedAsync(cancellationToken);

        await _db.Transactions.AddRangeAsync(transactions, cancellationToken);
        var inserted = await _db.SaveChangesAsync(cancellationToken);

        stopwatch.Stop();
        _logger.LogInformation(
            "Pipeline: finished. Inserted {Inserted} transaction(s). Lines read: {Lines}, lines skipped: {Skipped}. Took {Ms} ms.",
            inserted,
            totalLines,
            skippedLines,
            stopwatch.ElapsedMilliseconds);

        return inserted;
    }
}
