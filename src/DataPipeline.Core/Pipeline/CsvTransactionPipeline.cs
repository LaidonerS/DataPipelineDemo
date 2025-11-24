using System.Globalization;
using DataPipeline.Core.Data;
using DataPipeline.Core.Models;
using Microsoft.EntityFrameworkCore;

namespace DataPipeline.Core.Pipeline;

public class CsvTransactionPipeline : IDataPipeline
{
    private readonly AppDbContext _db;
    private readonly string _inputFolder;

    public CsvTransactionPipeline(AppDbContext db, string? inputFolder = null)
    {
        _db = db;
        _inputFolder = inputFolder ?? Path.Combine(Directory.GetCurrentDirectory(), "data", "input");
    }

    public async Task<int> RunAsync(CancellationToken cancellationToken = default)
    {
        if (!Directory.Exists(_inputFolder))
        {
            return 0;
        }

        var csvFiles = Directory.GetFiles(_inputFolder, "*.csv");
        var transactions = new List<Transaction>();

        foreach (var file in csvFiles)
        {
            foreach (var line in File.ReadLines(file).Skip(1)) // skip header
            {
                if (string.IsNullOrWhiteSpace(line))
                    continue;

                var parts = line.Split(',');

                if (parts.Length < 5)
                    continue; // ignore malformed rows

                // Expected format:
                // Timestamp,Customer,Item,Amount,Currency
                if (!DateTime.TryParse(
                        parts[0],
                        CultureInfo.InvariantCulture,
                        DateTimeStyles.AssumeUniversal,
                        out var timestamp))
                {
                    continue;
                }

                if (!decimal.TryParse(
                        parts[3],
                        NumberStyles.Number,
                        CultureInfo.InvariantCulture,
                        out var amount))
                {
                    continue;
                }

                var transaction = new Transaction
                {
                    Timestamp = timestamp,
                    Customer = parts[1].Trim(),
                    Item = parts[2].Trim(),
                    Amount = amount,
                    Currency = parts[4].Trim()
                };

                transactions.Add(transaction);
            }
        }

        if (transactions.Count == 0)
            return 0;

        // Ensure DB & migrations are applied
	await _db.Database.EnsureCreatedAsync(cancellationToken);

        await _db.Transactions.AddRangeAsync(transactions, cancellationToken);
        var inserted = await _db.SaveChangesAsync(cancellationToken);

        return inserted;
    }
}
