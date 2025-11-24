using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DataPipeline.Core.Data;
using DataPipeline.Core.Pipeline;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace DataPipeline.Tests;

public class PipelineTests
{
    private static AppDbContext CreateInMemoryDb()
    {
        var options = new DbContextOptionsBuilder<AppDbContext>()
            .UseInMemoryDatabase(databaseName: Guid.NewGuid().ToString())
            .Options;

        return new AppDbContext(options);
    }

    [Fact]
    public async Task Pipeline_IgnoresEmptyFolder()
    {
        using var db = CreateInMemoryDb();

        var tempFolder = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(tempFolder);

        var logger = NullLogger<CsvTransactionPipeline>.Instance;
        var pipeline = new CsvTransactionPipeline(db, logger, tempFolder);

        var inserted = await pipeline.RunAsync();

        Assert.Equal(0, inserted);
        Assert.Empty(db.Transactions);
    }

    [Fact]
    public async Task Pipeline_IngestsValidCsvRows()
    {
        using var db = CreateInMemoryDb();

        var tempFolder = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(tempFolder);

        var csvPath = Path.Combine(tempFolder, "test.csv");
        await File.WriteAllTextAsync(csvPath,
            "Timestamp,Customer,Item,Amount,Currency\n" +
            "2025-11-24T09:00:00Z,Alice,Apples,12.50,USD\n" +
            "2025-11-24T10:15:00Z,Bob,Oranges,7.20,USD\n");

        var logger = NullLogger<CsvTransactionPipeline>.Instance;
        var pipeline = new CsvTransactionPipeline(db, logger, tempFolder);

        var inserted = await pipeline.RunAsync();

        Assert.Equal(2, inserted);
        Assert.Equal(2, db.Transactions.Count());
    }

    [Fact]
    public async Task Pipeline_SkipsMalformedRows()
    {
        using var db = CreateInMemoryDb();

        var tempFolder = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(tempFolder);

        var csvPath = Path.Combine(tempFolder, "mixed.csv");
        await File.WriteAllTextAsync(csvPath,
            "Timestamp,Customer,Item,Amount,Currency\n" +
            "invalid-date,Alice,Apples,12.50,USD\n" +              // bad date
            "2025-11-24T10:15:00Z,Bob,Oranges,not-a-number,USD\n" + // bad amount
            "2025-11-24T11:00:00Z,Charlie,Bananas,5.00,USD\n");     // valid

        var logger = NullLogger<CsvTransactionPipeline>.Instance;
        var pipeline = new CsvTransactionPipeline(db, logger, tempFolder);

        var inserted = await pipeline.RunAsync();

        Assert.Equal(1, inserted);
        Assert.Single(db.Transactions);
        Assert.Equal("Charlie", db.Transactions.Single().Customer);
    }

    [Fact]
    public async Task Pipeline_ComputesUsdAndHighValueFlags()
    {
        using var db = CreateInMemoryDb();

        var tempFolder = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(tempFolder);

        var csvPath = Path.Combine(tempFolder, "fx.csv");
        await File.WriteAllTextAsync(csvPath,
            "Timestamp,Customer,Item,Amount,Currency\n" +
            "2025-11-24T09:00:00Z,Alice,Apples,100.00,USD\n" +   // 100 USD -> high
            "2025-11-24T10:00:00Z,Bob,Oranges,1000.00,THB\n" +   // 1000 THB * 0.028 = 28 -> not high
            "2025-11-24T11:00:00Z,Charlie,Mango,2000.00,EUR\n"); // 2000 EUR * 1.1 = 2200 -> high

        var logger = NullLogger<CsvTransactionPipeline>.Instance;
        var pipeline = new CsvTransactionPipeline(db, logger, tempFolder);

        var inserted = await pipeline.RunAsync();

        Assert.Equal(3, inserted);

        var all = db.Transactions.OrderBy(t => t.Timestamp).ToList();

        // Alice
        Assert.Equal(100m, all[0].Amount);
        Assert.Equal(100m, all[0].AmountUsd);
        Assert.True(all[0].IsHighValue);

        // Bob
        Assert.Equal(1000m, all[1].Amount);
        Assert.Equal(28m, all[1].AmountUsd);
        Assert.False(all[1].IsHighValue);

        // Charlie
        Assert.Equal(2000m, all[2].Amount);
        Assert.Equal(2200m, all[2].AmountUsd);
        Assert.True(all[2].IsHighValue);
    }
}
