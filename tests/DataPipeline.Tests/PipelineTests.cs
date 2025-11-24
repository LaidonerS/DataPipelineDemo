using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DataPipeline.Core.Data;
using DataPipeline.Core.Pipeline;
using Microsoft.EntityFrameworkCore;
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

        var pipeline = new CsvTransactionPipeline(db, tempFolder);

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

        var pipeline = new CsvTransactionPipeline(db, tempFolder);

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

        var pipeline = new CsvTransactionPipeline(db, tempFolder);

        var inserted = await pipeline.RunAsync();

        Assert.Equal(1, inserted);
        Assert.Single(db.Transactions);
        Assert.Equal("Charlie", db.Transactions.Single().Customer);
    }
}
