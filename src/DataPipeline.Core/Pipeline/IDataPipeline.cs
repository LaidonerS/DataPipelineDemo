namespace DataPipeline.Core.Pipeline;

public interface IDataPipeline
{
    /// <summary>
    /// Runs the ETL pipeline:
    /// - reads CSV files from an input folder,
    /// - parses & validates rows,
    /// - stores them in the database.
    /// Returns the number of transactions ingested.
    /// </summary>
    Task<int> RunAsync(CancellationToken cancellationToken = default);
}
