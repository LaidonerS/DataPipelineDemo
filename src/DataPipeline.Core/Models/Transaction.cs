namespace DataPipeline.Core.Models;

public class Transaction
{
    public int Id { get; set; }

    public DateTime Timestamp { get; set; }

    public string Customer { get; set; } = string.Empty;

    public string Item { get; set; } = string.Empty;

    public decimal Amount { get; set; }

    public string Currency { get; set; } = "USD";

    /// <summary>
    /// Amount converted to USD using a simple static FX table.
    /// </summary>
    public decimal AmountUsd { get; set; }

    /// <summary>
    /// Flag to highlight unusually high-value transactions (in USD).
    /// </summary>
    public bool IsHighValue { get; set; }
}
