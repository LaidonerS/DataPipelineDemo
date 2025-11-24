namespace DataPipeline.Core.Models;

public class Transaction
{
    public int Id { get; set; }

    public DateTime Timestamp { get; set; }

    public string Customer { get; set; } = string.Empty;

    public string Item { get; set; } = string.Empty;

    public decimal Amount { get; set; }

    public string Currency { get; set; } = "USD";
}
