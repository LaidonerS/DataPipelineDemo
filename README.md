🚀 DataPipelineDemo
A clean, modern .NET 8 data pipeline with CSV ingestion, REST API, background worker & tests.

Minimal. Efficient. Production-inspired.
Built to showcase backend engineering, data processing, and clean architecture in one elegant package.

🔥 What It Does

📥 Ingests CSV files from /data/input

🔄 Transforms data (USD conversion, high-value detection)

🗃️ Stores transactions in SQLite using EF Core

⚙️ Runs automatically via a background worker (every 10 seconds)

🌐 Exposes a REST API (Swagger included)

🧪 Has full xUnit test suite (EF InMemory)

🤖 Runs CI through GitHub Actions (restore → build → test)

Small project. Big signal.

🛠️ Tech Snapshot
Category	Stack
Backend	.NET 8 Minimal API
Database	SQLite + EF Core
Pipeline	Custom CSV processor
Background Jobs	HostedService
Testing	xUnit + EF InMemory
CI/CD	GitHub Actions
⚡ Run It Locally
git clone https://github.com/LaidonerS/DataPipelineDemo
cd DataPipelineDemo
dotnet test
cd src/DataPipeline.Api
dotnet run


Swagger UI → http://localhost:5271/swagger

📂 Project Layout
DataPipelineDemo/
├── data/input/              → Drop CSVs here
├── src/DataPipeline.Core    → Pipeline & DB logic
├── src/DataPipeline.Api     → API + background worker
└── tests/                   → Automated tests

🧪 Example CSV
Timestamp,Customer,Item,Amount,Currency
2025-11-24T10:00:00Z,Alice,Apples,100,USD
2025-11-24T11:15:00Z,Bob,Mango,2000,EUR


Drop it into data/input/ and the system takes care of everything.

🌐 Useful Endpoints

POST /pipeline/run — Trigger pipeline manually

GET /transactions — View all data

GET /transactions/summary — Aggregated stats
