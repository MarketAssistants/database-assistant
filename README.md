# database_assistant

Handles all database operations, including updating price/volume and technical metrics. Object-oriented design via the `Database_Assistant` class. Interacts with other assistants for data analysis and reporting.

## Requirements
- Place `nasdaq_screener.csv` (from [Nasdaq Screener](https://www.nasdaq.com/market-activity/stocks/screener)) in the `DATABASE` folder.
- Install dependencies from the main MarketAssistants folder using the shared requirements file.

## Features
- Update and maintain price/volume database
- Store and update technical analysis metrics
- Push results to AWS Postgres DB

## Usage
Import and instantiate `Database_Assistant` in your scripts. See `tests/db-assistant.py` for examples.
