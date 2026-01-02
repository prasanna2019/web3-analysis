# Ethereum Transactions & Logs Ingestion Pipeline

A lightweight data engineering pipeline that ingests **latest Ethereum block transactions**, transforms raw on-chain data, and loads it into **Google BigQuery**. The pipeline then fetches **transaction receipts (logs)** using transaction hashes and ingests them into a separate BigQuery table.

## What This Does

- Fetches latest Ethereum block transactions via JSON-RPC
- Transforms and normalizes transaction data
- Loads transactions into BigQuery
- Fetches transaction receipts using tx hashes
- Loads logs into BigQuery
- Handles errors and logs pipeline execution

## Tech Stack

- Python
- Ethereum JSON-RPC (Infura)
- Pandas
- Google BigQuery
- dotenv

