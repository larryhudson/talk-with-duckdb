# DuckDB LLM CLI

A command-line interface for querying DuckDB databases using natural language powered by Large Language Models (LLMs).

**Note: This is a work in progress. Features and APIs may change.**

## Overview

This tool allows you to:
- Query DuckDB databases using natural language instead of SQL
- Load and analyze CSV and Parquet files
- Get AI-powered analysis of query results
- Have interactive follow-up conversations about your data

## Features

- Natural language to SQL translation using LLMs
- Support for CSV and Parquet file formats
- Automatic Parquet caching for better performance
- Interactive mode for follow-up questions
- Detailed schema analysis including relationships
- Result analysis with context awareness
- Verbose mode for debugging

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd duckdb-llm-cli
```

2. Install dependencies:
```bash
pip install -e .
```

3. Set up environment variables:
Create a `.env` file with your LLM API credentials:
```
LLM_MODEL=gpt-4  # or another supported model
OPENAI_API_KEY=your_api_key_here
```

## Usage

### Basic Query
```bash
duckdb-llm query data.csv "What were the total emissions by facility?"
```

### With Analysis
```bash
duckdb-llm query data.csv "Show me emission trends over time" --analyze
```

### Interactive Mode
```bash
duckdb-llm query data.csv "Compare emissions across departments" --analyze --interactive
```

### Verbose Output
```bash
duckdb-llm query data.csv "Find highest emitting activities" -v
```

## Sample Data Generation

The project includes a script to generate sample emissions data for testing:

```bash
python scripts/generate_emissions_data.py
```

This creates:
- carbon_emissions_data.csv: Main emissions data
- emission_factors.csv: Reference data for emission factors
- activities.csv: Reference data for activities

## Project Structure

- `duckdb_llm_cli/`: Main package directory
  - `cli.py`: Core CLI implementation
- `scripts/`: Utility scripts
  - `generate_emissions_data.py`: Sample data generator

## Development

This is an active project under development. Current areas of focus:

- Expanding query capabilities
- Improving result analysis
- Adding more data format support
- Enhancing interactive features
- Documentation improvements

## Contributing

Contributions are welcome! Please feel free to submit issues and pull requests.

## License

[Add your chosen license here]
