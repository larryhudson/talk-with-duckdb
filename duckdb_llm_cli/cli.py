import click
import duckdb
import openai
from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

class DuckLLMContext:
    def __init__(self):
        self.conn = None
        self.client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    def get_schema_info(self):
        if not self.conn:
            return ""
        tables = self.conn.execute("SHOW TABLES").fetchall()
        schema_info = []
        for table in tables:
            table_name = table[0]
            columns = self.conn.execute(f"DESCRIBE {table_name}").fetchall()
            cols_info = [f"{col[0]} {col[1]}" for col in columns]
            schema_info.append(f"Table: {table_name}\nColumns: {', '.join(cols_info)}")
        return "\n\n".join(schema_info)

    def generate_sql(self, question, schema_info):
        prompt = f"""Given the following database schema:

{schema_info}

Generate a SQL query to answer this question: {question}

Return only the SQL query, nothing else."""

        response = self.client.chat.completions.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}]
        )
        return response.choices[0].message.content.strip()

    def analyze_results(self, question, data, schema_info):
        data_str = data.to_string()
        prompt = f"""Given the following database schema and query results:

Schema:
{schema_info}

Query Results:
{data_str}

Please answer this question: {question}"""

        response = self.client.chat.completions.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}]
        )
        return response.choices[0].message.content.strip()

pass_context = click.make_pass_decorator(DuckLLMContext, ensure=True)

@click.group()
@click.pass_context
def cli(ctx):
    """Natural language queries for DuckDB using LLMs"""
    ctx.obj = DuckLLMContext()

@cli.command()
@click.argument('file_path', type=click.Path(exists=True))
@click.argument('question')
@click.option('--analyze/--no-analyze', default=False, 
              help='Whether to analyze the results using the LLM')
@pass_context
def query(ctx, file_path, question, analyze):
    """Query a DuckDB database using natural language"""
    file_path = Path(file_path)
    
    # Connect to database or create from file
    if file_path.suffix == '.duckdb':
        ctx.obj.conn = duckdb.connect(str(file_path))
    else:
        ctx.obj.conn = duckdb.connect(":memory:")
        if file_path.suffix in ['.csv', '.parquet']:
            table_name = file_path.stem
            ctx.obj.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{file_path}')")
    
    # Get schema info and generate SQL
    schema_info = ctx.obj.get_schema_info()
    sql_query = ctx.obj.generate_sql(question, schema_info)
    click.echo(f"Generated SQL:\n{sql_query}\n")
    
    # Execute query
    result = ctx.obj.conn.execute(sql_query).df()
    click.echo("Results:")
    click.echo(result)
    
    # Optionally analyze results
    if analyze:
        analysis = ctx.obj.analyze_results(question, result, schema_info)
        click.echo("\nAnalysis:")
        click.echo(analysis)

if __name__ == '__main__':
    cli()
