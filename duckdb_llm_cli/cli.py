import click
import duckdb
import re
from litellm import completion
from pathlib import Path
from dotenv import load_dotenv
import os
import hashlib

load_dotenv()

class DuckLLMContext:
    def __init__(self):
        self.conn = None
        self.model = os.getenv("LLM_MODEL", "gpt-4")  # Default to GPT-4 but allow override
        self.verbose = False
        self.cache_dir = Path.home() / ".duckdb_llm" / "cache"
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def get_parquet_cache_path(self, file_path: Path) -> Path:
        """Generate a unique cache file path for the input file"""
        # Create hash of the original file path and modification time
        file_stat = file_path.stat()
        hash_input = f"{file_path.absolute()}{file_stat.st_mtime}"
        file_hash = hashlib.md5(hash_input.encode()).hexdigest()
        return self.cache_dir / f"{file_path.stem}_{file_hash}.parquet"

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
        schema_text = "\n\n".join(schema_info)
        if self.verbose:
            click.echo("\nSchema Information:")
            click.echo(schema_text)
        return schema_text

    def generate_sql(self, question, schema_info):
        prompt = f"""Given the following database schema:

{schema_info}

Generate a SQL query to answer this question: {question}

Think through the solution step by step, putting your reasoning in <reasoning></reasoning> tags.
Then put your final SQL query in <answer></answer> tags.

For example:
<reasoning>
1. First we need to...
2. Then we should...
3. Finally we...
</reasoning>
<answer>
SELECT * FROM table;
</answer>"""

        if self.verbose:
            click.echo("\nSending prompt to LLM:")
            click.echo(prompt)
            click.echo("\nWaiting for LLM response...")

        response = completion(
            model=self.model,
            messages=[{"role": "user", "content": prompt}]
        )
        content = response.choices[0].message.content.strip()
        
        # Extract SQL query from <answer> tags
        match = re.search(r'<answer>(.*?)</answer>', content, re.DOTALL)
        if not match:
            raise ValueError("No SQL query found in <answer> tags in the response")
            
        return match.group(1).strip()

    def analyze_results(self, question, data, schema_info, previous_context=None):
        data_str = data.to_string()
        
        if previous_context:
            prompt = f"""Given the following database schema and query results:

Schema:
{schema_info}

Previous Context:
{previous_context}

Current Query Results:
{data_str}

Please answer this follow-up question: {question}"""
        else:
            prompt = f"""Given the following database schema and query results:

Schema:
{schema_info}

Query Results:
{data_str}

Please answer this question: {question}"""

        response = completion(
            model=self.model,
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
@click.option('--interactive/--no-interactive', default=False,
              help='Enable interactive follow-up questions')
@click.option('--verbose', '-v', is_flag=True, help='Show detailed progress')
@pass_context
def query(ctx_obj, file_path, question, analyze, verbose):
    """Query a DuckDB database using natural language"""
    file_path = Path(file_path)
    ctx_obj.verbose = verbose
    
    if verbose:
        click.echo("Connecting to database...")
    
    # Connect to database or create from file
    if file_path.suffix == '.duckdb':
        ctx_obj.conn = duckdb.connect(str(file_path))
        if verbose:
            click.echo(f"Connected to DuckDB file: {file_path}")
    else:
        ctx_obj.conn = duckdb.connect(":memory:")
        if verbose:
            click.echo("Created in-memory database")
        if file_path.suffix == '.csv':
            if verbose:
                click.echo(f"Processing CSV file...")
            
            table_name = file_path.stem
            cache_path = ctx_obj.get_parquet_cache_path(file_path)
            
            if cache_path.exists():
                if verbose:
                    click.echo(f"Using cached Parquet file: {cache_path}")
                ctx_obj.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM parquet_scan('{cache_path}')")
            else:
                if verbose:
                    click.echo(f"Creating new Parquet cache from CSV...")
                # Load CSV and create Parquet cache
                ctx_obj.conn.execute(f"""
                    CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{file_path}');
                    COPY (SELECT * FROM {table_name}) TO '{cache_path}' (FORMAT PARQUET);
                """)
                if verbose:
                    click.echo(f"Created Parquet cache: {cache_path}")
            
            if verbose:
                click.echo(f"Created table: {table_name}")
        elif file_path.suffix == '.parquet':
            if verbose:
                click.echo(f"Loading Parquet file into memory...")
            table_name = file_path.stem
            ctx_obj.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM parquet_scan('{file_path}')")
            if verbose:
                click.echo(f"Created table: {table_name}")
    
    if verbose:
        click.echo("\nGathering schema information...")
    schema_info = ctx_obj.get_schema_info()
    if verbose:
        click.echo("Schema information gathered")
        click.echo("\nGenerating SQL query...")
    
    sql_query = ctx_obj.generate_sql(question, schema_info)
    click.echo(f"Generated SQL:\n{sql_query}\n")
    
    if verbose:
        click.echo("Executing query...")
    
    # Execute query
    result = ctx_obj.conn.execute(sql_query).df()
    click.echo("Results:")
    click.echo(result)
    
    # Optionally analyze results
    if analyze:
        if verbose:
            click.echo("\nAnalyzing results...")
        analysis = ctx_obj.analyze_results(question, result, schema_info)
        click.echo("\nAnalysis:")
        click.echo(analysis)
        
        # Interactive mode for follow-up questions
        if interactive:
            previous_context = f"Previous question: {question}\nPrevious analysis: {analysis}"
            
            while True:
                try:
                    follow_up = click.prompt("\nAsk a follow-up question (or press Ctrl+C to exit)")
                    follow_up_analysis = ctx_obj.analyze_results(
                        follow_up, result, schema_info, previous_context
                    )
                    click.echo("\nFollow-up Analysis:")
                    click.echo(follow_up_analysis)
                    previous_context += f"\n\nFollow-up question: {follow_up}\nFollow-up analysis: {follow_up_analysis}"
                except (KeyboardInterrupt, click.exceptions.Abort):
                    click.echo("\nExiting interactive mode...")
                    break

if __name__ == '__main__':
    cli()
