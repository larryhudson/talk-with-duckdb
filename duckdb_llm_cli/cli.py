import click
import duckdb
import re
from litellm import completion
from pathlib import Path
from dotenv import load_dotenv
import os
import hashlib

load_dotenv()

def fill_prompt(schema, question):
    return """
You are an AI assistant specialized in generating SQL queries for a DuckDB database based on natural language questions from users. Your primary goal is to help users explore and gain insights from their data.

First, familiarize yourself with the schema of the database you will be querying:

<schema>
{schema}
</schema>

When a user asks a question, your task is to write an SQL query that answers their question and helps them explore the data. Follow these guidelines:

1. Analyze the user's question and the database schema to determine relevant tables and columns.
2. Write a clear and efficient SQL query that answers the user's question.
3. Prioritize data exploration by returning queries that provide meaningful insights.
4. Use advanced SQL features when appropriate (e.g., JOINs, GROUP BY, HAVING, subqueries, window functions).
5. If the question is vague or open to multiple interpretations, choose the one that will provide the most interesting or useful data exploration.
6. Limit the number of rows returned to 10 by default, unless specifically asked for more or fewer results.
7. If the question cannot be answered with the given schema, explain why and suggest a related query that might be helpful.

When you receive a question, follow these steps:

1. Analyze the question and relevant schema elements in <query_planning></query_planning> tags.
2. Write your SQL query in <answer></answer> tags.
3. Provide a brief explanation of what the query does and how it answers the user's question after the <answer> tags.

Here's an example of how your response should be structured:

<query_planning>
[Your thought process, including:
a. Identify key tables and columns relevant to the question
b. List potential JOIN conditions
c. Outline filtering criteria
d. Consider aggregation or grouping needs
e. Determine appropriate sorting]
</query_planning>

<answer>
SELECT column1, column2
FROM table1
JOIN table2 ON table1.id = table2.id
WHERE condition
LIMIT 10;
</answer>

Explanation: This query joins table1 and table2 on their id columns, selects column1 and column2, filters the results based on the specified condition, and limits the output to 10 rows. It answers the user's question by [brief explanation of how it addresses the user's needs].

Now, here's the user's question:

<question>
{question}
</question>

Please provide your response following the structure outlined above. After your initial response, be prepared for follow-up questions. For each follow-up:

1. Consider how it relates to the previous query and results.
2. Modify your previous query or write a new one to address the follow-up question.
3. Follow the same output format as the initial response.

Remember, your goal is to help the user explore the data and gain valuable insights through thoughtful and effective SQL queries.
"""


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
            # Get column information
            columns = self.conn.execute(f"DESCRIBE {table_name}").fetchall()
            cols_info = [f"{col[0]} {col[1]}" for col in columns]
            
            # Get sample data (first 3 rows)
            sample_data = self.conn.execute(f"SELECT * FROM {table_name} LIMIT 3").fetchall()
            sample_str = "\nSample data:"
            for row in sample_data:
                sample_str += f"\n  {row}"
            
            # Get row count
            row_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            
            # Detect potential relationships based on column names
            relationships = []
            for col in columns:
                col_name = col[0].lower()
                if col_name.endswith('_id'):
                    related_table = col_name.replace('_id', '')
                    relationships.append(f"Possible foreign key: {col_name} -> {related_table}")
            
            table_info = [
                f"Table: {table_name}",
                f"Rows: {row_count}",
                f"Columns: {', '.join(cols_info)}",
                sample_str
            ]
            if relationships:
                table_info.append("Relationships:\n  " + "\n  ".join(relationships))
            
            schema_info.append("\n".join(table_info))
        
        schema_text = "\n\n" + "\n\n".join(schema_info)
        if self.verbose:
            click.echo("\nSchema Information:")
            click.echo(schema_text)
        return schema_text

    def generate_sql(self, question, schema_info):
        messages = [
            {"role": "user", "content": fill_prompt(schema_info, question)},
        ]

        if self.verbose:
            click.echo("\nSending messages to LLM:")
            for msg in messages:
                click.echo(f"\n{msg['role']}: {msg['content']}")
            click.echo("\nWaiting for LLM response...")

        response = completion(
            model=self.model,
            messages=messages
        )

        content = response.choices[0].message.content.strip()
        
        # Add assistant's response to message history
        messages.append({"role": "assistant", "content": content})
        
        # Extract SQL query from <answer> tags
        match = re.search(r'<answer>(.*?)</answer>', content, re.DOTALL)
        if not match:
            raise ValueError("No SQL query found in <answer> tags in the response")
            
        return match.group(1).strip()

    def analyze_results(self, question, data, schema_info, previous_context=None):
        data_str = data.to_string()
        
        messages = [
            {"role": "system", "content": "You are a data analyst expert that helps analyze and explain SQL query results."},
            {"role": "user", "content": f"Here is the database schema:\n\n{schema_info}"},
            {"role": "user", "content": f"Here are the query results:\n\n{data_str}"}
        ]
        
        else:
            messages.append({"role": "user", "content": f"Please analyze these results to answer: {question}"})

        if self.verbose:
            click.echo("\nSending messages to LLM:")
            for msg in messages:
                click.echo(f"\n{msg['role']}: {msg['content']}")
            click.echo("\nWaiting for LLM response...")

        response = completion(
            model=self.model,
            messages=messages
        )
        content = response.choices[0].message.content.strip()
        
        # Add assistant's response to message history
        messages.append({"role": "assistant", "content": content})
        
        return content

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
def query(ctx_obj, file_path, question, analyze, interactive, verbose):
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
                click.echo("Processing CSV file...")
            
            # Get the directory containing the main CSV file
            data_dir = file_path.parent
            
            # Load all related CSV files if they exist
            main_table = file_path.stem
            ef_path = data_dir / "emission_factors.csv"
            act_path = data_dir / "activities.csv"
            
            # Load main emissions data
            cache_path = ctx_obj.get_parquet_cache_path(file_path)
            if cache_path.exists():
                if verbose:
                    click.echo(f"Using cached Parquet file: {cache_path}")
                ctx_obj.conn.execute(f"CREATE TABLE {main_table} AS SELECT * FROM parquet_scan('{cache_path}')")
            else:
                if verbose:
                    click.echo("Creating new Parquet cache from CSV...")
                ctx_obj.conn.execute(f"""
                    CREATE TABLE {main_table} AS SELECT * FROM read_csv_auto('{file_path}');
                    COPY (SELECT * FROM {main_table}) TO '{cache_path}' (FORMAT PARQUET);
                """)
                if verbose:
                    click.echo(f"Created Parquet cache: {cache_path}")
            
            # Load emission factors if available
            if ef_path.exists():
                if verbose:
                    click.echo("Loading emission factors data...")
                ctx_obj.conn.execute(
                    "CREATE TABLE emission_factors AS SELECT * FROM read_csv_auto(?)",
                    [str(ef_path)]
                )
            
            # Load activities if available
            if act_path.exists():
                if verbose:
                    click.echo("Loading activities data...")
                ctx_obj.conn.execute(
                    "CREATE TABLE activities AS SELECT * FROM read_csv_auto(?)",
                    [str(act_path)]
                )
            
            if verbose:
                click.echo(f"Created table: {main_table}")
        elif file_path.suffix == '.parquet':
            if verbose:
                click.echo("Loading Parquet file into memory...")
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
