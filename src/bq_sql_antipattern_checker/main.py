"""Command-line interface for BigQuery SQL Antipattern Checker.

This module provides a Typer-based CLI application for running antipattern analysis
on BigQuery jobs, managing configuration files, and displaying results.

The CLI supports multiple commands:
- run: Execute antipattern analysis
- list-antipatterns: Display available antipatterns and their status
- create-config: Generate a default configuration file

Example:
    Run antipattern analysis:

    $ bq-antipattern-checker run --config my-config.yaml --verbose

    List available antipatterns:

    $ bq-antipattern-checker list-antipatterns
"""

import json
import time
from enum import Enum
from pathlib import Path

import typer
from pandas import DataFrame
from rich.console import Console
from rich.table import Table

from bq_sql_antipattern_checker import functions
from bq_sql_antipattern_checker.antipatterns import Antipatterns
from bq_sql_antipattern_checker.classes import Job
from bq_sql_antipattern_checker.config import Config


class OutputFormat(str, Enum):
    """Supported output formats for dry-run results."""

    CONSOLE = "console"
    JSON = "json"
    CSV = "csv"
    PARQUET = "parquet"


app = typer.Typer(help="BigQuery SQL Antipattern Checker")
console = Console()


def save_results_locally(
    df: DataFrame, output_format: OutputFormat, output_file: Path | None, config: Config
) -> None:
    """Save results locally in the specified format."""
    if output_format == OutputFormat.CONSOLE:
        display_results_console(df)
        return

    # Generate filename if not provided
    if output_file is None:
        date_str = config.date_values["partition_date"]
        output_file = Path(f"antipattern_results_{date_str}.{output_format.value}")

    # Ensure output directory exists
    output_file.parent.mkdir(parents=True, exist_ok=True)

    try:
        if output_format == OutputFormat.JSON:
            # Convert DataFrame to JSON
            json_data = df.to_dict(orient="records")
            with open(output_file, "w") as f:
                json.dump(json_data, f, indent=2, default=str)

        elif output_format == OutputFormat.CSV:
            df.to_csv(output_file, index=False)

        elif output_format == OutputFormat.PARQUET:
            df.to_parquet(output_file, index=False)

        console.print(f"💾 Results saved to: {output_file}", style="green")

    except Exception as e:
        console.print(f"❌ Error saving results: {e}", style="red")
        raise typer.Exit(code=1)


def display_results_console(df: DataFrame) -> None:
    """Display results in a formatted table in the console."""
    # Show summary statistics
    total_jobs = len(df)
    console.print(f"\n📊 Analysis Summary for {total_jobs} jobs:", style="bold blue")

    # Count antipatterns
    antipattern_cols = [
        "select_star",
        "partition_not_used",
        "big_date_range",
        "no_date_on_big_table",
        "references_cte_multiple_times",
        "semi_join_without_aggregation",
        "order_without_limit",
        "like_before_more_selective",
        "regexp_in_where",
        "queries_unpartitioned_table",
        "distinct_on_big_table",
        "count_distinct_on_big_table",
    ]

    summary_table = Table(title="Antipattern Detection Summary")
    summary_table.add_column("Antipattern", style="cyan")
    summary_table.add_column("Count", style="yellow")
    summary_table.add_column("Percentage", style="green")

    for col in antipattern_cols:
        if col in df.columns:
            count = df[col].sum() if df[col].dtype == "bool" else len(df[df[col] == True])
            percentage = f"{(count / total_jobs) * 100:.1f}%" if total_jobs > 0 else "0.0%"
            summary_table.add_row(col.replace("_", " ").title(), str(count), percentage)

    console.print(summary_table)

    # Show top problematic queries (if any)
    if total_jobs > 0:
        console.print("\n🔍 Top Issues Found:", style="bold yellow")

        # Count total antipatterns per job
        df_copy = df.copy()
        for col in antipattern_cols:
            if col in df_copy.columns:
                df_copy[col] = df_copy[col].astype(bool)

        antipattern_count = df_copy[antipattern_cols].sum(axis=1)
        df_copy["total_antipatterns"] = antipattern_count

        # Show jobs with most antipatterns
        top_issues = df_copy.nlargest(5, "total_antipatterns")

        if len(top_issues[top_issues["total_antipatterns"] > 0]) > 0:
            issues_table = Table()
            issues_table.add_column("Job ID", style="cyan")
            issues_table.add_column("User", style="yellow")
            issues_table.add_column("Antipatterns", style="red")
            issues_table.add_column("Total Slot Hours", style="green")

            for _, row in top_issues.iterrows():
                if row["total_antipatterns"] > 0:
                    issues_table.add_row(
                        str(row.get("job_id", "N/A"))[:20] + "...",
                        str(row.get("user_email", "N/A")),
                        str(int(row["total_antipatterns"])),
                        f"{row.get('total_slot_hrs', 0):.2f}",
                    )

            console.print(issues_table)
        else:
            console.print("✅ No significant antipatterns found!", style="green")


@app.command("run")
def run_antipattern_check(
    config_file: Path | None = typer.Option(
        Path("antipattern-config.yaml"),
        "--config",
        "-c",
        help="Path to the YAML configuration file",
        file_okay=True,
        dir_okay=False,
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose output"),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Save results locally instead of pushing to BigQuery"
    ),
    limit_row: int | None = typer.Option(
        None,
        "--limit-row",
        help="Limit number of rows to process (default: 100)",
    ),
    cumul_perc: float | None = typer.Option(
        1,
        "--cumul-perc",
        help="Cumulative percentage of cost incurred in project. If you want to limit number of jobs you want to process to top costing ones. If you say 0.8 it would limit to top costing jobs cumulatively making 80% of the cost of that project  (default: 1)",
    ),
    output_format: OutputFormat = typer.Option(
        OutputFormat.CONSOLE,
        "--output-format",
        help="Output format when using --dry-run (console, json, csv, parquet)",
        case_sensitive=False,
    ),
    output_file: Path | None = typer.Option(
        None, "--output-file", help="Output file path (optional, auto-generated if not specified)"
    ),
) -> None:
    """Run the BigQuery SQL antipattern checker.

    By default, results are pushed to BigQuery. Use --dry-run to save results
    locally instead. When using --dry-run, you can specify the output format:
    - console: Display results in terminal (default)
    - json: Save as JSON file
    - csv: Save as CSV file
    - parquet: Save as Parquet file
    """
    try:
        # Load configuration
        if config_file and config_file.exists():
            config = Config.from_yaml(config_file)
            console.print(f"✓ Loaded configuration from {config_file}", style="green")
        else:
            config = Config.from_env()
            console.print("✓ Using environment variables for configuration", style="yellow")

        if verbose:
            show_config(config_file)

        # Validate output format when dry-run is used
        if not dry_run and output_format != OutputFormat.CONSOLE:
            console.print("⚠️  --output-format can only be used with --dry-run", style="yellow")
            raise typer.Exit(code=1)

        # Run the antipattern check
        run_check(config, verbose, dry_run, limit_row, cumul_perc, output_format, output_file)

    except Exception as e:
        console.print(f"✗ Error: {e}", style="red")
        raise typer.Exit(code=1)


@app.command("list-antipatterns")
def list_antipatterns(
    config_file: Path | None = typer.Option(
        Path("antipattern-config.yaml"),
        "--config",
        "-c",
        help="Path to the YAML configuration file",
        file_okay=True,
        dir_okay=False,
    ),
) -> None:
    """List all available antipatterns and their status."""
    try:
        # Load configuration
        if config_file and config_file.exists():
            config = Config.from_yaml(config_file)
        else:
            config = Config.from_env()

        # Create table
        table = Table(title="Available Antipatterns")
        table.add_column("Antipattern", style="cyan")
        table.add_column("Status", style="green")
        table.add_column("Description", style="yellow")

        if config.antipatterns:
            antipatterns_items = list(config.antipatterns.items())
        else:
            antipatterns_items = []

        for name, antipattern_config in antipatterns_items:
            status = "✓ Enabled" if antipattern_config.enabled else "✗ Disabled"
            status_style = "green" if antipattern_config.enabled else "red"
            table.add_row(
                name,
                f"[{status_style}]{status}[/{status_style}]",
                antipattern_config.description or "No description available",
            )

        console.print(table)

    except Exception as e:
        console.print(f"✗ Error: {e}", style="red")
        raise typer.Exit(code=1)


@app.command("show-config")
def show_config(
    config_file: Path | None = typer.Option(
        Path("antipattern-config.yaml"),
        "--config",
        "-c",
        help="Path to the YAML configuration file",
        file_okay=True,
        dir_okay=False,
    ),
) -> None:
    """Display the current configuration settings."""
    try:
        # Load configuration
        if config_file and config_file.exists():
            config = Config.from_yaml(config_file)
            console.print(f"📄 Configuration loaded from: {config_file}", style="blue")
        else:
            config = Config.from_env()
            console.print("📄 Configuration loaded from environment variables", style="blue")

        # Display BigQuery settings
        console.print("\n🔧 BigQuery Configuration", style="bold cyan")
        bq_table = Table()
        bq_table.add_column("Setting", style="yellow")
        bq_table.add_column("Value", style="green")

        bq_table.add_row("Job Project", config.bigquery_job_project)
        bq_table.add_row("Dataset Project", config.bigquery_dataset_project)
        bq_table.add_row("Dataset", config.bigquery_dataset)
        bq_table.add_row("Region", config.bigquery_region)
        bq_table.add_row("Information Schema Project", ", ".join(config.information_schema_project))
        bq_table.add_row("Query Project", ", ".join(config.query_project))
        bq_table.add_row("Results Table", config.results_table_name)

        console.print(bq_table)

        # Display thresholds
        console.print("\n📊 Thresholds", style="bold cyan")
        threshold_table = Table()
        threshold_table.add_column("Setting", style="yellow")
        threshold_table.add_column("Value", style="green")

        threshold_table.add_row("Large Table Row Count", str(config.large_table_row_count))
        threshold_table.add_row(
            "Distinct Function Row Count", str(config.distinct_function_row_count)
        )
        threshold_table.add_row("Days Back", str(config.days_back))

        console.print(threshold_table)

        # Display date configuration
        console.print("\n📅 Date Configuration", style="bold cyan")
        date_table = Table()
        date_table.add_column("Setting", style="yellow")
        date_table.add_column("Value", style="green")
        date_table.add_row("Run Date(s)", str(config.run_dates))

        console.print(date_table)

        # Display antipatterns status
        console.print("\n🔍 Antipatterns Configuration", style="bold cyan")
        antipattern_table = Table()
        antipattern_table.add_column("Antipattern", style="cyan")
        antipattern_table.add_column("Status", style="green")
        antipattern_table.add_column("Description", style="yellow")

        if config.antipatterns:
            for name, antipattern_config in config.antipatterns.items():
                status = "✓ Enabled" if antipattern_config.enabled else "✗ Disabled"
                status_style = "green" if antipattern_config.enabled else "red"
                antipattern_table.add_row(
                    name,
                    f"[{status_style}]{status}[/{status_style}]",
                    antipattern_config.description or "No description available",
                )

        console.print(antipattern_table)

        # Summary
        enabled_count = len(config.get_enabled_antipatterns()) if config.antipatterns else 0
        total_count = len(config.antipatterns) if config.antipatterns else 0
        console.print(
            f"\n📈 Summary: {enabled_count}/{total_count} antipatterns enabled", style="bold green"
        )

    except Exception as e:
        console.print(f"✗ Error loading configuration: {e}", style="red")
        raise typer.Exit(code=1)


@app.command("create-config")
def create_config(
    output_file: Path = typer.Option(
        Path("antipattern-config.yaml"),
        "--output",
        "-o",
        help="Output file path for the configuration",
    ),
    force: bool = typer.Option(False, "--force", "-f", help="Overwrite existing file"),
) -> None:
    """Create a default configuration file."""
    try:
        if output_file.exists() and not force:
            console.print(
                f"✗ Configuration file {output_file} already exists. Use --force to overwrite.",
                style="red",
            )
            raise typer.Exit(code=1)

        # Read the default config from the package
        package_config_path = Path(__file__).parent / "antipattern-config.yaml"

        if package_config_path.exists():
            # Copy the default config
            import shutil

            shutil.copy(package_config_path, output_file)
            console.print(f"✓ Created configuration file at {output_file}", style="green")
            return

        # Fallback: Create default config content
        default_config_content = """# BigQuery SQL Antipattern Checker Configuration
# This file contains the configuration for the BQ SQL Antipattern Checker

# BigQuery Configuration
bigquery_job_project: "dev-project"  # Project where SQL commands are executed for this application
bigquery_dataset_project: "dev-dataset-project"  # Project where your results table resides
bigquery_dataset: "dev_dataset"  # Dataset where your results table resides
bigquery_region: "region-EU"  # BigQuery region (e.g., "US", "EU", "asia-northeast1")
information_schema_project:
  - "dev-dataset-project"  # Project where TABLE_STORAGE and COLUMNS views are stored
query_project:
  - "dev-project"  # Project where INFORMATION_SCHEMA.JOBS view resides

# Table Configuration
results_table_name: "antipattern_results"  # Name of the results table

# Thresholds
large_table_row_count: 1000  # Minimum row count to consider a table "large"
distinct_function_row_count: 10000  # Threshold for distinct function checks

# Date Configuration
days_back: 1  # Number of days back to check jobs (default: yesterday's jobs)

# Antipattern Configuration
# Each antipattern can be enabled/disabled and optionally have a description
antipatterns:
  select_star:
    enabled: true
    description: "Check for SELECT * statements that can impact performance"
  
  semi_join_without_aggregation:
    enabled: true
    description: "Check for semi-joins without proper aggregation"
  
  order_without_limit:
    enabled: true
    description: "Check for ORDER BY clauses without LIMIT"
  
  regexp_in_where:
    enabled: true
    description: "Check for expensive REGEXP functions in WHERE clauses"
  
  like_before_more_selective:
    enabled: true
    description: "Check for LIKE conditions placed before more selective conditions"
  
  multiple_cte_reference:
    enabled: true
    description: "Check for CTEs that are referenced multiple times (may cause re-evaluation)"
  
  partition_used:
    enabled: true
    description: "Check if partitioned tables are properly filtered by partition key"
  
  big_date_range:
    enabled: true
    description: "Check for date ranges larger than 365 days"
  
  big_table_no_date:
    enabled: true
    description: "Check for queries on large tables without date filters"
  
  unpartitioned_tables:
    enabled: true
    description: "Check for queries on large unpartitioned tables"
  
  distinct_on_big_table:
    enabled: true
    description: "Check for DISTINCT operations on large tables"
  
  count_distinct_on_big_table:
    enabled: true
    description: "Check for COUNT DISTINCT operations on large tables"
"""

        # Write the default config
        with open(output_file, "w") as f:
            f.write(default_config_content)
        console.print(f"✓ Created configuration file at {output_file}", style="green")

    except Exception as e:
        console.print(f"✗ Error creating configuration: {e}", style="red")
        raise typer.Exit(code=1)


def run_check(
    config: Config,
    verbose: bool = False,
    dry_run: bool = False,
    limit_row: int | None = None,
    cumul_perc: float | None = None,
    output_format: OutputFormat = OutputFormat.CONSOLE,
    output_file: Path | None = None,
) -> None:
    """Run the antipattern check with the given configuration."""
    start = time.perf_counter()
    console.print(
        f"🔍 Checking Jobs Ran On: {config.date_values['query_run_date_str']}", style="blue"
    )

    # Get columns dictionary
    if verbose:
        console.print("📊 Fetching column information...", style="blue")

    columns_dict = functions.get_columns_dict(config)

    # Get jobs dictionary
    if verbose:
        console.print("📋 Fetching job information...", style="blue")

    jobs_dict = functions.get_jobs_dict(config, limit_row, cumul_perc)

    console.print(f"📈 Jobs Found: {len(jobs_dict)}", style="green")

    # Process jobs
    job_output = {}
    processed_jobs = 0
    antipatterns = Antipatterns(config)
    for k, v in jobs_dict.items():
        job_id = v["job_id"]
        job = Job(v, antipatterns)
        job.check_antipatterns(columns_dict, config)

        job_output[job_id] = job.__dict__
        del job_output[job_id][
            "antipatterns"
        ]  # antipatterns object needs to be removed to convert dataframe
        processed_jobs += 1

        if verbose and processed_jobs % 100 == 0:
            console.print(f"Processed {processed_jobs}/{len(jobs_dict)} jobs", style="blue")

    # Prepare output DataFrame
    if verbose:
        console.print("📊 Preparing output data...", style="blue")

    job_output_df = functions.get_output_df(job_output, "job_id")

    # Handle output - either push to BigQuery or save locally
    if dry_run:
        if verbose:
            console.print("💾 Saving results locally...", style="blue")

        save_results_locally(job_output_df, output_format, output_file, config)
    else:
        if verbose:
            console.print("📤 Pushing results to BigQuery...", style="blue")

        functions.push_df_to_bq(job_output_df, config)

    end = time.perf_counter()
    elapsed = end - start

    console.print(f"✅ Analysis complete! Time taken: {elapsed:.2f} seconds", style="green")

    if dry_run:
        console.print(f"📁 Results saved locally in {output_format.value} format", style="blue")
    else:
        console.print(
            f"📊 Results pushed to: {config.bigquery_dataset_project}.{config.bigquery_dataset}.{config.table_names['results']}",
            style="blue",
        )


def main() -> None:
    """Main entry point for the CLI application."""
    app()


if __name__ == "__main__":
    main()
