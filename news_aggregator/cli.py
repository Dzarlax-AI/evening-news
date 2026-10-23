"""Command line interface for Evening News v2."""

import asyncio
import sys
from typing import Optional

import click
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn

from .orchestrator import NewsOrchestrator
from .services.source_manager import SourceManager
from .database import AsyncSessionLocal
from .config import settings


console = Console()


def async_command(f):
    """Decorator to run async CLI commands."""
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))
    return wrapper


@click.group()
@click.version_option(version="2.0.0")
def cli():
    """Evening News v2 - Modern news aggregator."""
    pass


@cli.command()
@click.option('--verbose', '-v', is_flag=True, help='Verbose output')
def process(verbose: bool):
    """Run the main news processing cycle."""
    async def _process():
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            
            task = progress.add_task("Processing news...", total=None)
            
            try:
                if settings.background_execution_mode == "worker":
                    import secrets
                    from .services.background_jobs import enqueue_job
                    job = await enqueue_job("news_processing", {}, secrets.token_hex(24))
                    progress.update(task, description="✅ Processing queued")
                    console.print(f"Background job accepted: {job.id} (status: {job.state})")
                    return
                orchestrator = NewsOrchestrator()
                stats = await orchestrator.run_full_cycle()
                progress.update(task, description="✅ Processing completed!")
            
                # Display results
                console.print("\n[bold green]Processing Summary:[/bold green]")
                console.print(f"• Articles fetched: {stats['articles_fetched']}")
                console.print(f"• Articles processed: {stats['articles_processed']}")
                console.print(f"• API calls made: {stats['api_calls_made']}")
                console.print(f"• Duration: {stats.get('duration_seconds', 0):.1f}s")
                
                if stats['errors']:
                    console.print(f"\n[bold yellow]Errors ({len(stats['errors'])}):[/bold yellow]")
                    for error in stats['errors'][:5]:  # Show first 5 errors
                        console.print(f"• {error}")
                    if len(stats['errors']) > 5:
                        console.print(f"• ... and {len(stats['errors']) - 5} more")
                
            except Exception as e:
                progress.update(task, description="❌ Processing failed!")
                console.print(f"\n[bold red]Error:[/bold red] {e}")
                sys.exit(1)
    
    # Run the async function
    asyncio.run(_process())


@cli.command()
@click.option('--url', required=True, help='Article URL to extract')
@async_command
async def extract_url(url: str):
    """Extract article content and metadata from a single URL."""
    try:
        from .extraction import ContentExtractor
        console.print(f"[cyan]Extracting:[/cyan] {url}")
        async with ContentExtractor() as extractor:
            result = await extractor.extract_article_content_with_metadata(url)
        content = result.get('content')
        pub_date = result.get('publication_date')
        full_url = result.get('full_article_url')
        # Print short summary
        preview = (content[:400] + '...') if content and len(content) > 400 else (content or '')
        table = Table(title="Extraction Result")
        table.add_column("Field", style="cyan")
        table.add_column("Value")
        table.add_row("Publication Date", str(pub_date))
        table.add_row("Full Article URL", str(full_url))
        table.add_row("Content Preview", preview)
        console.print(table)
    except Exception as e:
        console.print(f"[red]❌ Extraction failed:[/red] {e}")
        sys.exit(1)


@cli.command()
@async_command
async def sources():
    """Manage news sources."""
    source_manager = SourceManager()
    
    async with AsyncSessionLocal() as db:
        sources_list = await source_manager.get_sources(db)
        
        if not sources_list:
            console.print("[yellow]No sources configured.[/yellow]")
            return
        
        # Create table
        table = Table(title="News Sources")
        table.add_column("ID", style="cyan")
        table.add_column("Name", style="bold")
        table.add_column("Type", style="magenta")
        table.add_column("Status", style="green")
        table.add_column("Last Fetch", style="dim")
        table.add_column("Errors", style="red")
        
        for source in sources_list:
            status = "✅ Enabled" if source.enabled else "❌ Disabled"
            last_fetch = source.last_fetch.strftime("%Y-%m-%d %H:%M") if source.last_fetch else "Never"
            errors = str(source.error_count) if source.error_count > 0 else "-"
            
            table.add_row(
                str(source.id),
                source.name,
                source.source_type,
                status,
                last_fetch,
                errors
            )
        
        console.print(table)


@cli.command()
@click.argument('name')
@click.argument('source_type', type=click.Choice(['rss', 'telegram', 'custom']))
@click.argument('url')
async def add_source(name: str, source_type: str, url: str):
    """Add a new news source."""
    source_manager = SourceManager()
    
    try:
        async with AsyncSessionLocal() as db:
            worker_mode = settings.background_execution_mode == "worker"
            source = await source_manager.create_source(
                db, name, source_type, url, skip_connection_test=worker_mode
            )
            
            if worker_mode:
                console.print(f"[green]✅ Added source '{name}'. Use `news test-source {source.id}` to queue its connection test.[/green]")
            elif source.enabled:
                console.print(f"[green]✅ Added source '{name}' successfully![/green]")
            else:
                console.print(f"[yellow]⚠️ Added source '{name}' but connection test failed.[/yellow]")
                console.print(f"Error: {source.last_error}")
    
    except Exception as e:
        console.print(f"[red]❌ Failed to add source: {e}[/red]")
        sys.exit(1)


@cli.command()
@click.argument('source_id', type=int)
async def test_source(source_id: int):
    """Test connection to a source."""
    if settings.background_execution_mode == "worker":
        from sqlalchemy import select
        from .models import Source
        from .services.background_jobs import enqueue_job
        async with AsyncSessionLocal() as db:
            exists = await db.scalar(select(Source.id).where(Source.id == source_id))
        if exists is None:
            console.print(f"[red]Source {source_id} was not found.[/red]")
            sys.exit(1)
        import secrets
        job = await enqueue_job("source_test", {"source_id": source_id}, secrets.token_hex(24))
        console.print(f"Source test queued: {job.id}")
        return
    source_manager = SourceManager()
    
    try:
        async with AsyncSessionLocal() as db:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
            ) as progress:
                task = progress.add_task(f"Testing source {source_id}...", total=None)
                
                is_connected = await source_manager.test_source_connection(db, source_id)
                
                if is_connected:
                    progress.update(task, description="✅ Connection successful!")
                    console.print(f"[green]Source {source_id} is working correctly.[/green]")
                else:
                    progress.update(task, description="❌ Connection failed!")
                    console.print(f"[red]Source {source_id} connection failed.[/red]")
    
    except Exception as e:
        console.print(f"[red]❌ Error testing source: {e}[/red]")
        sys.exit(1)


@cli.command()
@click.option('--days', '-d', default=7, help='Number of days to show stats for')
async def stats(days: int):
    """Show processing statistics."""
    orchestrator = None if settings.background_execution_mode == "worker" else NewsOrchestrator()
    
    try:
        if orchestrator is not None:
            stats_data = await orchestrator.get_processing_stats(days)
        else:
            from .processing.processing_stats_service import get_processing_stats_service
            stats_data = await get_processing_stats_service().get_processing_stats(days)
        
        # Show totals
        totals = stats_data['totals']
        console.print(f"[bold blue]Statistics for last {days} days:[/bold blue]")
        console.print(f"• Articles fetched: {totals['articles_fetched']}")
        console.print(f"• Articles processed: {totals['articles_processed']}")
        console.print(f"• API calls made: {totals['api_calls_made']}")
        console.print(f"• Total processing time: {totals['total_processing_time']}s")
        
        if totals['errors_count'] > 0:
            console.print(f"• [red]Errors: {totals['errors_count']}[/red]")
        
        # Show daily breakdown
        if stats_data['daily_stats']:
            console.print(f"\n[bold]Daily breakdown:[/bold]")
            table = Table()
            table.add_column("Date")
            table.add_column("Articles", justify="right")
            table.add_column("API Calls", justify="right")
            table.add_column("Errors", justify="right", style="red")
            
            for day_stat in stats_data['daily_stats'][:10]:  # Show last 10 days
                table.add_row(
                    day_stat['date'],
                    str(day_stat['articles_fetched']),
                    str(day_stat['api_calls_made']),
                    str(day_stat['errors_count']) if day_stat['errors_count'] > 0 else "-"
                )
            
            console.print(table)
    
    except Exception as e:
        console.print(f"[red]❌ Error getting stats: {e}[/red]")
        sys.exit(1)


@cli.command()
async def config():
    """Show current configuration."""
    console.print("[bold blue]Evening News v2 Configuration:[/bold blue]")
    
    # Show key settings (without secrets)
    table = Table()
    table.add_column("Setting", style="cyan")
    table.add_column("Value", style="white")
    
    config_items = [
        ("Database URL", settings.database_url),
        ("Gemini API Endpoint", settings.gemini_api_endpoint),
        ("API Rate Limit", f"{settings.api_rate_limit} req/sec"),
        ("Log Level", settings.log_level),
        ("Max Workers", str(settings.max_workers)),
        ("Cache TTL", f"{settings.cache_ttl}s"),
        ("Cache Directory", settings.cache_dir),
        ("Development Mode", "Yes" if settings.development else "No"),
    ]
    
    for key, value in config_items:
        # Hide sensitive values
        if "token" in key.lower() or "secret" in key.lower() or "password" in key.lower():
            value = "***hidden***" if value else "Not configured"
        
        table.add_row(key, str(value))
    
    console.print(table)


@cli.command()
@click.option('--limit', default=50, help='Maximum number of articles to process')
@click.option('--dry-run', is_flag=True, help='Only show candidates without processing')
async def reprocess_failed(limit: int, dry_run: bool):
    """Process due pending and retry-wait articles through the bounded pipeline."""
    if settings.background_execution_mode == "worker":
        import secrets
        from .services.background_jobs import enqueue_job
        job = await enqueue_job(
            "reprocess_failed", {"limit": limit, "dry_run": bool(dry_run)}, secrets.token_hex(24)
        )
        console.print(f"Failed article reprocessing queued: {job.id}")
        return
    orchestrator = NewsOrchestrator()
    
    try:
        await orchestrator.start()
        
        if dry_run:
            console.print(f"[bold yellow]🔍 Dry run: Finding due articles to process (limit: {limit})[/bold yellow]")
        else:
            console.print(f"[bold green]🔧 Processing due articles (limit: {limit})[/bold green]")
        
        # Run the reprocessing
        results = await orchestrator.reprocess_failed_extractions(limit=limit, dry_run=dry_run)
        
        # Display results in a nice table
        console.print(f"\n[bold blue]📊 Results:[/bold blue]")
        
        # Summary table
        summary_table = Table()
        summary_table.add_column("Metric", style="cyan")
        summary_table.add_column("Value", style="white")
        
        if dry_run:
            candidate_count = results.get('found_candidates', results.get('total_candidates'))
            if candidate_count is not None:
                summary_table.add_row("Found candidates", str(candidate_count))
        else:
            completed = results.get('processed', results.get('articles_processed', 0))
            summaries = results.get('articles_summarized', results.get('improved'))
            summary_table.add_row("Completed", str(completed))
            if summaries is not None:
                summary_table.add_row("Summaries generated", str(summaries))
            summary_table.add_row("Errors", str(len(results.get('errors') or [])))
        if results.get('status'):
            summary_table.add_row("Status", str(results['status']))
        
        console.print(summary_table)
        
        # Show improvements if any
        if not dry_run and results.get('improvements'):
            console.print(f"\n[bold green]✅ Articles with improved content:[/bold green]")
            
            improvements_table = Table()
            improvements_table.add_column("ID", style="cyan")
            improvements_table.add_column("Title", style="white", max_width=50)
            improvements_table.add_column("Original", style="red")
            improvements_table.add_column("New", style="green")
            improvements_table.add_column("Improvement", style="yellow")
            
            for improvement in results['improvements'][:10]:  # Show top 10
                improvements_table.add_row(
                    str(improvement['article_id']),
                    improvement['title'],
                    f"{improvement['original_length']} chars",
                    f"{improvement['new_length']} chars",
                    f"+{improvement['improvement']} (+{improvement['percentage']:.1f}%)"
                )
            
            console.print(improvements_table)
            
            if len(results['improvements']) > 10:
                console.print(f"[dim]... and {len(results['improvements']) - 10} more improvements[/dim]")
        
        # Show candidates in dry run mode
        if dry_run and results.get('candidates'):
            console.print(f"\n[bold yellow]🎯 Reprocessing candidates:[/bold yellow]")
            
            candidates_table = Table()
            candidates_table.add_column("ID", style="cyan")
            candidates_table.add_column("Title", style="white", max_width=50)
            candidates_table.add_column("Domain", style="blue")
            candidates_table.add_column("Content", style="red")
            
            for candidate in results['candidates'][:15]:  # Show top 15
                candidates_table.add_row(
                    str(candidate['id']),
                    candidate['title'],
                    candidate['domain'],
                    f"{candidate['content_length']} chars"
                )
            
            console.print(candidates_table)
            
            if len(results['candidates']) > 15:
                console.print(f"[dim]... and {len(results['candidates']) - 15} more candidates[/dim]")
            
            console.print(f"\n[bold yellow]💡 Run without --dry-run to process these articles[/bold yellow]")
        
        # Show errors if any
        if not dry_run and results.get('errors'):
            console.print(f"\n[bold red]❌ Processing errors:[/bold red]")
            
            for error in results['errors'][:5]:  # Show first 5 errors
                if isinstance(error, dict):
                    message = error.get('error', error)
                    article_id = error.get('article_id')
                    prefix = f"Article {article_id}: " if article_id is not None else ""
                    console.print(f"   {prefix}{message}")
                else:
                    console.print(f"   {error}")
            
            if len(results['errors']) > 5:
                console.print(f"   [dim]... and {len(results['errors']) - 5} more errors[/dim]")
        
    except Exception as e:
        console.print(f"[bold red]❌ Error during reprocessing: {e}[/bold red]")
        raise
    finally:
        await orchestrator.stop()


@cli.command()
@click.argument('ai_category')
@click.argument('new_fixed_category')
@click.option('--description', help='Description for the mapping change')
async def update_mapping(ai_category: str, new_fixed_category: str, description: str):
    """Update a category mapping and apply changes to existing articles."""
    try:
        from .services.category_service import CategoryService
        
        async with AsyncSessionLocal() as db:
            service = CategoryService(db)
            
            # Validate fixed category
            if new_fixed_category not in service.ALLOWED_CATEGORIES:
                console.print(f"[red]❌ Invalid fixed category: {new_fixed_category}[/red]")
                console.print(f"[yellow]Allowed categories: {list(service.ALLOWED_CATEGORIES.keys())}[/yellow]")
                sys.exit(1)
            
            console.print(f"[cyan]🔄 Updating mapping: {ai_category} → {new_fixed_category}[/cyan]")
            
            # Apply the mapping change
            success = await service.update_category_mapping(
                ai_category=ai_category,
                new_fixed_category=new_fixed_category,
                description=description or f"CLI update: {ai_category} → {new_fixed_category}"
            )
            
            if success:
                console.print(f"[green]✅ Successfully updated mapping and applied to existing articles[/green]")
            else:
                console.print(f"[red]❌ Failed to update mapping[/red]")
                sys.exit(1)
    
    except Exception as e:
        console.print(f"[red]❌ Error updating mapping: {e}[/red]")
        sys.exit(1)






# Async command support
def async_command(f):
    """Decorator to support async click commands."""
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))
    return wrapper


# Apply async decorator to all async commands
process = async_command(process)
sources = async_command(sources)
add_source = async_command(add_source)
test_source = async_command(test_source)
stats = async_command(stats)
config = async_command(config)
reprocess_failed = async_command(reprocess_failed)
update_mapping = async_command(update_mapping)


if __name__ == '__main__':
    cli()
