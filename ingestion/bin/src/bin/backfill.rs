use chrono::NaiveDate;
use clap::Parser;
use ingestion_application::backfill_service::BackfillService;
use shaku::HasComponent;
use std::sync::Arc;

mod di {
    include!("../di.rs");
}

#[derive(Parser)]
#[command(name = "backfill")]
#[command(about = "Backfill historical tick data", long_about = None)]
struct Cli {
    #[arg(long)]
    symbol: String,

    #[arg(short, long)]
    start_date: String,

    #[arg(short, long)]
    end_date: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    let start_date = NaiveDate::parse_from_str(&cli.start_date, "%Y-%m-%d")?;
    let end_date = NaiveDate::parse_from_str(&cli.end_date, "%Y-%m-%d")?;

    let range = ingestion_domain::DateRange::new(start_date, end_date)?;

    println!("Starting backfill for {} from {} to {}",
        cli.symbol, start_date, end_date);

    let module = di::create_app_module();
    let service: Arc<dyn BackfillService> = module.resolve();

    let report = service.backfill_range(&cli.symbol, range).await?;

    println!("\nBackfill completed:");
    println!("  Symbol: {}", report.symbol);
    println!("  Days processed: {}", report.days_processed);
    println!("  Total ticks: {}", report.total_ticks);

    if !report.failed_days.is_empty() {
        println!("\n  Failed days:");
        for (date, error) in &report.failed_days {
            println!("    {} - {}", date, error);
        }
    }

    Ok(())
}