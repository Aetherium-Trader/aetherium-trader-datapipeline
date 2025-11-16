use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;
use std::path::Path;

/// 驗證 Parquet 檔案內容
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = "./data/NQ_20251114_04.parquet";

    println!("Verifying Parquet file: {}", file_path);

    let file = File::open(Path::new(file_path))?;
    let reader = SerializedFileReader::new(file)?;

    let metadata = reader.metadata();
    println!("\n📊 File Metadata:");
    println!("  - Version: {}", metadata.file_metadata().version());
    println!("  - Num rows: {}", metadata.file_metadata().num_rows());
    println!("  - Num row groups: {}", metadata.num_row_groups());

    println!("\n📋 Schema:");
    println!("{:?}", metadata.file_metadata().schema());

    println!("\n📦 Row Groups:");
    for (i, rg) in metadata.row_groups().iter().enumerate() {
        println!("  Row Group {}:", i);
        println!("    - Num rows: {}", rg.num_rows());
        println!("    - Total byte size: {} bytes", rg.total_byte_size());
    }

    println!("\n✅ Parquet file is valid!");
    Ok(())
}