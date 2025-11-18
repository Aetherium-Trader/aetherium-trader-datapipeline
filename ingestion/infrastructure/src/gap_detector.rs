use async_trait::async_trait;
use chrono::NaiveDate;
use ingestion_application::{GapDetectionError, GapDetector};
use ingestion_domain::DateRange;
use parquet::file::reader::{FileReader, SerializedFileReader};
use shaku::Component;
use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;

#[derive(Component)]
#[shaku(interface = GapDetector)]
pub struct ParquetGapDetector {
    data_dir: PathBuf,
}

impl ParquetGapDetector {
    fn get_existing_dates(&self, symbol: &str) -> Result<HashSet<NaiveDate>, GapDetectionError> {
        let mut dates = HashSet::new();

        let entries = fs::read_dir(&self.data_dir)?;

        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            if !path.is_file() {
                continue;
            }

            let filename = match path.file_name().and_then(|n| n.to_str()) {
                Some(name) => name,
                None => continue,
            };

            if !filename.ends_with(".parquet") {
                continue;
            }

            if !filename.starts_with(&format!("{}_", symbol)) {
                continue;
            }

            let parts: Vec<&str> = filename.trim_end_matches(".parquet").split('_').collect();
            if parts.len() != 3 {
                continue;
            }

            let date_str = parts[1];
            if date_str.len() != 8 {
                continue;
            }

            let year = date_str[0..4].parse::<i32>().ok();
            let month = date_str[4..6].parse::<u32>().ok();
            let day = date_str[6..8].parse::<u32>().ok();

            if let (Some(y), Some(m), Some(d)) = (year, month, day) {
                if let Some(date) = NaiveDate::from_ymd_opt(y, m, d) {
                    if Self::file_has_data(&path)? {
                        dates.insert(date);
                    }
                }
            }
        }

        Ok(dates)
    }

    fn file_has_data(path: &PathBuf) -> Result<bool, GapDetectionError> {
        let file = fs::File::open(path)?;
        let reader = SerializedFileReader::new(file)
            .map_err(|e| GapDetectionError::IoError(
                std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
            ))?;

        let metadata = reader.metadata();
        let num_rows: i64 = metadata.file_metadata().num_rows();

        Ok(num_rows > 0)
    }
}

#[async_trait]
impl GapDetector for ParquetGapDetector {
    async fn detect_gaps(
        &self,
        symbol: &str,
        range: DateRange,
    ) -> Result<Vec<DateRange>, GapDetectionError> {
        if range.start() > range.end() {
            return Err(GapDetectionError::InvalidDateRange);
        }

        let existing_dates = self.get_existing_dates(symbol)?;
        let existing_vec: Vec<NaiveDate> = existing_dates.into_iter().collect();

        let gaps = ingestion_domain::detect_gaps(symbol, range, &existing_vec);

        Ok(gaps.into_iter().map(|g| g.range().clone()).collect())
    }
}