use arrow::array::{
    ArrayRef, Decimal128Array, RecordBatch, StringArray, TimestampMicrosecondArray, UInt32Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use ingestion_application::ports::{RepositoryError, TickRepository};
use ingestion_domain::Tick;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use rust_decimal::prelude::ToPrimitive;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use shaku::Component;
use tokio::sync::Mutex;
use tracing::{info, warn};

#[derive(Component)]
#[shaku(interface = TickRepository)]
pub struct ParquetTickRepository {
    output_dir: PathBuf,
    writer: Arc<Mutex<Option<ArrowWriter<File>>>>,
    current_hour: Arc<Mutex<Option<DateTime<Utc>>>>,
}

impl ParquetTickRepository {
    fn create_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new("symbol", DataType::Utf8, false),
            Field::new("bid_price", DataType::Decimal128(10, 4), false),
            Field::new("bid_size", DataType::UInt32, false),
            Field::new("ask_price", DataType::Decimal128(10, 4), false),
            Field::new("ask_size", DataType::UInt32, false),
            Field::new("last_price", DataType::Decimal128(10, 4), false),
            Field::new("last_size", DataType::UInt32, false),
        ]))
    }

    fn generate_file_path(&self, symbol: &str, timestamp: DateTime<Utc>) -> PathBuf {
        let filename = format!("{}_{}.parquet", symbol, timestamp.format("%Y%m%d_%H"));
        self.output_dir.join(filename)
    }

    fn should_rotate(&self, current: DateTime<Utc>, last: Option<DateTime<Utc>>) -> bool {
        match last {
            None => true,
            Some(last) => {
                current.format("%Y%m%d%H").to_string() != last.format("%Y%m%d%H").to_string()
            }
        }
    }

    async fn rotate_writer(
        &self,
        symbol: &str,
        timestamp: DateTime<Utc>,
    ) -> Result<(), RepositoryError> {
        // 關閉舊 writer
        let mut writer_guard = self.writer.lock().await;
        if let Some(writer) = writer_guard.take() {
            writer
                .close()
                .map_err(|e| RepositoryError::FileRotationError(e.to_string()))?;
            info!("Closed previous parquet file");
        }

        let file_path = self.generate_file_path(symbol, timestamp);
        info!("Creating new parquet file: {}", file_path.display());

        let file = File::create(&file_path)?;
        let schema = Self::create_schema();
        let props = WriterProperties::builder().build();

        let new_writer = ArrowWriter::try_new(file, schema, Some(props))
            .map_err(|e| RepositoryError::SerializationError(e.to_string()))?;

        *writer_guard = Some(new_writer);
        *self.current_hour.lock().await = Some(timestamp);

        Ok(())
    }

    fn ticks_to_record_batch(ticks: &[Tick]) -> Result<RecordBatch, RepositoryError> {
        let schema = Self::create_schema();

        let timestamps: Vec<i64> = ticks
            .iter()
            .map(|t| t.timestamp().timestamp_micros())
            .collect();

        let symbols: Vec<&str> = ticks.iter().map(|t| t.symbol()).collect();

        let bid_prices: Vec<i128> = ticks
            .iter()
            .map(|t| (t.bid_price().to_f64().unwrap() * 10000.0) as i128)
            .collect();

        let bid_sizes: Vec<u32> = ticks.iter().map(|t| t.bid_size()).collect();

        let ask_prices: Vec<i128> = ticks
            .iter()
            .map(|t| (t.ask_price().to_f64().unwrap() * 10000.0) as i128)
            .collect();

        let ask_sizes: Vec<u32> = ticks.iter().map(|t| t.ask_size()).collect();

        let last_prices: Vec<i128> = ticks
            .iter()
            .map(|t| (t.last_price().to_f64().unwrap() * 10000.0) as i128)
            .collect();

        let last_sizes: Vec<u32> = ticks.iter().map(|t| t.last_size()).collect();

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(TimestampMicrosecondArray::from(timestamps).with_timezone("UTC")),
            Arc::new(StringArray::from(symbols)),
            Arc::new(
                Decimal128Array::from(bid_prices)
                    .with_precision_and_scale(10, 4)
                    .unwrap(),
            ),
            Arc::new(UInt32Array::from(bid_sizes)),
            Arc::new(
                Decimal128Array::from(ask_prices)
                    .with_precision_and_scale(10, 4)
                    .unwrap(),
            ),
            Arc::new(UInt32Array::from(ask_sizes)),
            Arc::new(
                Decimal128Array::from(last_prices)
                    .with_precision_and_scale(10, 4)
                    .unwrap(),
            ),
            Arc::new(UInt32Array::from(last_sizes)),
        ];

        RecordBatch::try_new(schema, arrays)
            .map_err(|e| RepositoryError::SerializationError(e.to_string()))
    }
}

#[async_trait]
impl TickRepository for ParquetTickRepository {
    async fn save_batch(&self, ticks: Vec<Tick>) -> Result<(), RepositoryError> {
        if ticks.is_empty() {
            warn!("Attempted to save empty batch, skipping");
            return Ok(());
        }

        let first_tick = &ticks[0];
        let symbol = first_tick.symbol();
        let timestamp = first_tick.timestamp();

        // 檢查是否需要滾動
        let last_hour = *self.current_hour.lock().await;
        if self.should_rotate(timestamp, last_hour) {
            self.rotate_writer(symbol, timestamp).await?;
        }

        // 轉換為 RecordBatch
        let batch = Self::ticks_to_record_batch(&ticks)?;

        // 寫入
        let mut writer_guard = self.writer.lock().await;
        if let Some(writer) = writer_guard.as_mut() {
            writer
                .write(&batch)
                .map_err(|e| RepositoryError::SerializationError(e.to_string()))?;
            info!("Wrote {} ticks to parquet", ticks.len());
        } else {
            return Err(RepositoryError::SerializationError(
                "Writer not initialized".to_string(),
            ));
        }

        Ok(())
    }

    async fn flush(&self) -> Result<(), RepositoryError> {
        let mut writer_guard = self.writer.lock().await;
        if let Some(writer) = writer_guard.as_mut() {
            writer
                .flush()
                .map_err(|e| RepositoryError::SerializationError(e.to_string()))?;
            info!("Flushed parquet writer");
        }
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), RepositoryError> {
        let mut writer_guard = self.writer.lock().await;
        if let Some(writer) = writer_guard.take() {
            writer
                .close()
                .map_err(|e| RepositoryError::SerializationError(e.to_string()))?;
            info!("Shutdown: Closed parquet writer");
        }
        Ok(())
    }
}
