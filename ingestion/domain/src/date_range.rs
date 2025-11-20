use chrono::{Days, NaiveDate};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DateRange {
    start: NaiveDate,
    end: NaiveDate,
}

impl DateRange {
    pub fn new(start: NaiveDate, end: NaiveDate) -> Result<Self, DateRangeError> {
        if start > end {
            return Err(DateRangeError::StartAfterEnd);
        }
        Ok(Self { start, end })
    }

    pub fn single_day(date: NaiveDate) -> Self {
        Self {
            start: date,
            end: date,
        }
    }

    pub fn start(&self) -> NaiveDate {
        self.start
    }

    pub fn end(&self) -> NaiveDate {
        self.end
    }

    pub fn days(&self) -> u32 {
        (self.end - self.start).num_days() as u32 + 1
    }

    pub fn contains(&self, date: NaiveDate) -> bool {
        date >= self.start && date <= self.end
    }

    pub fn overlaps(&self, other: &DateRange) -> bool {
        self.start <= other.end && self.end >= other.start
    }

    pub fn split_by_days(self) -> Vec<DateRange> {
        let mut result = Vec::new();
        let mut current = self.start;

        while current <= self.end {
            result.push(DateRange::single_day(current));
            current = current
                .checked_add_days(Days::new(1))
                .expect("Date overflow should not happen in valid range");
        }

        result
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DateRangeError {
    #[error("Start date must be before or equal to end date")]
    StartAfterEnd,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn test_valid_date_range() {
        let start = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2025, 1, 10).unwrap();
        let range = DateRange::new(start, end).unwrap();

        assert_eq!(range.days(), 10);
    }

    #[test]
    fn test_invalid_date_range() {
        let start = NaiveDate::from_ymd_opt(2025, 1, 10).unwrap();
        let end = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();

        assert!(matches!(
            DateRange::new(start, end),
            Err(DateRangeError::StartAfterEnd)
        ));
    }

    #[test]
    fn test_split_by_days() {
        let start = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2025, 1, 3).unwrap();
        let range = DateRange::new(start, end).unwrap();

        let days = range.split_by_days();
        assert_eq!(days.len(), 3);
        assert_eq!(
            days[0].start(),
            NaiveDate::from_ymd_opt(2025, 1, 1).unwrap()
        );
        assert_eq!(
            days[2].start(),
            NaiveDate::from_ymd_opt(2025, 1, 3).unwrap()
        );
    }
}
