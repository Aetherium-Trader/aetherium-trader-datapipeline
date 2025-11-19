use crate::DateRange;
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataGap {
    range: DateRange,
    symbol: String,
}

impl DataGap {
    pub fn new(symbol: String, range: DateRange) -> Self {
        Self { symbol, range }
    }

    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    pub fn range(&self) -> &DateRange {
        &self.range
    }

    pub fn days(&self) -> u32 {
        self.range.days()
    }
}

pub fn detect_gaps(
    symbol: &str,
    expected_range: DateRange,
    existing_dates: &[NaiveDate],
) -> Vec<DataGap> {
    let mut gaps = Vec::new();
    let mut current_gap_start: Option<NaiveDate> = None;

    for day in expected_range.clone().split_by_days() {
        let date = day.start();
        let exists = existing_dates.contains(&date);

        match (exists, current_gap_start) {
            (false, None) => {
                current_gap_start = Some(date);
            }
            (true, Some(gap_start)) => {
                let gap_end = date.pred_opt().expect("Date underflow");
                let gap_range =
                    DateRange::new(gap_start, gap_end).expect("Gap range should be valid");
                gaps.push(DataGap::new(symbol.to_string(), gap_range));
                current_gap_start = None;
            }
            _ => {}
        }
    }

    if let Some(gap_start) = current_gap_start {
        let gap_range = DateRange::new(gap_start, expected_range.end())
            .expect("Final gap range should be valid");
        gaps.push(DataGap::new(symbol.to_string(), gap_range));
    }

    gaps
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn test_no_gaps() {
        let expected = DateRange::new(
            NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 5).unwrap(),
        )
        .unwrap();

        let existing: Vec<NaiveDate> = (1..=5)
            .map(|d| NaiveDate::from_ymd_opt(2025, 1, d).unwrap())
            .collect();

        let gaps = detect_gaps("NQ", expected, &existing);
        assert_eq!(gaps.len(), 0);
    }

    #[test]
    fn test_single_gap() {
        let expected = DateRange::new(
            NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 5).unwrap(),
        )
        .unwrap();

        let existing = vec![
            NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 2).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 5).unwrap(),
        ];

        let gaps = detect_gaps("NQ", expected, &existing);
        assert_eq!(gaps.len(), 1);
        assert_eq!(
            gaps[0].range().start(),
            NaiveDate::from_ymd_opt(2025, 1, 3).unwrap()
        );
        assert_eq!(
            gaps[0].range().end(),
            NaiveDate::from_ymd_opt(2025, 1, 4).unwrap()
        );
    }

    #[test]
    fn test_multiple_gaps() {
        let expected = DateRange::new(
            NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 10).unwrap(),
        )
        .unwrap();

        let existing = vec![
            NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 5).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 10).unwrap(),
        ];

        let gaps = detect_gaps("NQ", expected, &existing);
        assert_eq!(gaps.len(), 2);
    }
}
