//! Turns several venues' quotes for one asset into one price.

use rust_decimal::Decimal;

use crate::config::SourceKind;

#[derive(Debug, Clone, PartialEq)]
pub struct Aggregate {
    pub price: Decimal,
    /// Sources that agreed with the median and went into the final price.
    pub used: Vec<SourceKind>,
    /// Sources dropped for sitting too far from the median, with their quote.
    pub outliers: Vec<(SourceKind, Decimal)>,
    /// Every quote considered, so each source can be compared on its own later.
    pub quotes: Vec<(SourceKind, Decimal)>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggregateError {
    /// Fewer quotes than `min_sources`, before outliers are dropped.
    TooFewQuotes { have: usize, need: usize },
    /// Enough quotes, but too few agree with each other.
    TooFewAgree { agree: usize, need: usize },
}

impl std::fmt::Display for AggregateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AggregateError::TooFewQuotes { have, need } => {
                write!(f, "{have} fresh quotes, need {need}")
            }
            AggregateError::TooFewAgree { agree, need } => {
                write!(f, "{agree} quotes agree with the median, need {need}")
            }
        }
    }
}

/// Median of every quote, then the median again of those within `max_deviation_bps` of it.
///
/// Dropping outliers before the final median means one venue printing a bad price can't
/// move the result, and requiring `min_sources` agreeing venues means two venues that
/// disagree badly produce no price at all instead of a coin flip.
pub fn aggregate(
    quotes: &[(SourceKind, Decimal)],
    max_deviation_bps: u32,
    min_sources: usize,
) -> Result<Aggregate, AggregateError> {
    if quotes.len() < min_sources {
        return Err(AggregateError::TooFewQuotes {
            have: quotes.len(),
            need: min_sources,
        });
    }
    let first = median(quotes.iter().map(|(_, p)| *p).collect()).expect("non-empty");
    let limit = first * Decimal::from(max_deviation_bps) / Decimal::from(10_000);

    let (agree, outliers): (Vec<_>, Vec<_>) = quotes
        .iter()
        .copied()
        .partition(|(_, price)| (*price - first).abs() <= limit);
    if agree.len() < min_sources {
        return Err(AggregateError::TooFewAgree {
            agree: agree.len(),
            need: min_sources,
        });
    }
    let price = median(agree.iter().map(|(_, p)| *p).collect()).expect("non-empty");
    Ok(Aggregate {
        price,
        used: agree.into_iter().map(|(source, _)| source).collect(),
        outliers,
        quotes: quotes.to_vec(),
    })
}

/// Median; the mean of the middle two for an even count.
pub fn median(mut values: Vec<Decimal>) -> Option<Decimal> {
    if values.is_empty() {
        return None;
    }
    values.sort();
    let mid = values.len() / 2;
    Some(if values.len().is_multiple_of(2) {
        (values[mid - 1] + values[mid]) / Decimal::TWO
    } else {
        values[mid]
    })
}

/// Signed distance from `reference` in basis points.
pub fn deviation_bps(price: Decimal, reference: Decimal) -> Decimal {
    (price - reference) / reference * Decimal::from(10_000)
}

#[cfg(test)]
mod tests {
    use rust_decimal::dec;

    use super::*;
    use SourceKind::*;

    #[test]
    fn median_of_odd_and_even_counts() {
        assert_eq!(median(vec![dec!(3), dec!(1), dec!(2)]), Some(dec!(2)));
        assert_eq!(
            median(vec![dec!(4), dec!(1), dec!(2), dec!(3)]),
            Some(dec!(2.5))
        );
        assert_eq!(median(vec![]), None);
    }

    #[test]
    fn one_bad_venue_cannot_move_the_price() {
        let quotes = [
            (Binance, dec!(100.0)),
            (Coinbase, dec!(100.2)),
            (Kraken, dec!(99.9)),
            (Bybit, dec!(150.0)),
        ];
        let result = aggregate(&quotes, 100, 2).unwrap();
        assert_eq!(result.price, dec!(100.0));
        assert_eq!(result.outliers, vec![(Bybit, dec!(150.0))]);
        assert_eq!(result.used.len(), 3);
    }

    #[test]
    fn too_few_sources_or_too_little_agreement_gives_no_price() {
        assert_eq!(
            aggregate(&[(Binance, dec!(100))], 100, 2),
            Err(AggregateError::TooFewQuotes { have: 1, need: 2 })
        );
        assert_eq!(
            aggregate(&[(Binance, dec!(100)), (Coinbase, dec!(120))], 100, 2),
            Err(AggregateError::TooFewAgree { agree: 0, need: 2 })
        );
    }

    #[test]
    fn deviation_is_signed_basis_points() {
        assert_eq!(deviation_bps(dec!(101), dec!(100)), dec!(100));
        assert_eq!(deviation_bps(dec!(99.5), dec!(100)), dec!(-50));
    }
}
