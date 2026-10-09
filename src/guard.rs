//! Safety checks between an aggregated price and an oracle update.
//!
//! The exchange only clamps oracle prices to 10x the start-of-day value, so a bad tick from
//! the sources could move a market's oracle a long way at once, and liquidations follow
//! the oracle. The guard caps how far a price may move per update and refuses prices
//! outside the exchange's daily band.

use std::collections::HashMap;

use rust_decimal::Decimal;

/// What the guard did with one price.
#[derive(Debug, Clone, PartialEq)]
pub enum Verdict {
    /// Within limits; send as is.
    Accepted(Decimal),
    /// Moved more than the cap from the last accepted price; send the capped price,
    /// which closes the gap over the next updates.
    Clamped { wanted: Decimal, sent: Decimal },
    /// Outside 1/10x to 10x of the daily reference; send nothing for this asset.
    OutsideBand { price: Decimal, reference: Decimal },
}

impl Verdict {
    /// The price to send, if any.
    pub fn price(&self) -> Option<Decimal> {
        match self {
            Verdict::Accepted(price) | Verdict::Clamped { sent: price, .. } => Some(*price),
            Verdict::OutsideBand { .. } => None,
        }
    }
}

pub struct Guard {
    max_move_bps: u32,
    /// Last accepted oracle price per coin.
    last: HashMap<String, Decimal>,
}

impl Guard {
    pub fn new(max_move_bps: u32) -> Self {
        Self {
            max_move_bps,
            last: HashMap::new(),
        }
    }

    /// Anchors a coin to the price the DEX already shows, so the first update can't jump.
    /// Does nothing once the coin has an accepted price.
    pub fn seed(&mut self, coin: &str, price: Decimal) {
        self.last.entry(coin.to_string()).or_insert(price);
    }

    /// Checks one price. `reference` is the DEX's previous-day price, when known.
    pub fn check(&mut self, coin: &str, wanted: Decimal, reference: Option<Decimal>) -> Verdict {
        if let Some(reference) = reference.filter(|r| *r > Decimal::ZERO) {
            let ten = Decimal::TEN;
            if wanted > reference * ten || wanted < reference / ten {
                return Verdict::OutsideBand {
                    price: wanted,
                    reference,
                };
            }
        }
        let verdict = match self.last.get(coin) {
            Some(&last) => {
                let cap = last * Decimal::from(self.max_move_bps) / Decimal::from(10_000);
                if (wanted - last).abs() <= cap {
                    Verdict::Accepted(wanted)
                } else {
                    let sent = if wanted > last {
                        last + cap
                    } else {
                        last - cap
                    };
                    Verdict::Clamped { wanted, sent }
                }
            }
            None => Verdict::Accepted(wanted),
        };
        if let Some(price) = verdict.price() {
            self.last.insert(coin.to_string(), price);
        }
        verdict
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::dec;

    use super::*;

    #[test]
    fn small_moves_pass() {
        let mut guard = Guard::new(200);
        guard.seed("x:BTC", dec!(100));
        assert_eq!(
            guard.check("x:BTC", dec!(101.5), None),
            Verdict::Accepted(dec!(101.5))
        );
    }

    #[test]
    fn large_moves_are_capped_and_converge() {
        let mut guard = Guard::new(200);
        guard.seed("x:BTC", dec!(100));
        assert_eq!(
            guard.check("x:BTC", dec!(110), None),
            Verdict::Clamped {
                wanted: dec!(110),
                sent: dec!(102)
            }
        );
        // The next update starts from the capped price, so the gap closes step by step.
        assert_eq!(
            guard.check("x:BTC", dec!(110), None).price(),
            Some(dec!(104.04))
        );
        guard.seed("x:ETH", dec!(100));
        assert_eq!(guard.check("x:ETH", dec!(90), None).price(), Some(dec!(98)));
    }

    #[test]
    fn prices_outside_the_daily_band_are_refused_and_not_remembered() {
        let mut guard = Guard::new(200);
        guard.seed("x:BTC", dec!(100));
        assert!(matches!(
            guard.check("x:BTC", dec!(1001), Some(dec!(100))),
            Verdict::OutsideBand { .. }
        ));
        assert!(matches!(
            guard.check("x:BTC", dec!(9.9), Some(dec!(100))),
            Verdict::OutsideBand { .. }
        ));
        // The refused price didn't become the anchor.
        assert_eq!(
            guard.check("x:BTC", dec!(101), Some(dec!(100))),
            Verdict::Accepted(dec!(101))
        );
    }

    #[test]
    fn seeding_does_not_override_an_accepted_price() {
        let mut guard = Guard::new(200);
        assert_eq!(
            guard.check("x:BTC", dec!(100), None),
            Verdict::Accepted(dec!(100))
        );
        guard.seed("x:BTC", dec!(50));
        assert_eq!(
            guard.check("x:BTC", dec!(101), None),
            Verdict::Accepted(dec!(101))
        );
    }
}
