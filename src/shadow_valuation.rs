//! Same M5 formula and constants as Dashboard's Model5EventAndMarketVolumeAnchored.
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::Deserialize;

#[derive(Deserialize)]
pub struct M5Input {
    pub market_volume: Decimal,
    pub event_volume: Decimal,
    pub proposed_price: Decimal,
    pub seconds_after_propose: u64,
    pub winner_bid: Decimal,
    pub winner_ask: Decimal,
    pub winner_bid_depth: Decimal,
    pub loser_bid: Decimal,
}

pub fn expected_payout(input: &M5Input) -> Result<Decimal, &'static str> {
    let number = |v: Decimal| {
        v.to_f64()
            .filter(|f| f.is_finite())
            .ok_or("invalid_valuation_input")
    };
    let market = number(input.market_volume)?.max(1.0);
    let event = number(input.event_volume)?.max(1.0);
    let bid = number(input.winner_bid)?;
    let ask = number(input.winner_ask)?;
    let depth = number(input.winner_bid_depth)?;
    let loser = number(input.loser_bid)?;
    if !(0.0..=1.0).contains(&bid)
        || !(0.0..=1.0).contains(&ask)
        || !(0.0..=1.0).contains(&loser)
        || depth < 0.0
        || input.market_volume < Decimal::ZERO
        || input.event_volume < Decimal::ZERO
    {
        return Err("invalid_valuation_input");
    }
    let mut z = -12.281814 + 0.198278 * event.log10() + 2.156671 * market.log10();
    if input.proposed_price == Decimal::ONE {
        z -= 0.214394;
    }
    let seconds = input.seconds_after_propose.min(7200) as f64;
    let mut previous = (0.0, 0.0);
    let mut cdf = 0.0;
    for (end, value) in [
        (60.0, 0.054),
        (120.0, 0.108),
        (300.0, 0.297),
        (600.0, 0.419),
        (900.0, 0.473),
        (1200.0, 0.577),
        (1800.0, 0.635),
        (2700.0, 0.761),
        (3600.0, 0.757),
        (5400.0, 0.873),
        (7200.0, 0.959),
    ] {
        if seconds <= end {
            cdf = previous.1 + (seconds - previous.0) / (end - previous.0) * (value - previous.1);
            break;
        }
        previous = (end, value);
    }
    z += (1.0_f64 - cdf).max(0.001).log2() * 0.10;
    z += if bid * depth > 0.0 {
        (market / (bid * depth)).log10() * 0.10
    } else {
        0.20
    };
    z += (ask - bid).max(0.0) * 0.20;
    if loser > 0.0 {
        z += ((loser * 5.0).exp() - 1.0) * 0.35;
    }
    let probability = 1.0 / (1.0 + (-z).exp());
    Decimal::from_f64_retain(1.0 - probability * 0.448276).ok_or("invalid_valuation_result")
}
