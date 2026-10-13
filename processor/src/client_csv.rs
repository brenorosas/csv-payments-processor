use serde::Serialize;

use crate::storage::client::entity::Client;

const AMOUNT_SCALE: u64 = 10_000;

#[derive(Debug, Serialize)]
pub struct ClientCsv {
    pub client: u16,
    pub available: String,
    pub held: String,
    pub total: String,
    pub locked: bool,
}

impl From<&Client> for ClientCsv {
    fn from(client: &Client) -> Self {
        Self {
            client: client.id,
            available: format_amount(client.available_amount),
            held: format_amount(client.held_amount),
            total: format_amount(client.total_amount),
            locked: client.locked,
        }
    }
}

fn format_amount(amount: i64) -> String {
    let sign = if amount < 0 { "-" } else { "" };
    let amount = amount.unsigned_abs();
    let whole = amount / AMOUNT_SCALE;
    let fraction = amount % AMOUNT_SCALE;

    if fraction == 0 {
        return format!("{sign}{whole}");
    }

    let fraction = format!("{fraction:04}");
    format!("{sign}{whole}.{}", fraction.trim_end_matches('0'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_amount_prints_the_scaled_integer_as_a_decimal() {
        assert_eq!(format_amount(0), "0");
        assert_eq!(format_amount(10_000), "1");
        assert_eq!(format_amount(20_000), "2");
        assert_eq!(format_amount(2_500_000), "250");

        assert_eq!(format_amount(15_000), "1.5");
        assert_eq!(format_amount(12_300), "1.23");
        assert_eq!(format_amount(12_340), "1.234");
        assert_eq!(format_amount(12_345), "1.2345");

        assert_eq!(format_amount(5_000), "0.5");
        assert_eq!(format_amount(100), "0.01");
        assert_eq!(format_amount(1), "0.0001");

        assert_eq!(format_amount(-15_000), "-1.5");
        assert_eq!(format_amount(-20_000), "-2");
        assert_eq!(format_amount(-5_000), "-0.5");
        assert_eq!(format_amount(-1), "-0.0001");

        assert_eq!(format_amount(i64::MAX), "922337203685477.5807");
        assert_eq!(format_amount(i64::MIN), "-922337203685477.5808");
    }
}
