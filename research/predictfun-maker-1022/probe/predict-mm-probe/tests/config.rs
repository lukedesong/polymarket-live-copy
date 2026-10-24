//! --send 的硬上限:单日、累计亏损都 ≤ $100(10-24 Luke 定:亏损上限 $100,改动经独立审核)
use clap::Parser;
use predict_mm_probe::config::{Cli, Creds};

fn creds() -> Creds {
    Creds { api_key: Some("k".into()), private_key: Some("p".into()), account: Some("0x35FB7E1ac51a9c4504C1d6ffC64F3d45a420F651".into()) }
}

fn check(args: &[&str]) -> Result<(), String> {
    let mut v = vec!["predict-mm-probe"];
    v.extend_from_slice(args);
    Cli::try_parse_from(v).unwrap().validate(&creds()).map_err(|e| e.to_string())
}

#[test]
fn loss_cap_100_allowed_with_send() {
    check(&["--send", "--max-daily-loss-usd", "100", "--max-total-loss-usd", "100"]).unwrap();
    check(&["--send", "--max-daily-loss-usd", "30", "--max-total-loss-usd", "30"]).unwrap();
}

#[test]
fn loss_cap_above_100_rejected_with_send() {
    for a in [["--max-daily-loss-usd", "100.01"], ["--max-total-loss-usd", "101"], ["--max-daily-loss-usd", "1000"]] {
        let e = check(&["--send", a[0], a[1]]).unwrap_err();
        assert!(e.contains("硬上限"), "{a:?} → {e}");
    }
    // 非数也不行
    assert!(check(&["--send", "--max-daily-loss-usd", "NaN"]).is_err());
}

#[test]
fn defaults_unchanged() {
    let c = Cli::try_parse_from(["predict-mm-probe"]).unwrap();
    assert_eq!((c.max_daily_loss_usd, c.max_total_loss_usd), (30.0, 100.0), "默认值不变:要用 $100 必须显式传参");
    // dry-run 不受 --send 硬上限约束,但凭证缺失也能过
    Cli::try_parse_from(["predict-mm-probe", "--max-daily-loss-usd", "500"]).unwrap().validate(&Creds::default()).unwrap();
}
