//! 限额测试:交易(下单、撤单)40/秒、500/分钟,通用(查询)250/分钟,两类分开算,留 20% 余量;撤单优先。
use predict_mm_probe::ratelimit::*;

const MS: u64 = 1_000_000;
const S: u64 = 1_000_000_000;

#[test]
fn caps_with_20pct_margin() {
    let rl = RateLimiter::new(RateLimitConfig::default());
    assert_eq!(rl.caps(), (32, 400));
    assert_eq!(rl.general_caps(), (32, 200));
}

#[test]
fn trade_and_general_budgets_are_separate() {
    let mut rl = RateLimiter::new(RateLimitConfig::default());
    let t = 9 * S;
    // 交易额度用光,查询照样能发
    while rl.try_acquire(t, Priority::Cancel) {}
    assert!(rl.try_acquire(t, Priority::Query));
    // 通用额度用光,撤单照样能发
    let mut rl = RateLimiter::new(RateLimitConfig::default());
    while rl.try_acquire(t, Priority::QueryUrgent) {}
    assert!(!rl.try_acquire(t, Priority::Query));
    assert!(rl.try_acquire(t, Priority::Cancel));
    assert!(rl.try_acquire(t, Priority::Place));
    assert_eq!(Priority::Place.bucket(), Bucket::Trade);
    assert_eq!(Priority::Background.bucket(), Bucket::General);
}

#[test]
fn per_second_window() {
    let mut rl = RateLimiter::new(RateLimitConfig::default());
    let t0 = 100 * S;
    let mut n = 0;
    while rl.try_acquire(t0, Priority::Cancel) {
        n += 1;
    }
    assert_eq!(n, 32, "同一时刻最多 32 个");
    // 999ms 后仍满
    assert!(!rl.try_acquire(t0 + 999 * MS, Priority::Cancel));
    // 满 1 秒释放
    assert!(rl.try_acquire(t0 + S, Priority::Cancel));
}

#[test]
fn per_minute_windows_never_exceeded_in_any_60s() {
    let mut rl = RateLimiter::new(RateLimitConfig::default());
    let (mut trade, mut general): (Vec<u64>, Vec<u64>) = (vec![], vec![]);
    // 每 10ms 请求一次,跑 3 分钟,优先级混合
    let mut t = 0u64;
    while t < 180 * S {
        let prio = match (t / (10 * MS)) % 4 {
            0 => Priority::Cancel,
            1 => Priority::Place,
            2 => Priority::Query,
            _ => Priority::QueryUrgent,
        };
        if rl.try_acquire(t, prio) {
            if prio.bucket() == Bucket::Trade {
                trade.push(t)
            } else {
                general.push(t)
            }
        }
        t += 10 * MS;
    }
    // 任意 60 秒窗口:交易 ≤ 400、通用 ≤ 200;任意 1 秒窗口 ≤ 32
    for (sent, cap_min) in [(&trade, 400), (&general, 200)] {
        for (i, &a) in sent.iter().enumerate() {
            let in_min = sent[i..].iter().take_while(|&&b| b < a + 60 * S).count();
            let in_sec = sent[i..].iter().take_while(|&&b| b < a + S).count();
            assert!(in_min <= cap_min, "60s 窗口 {in_min}");
            assert!(in_sec <= 32, "1s 窗口 {in_sec}");
        }
    }
    // 长期平均接近预算(不会因保留额度饿死)
    assert!(trade.len() >= 3 * 400 - 50, "3 分钟交易 {}", trade.len());
    assert!(general.len() >= 3 * 200 - 30, "3 分钟查询 {}", general.len());
}

#[test]
fn cancels_have_priority_over_places_and_urgent_over_queries() {
    let cfg = RateLimitConfig::default();
    let mut rl = RateLimiter::new(cfg.clone());
    let t = 5 * S;
    // 新挂最多用到每秒剩 4 个
    let mut places = 0;
    while rl.try_acquire(t, Priority::Place) {
        places += 1;
    }
    assert_eq!(places, 32 - cfg.place_reserve_sec);
    // 撤单仍能用掉保留额度
    let mut cancels = 0;
    while rl.try_acquire(t, Priority::Cancel) {
        cancels += 1;
    }
    assert_eq!(cancels, cfg.place_reserve_sec);
    // 普通查询给紧急查询留保留额度;紧急查询能用掉它
    let mut rl = RateLimiter::new(cfg.clone());
    let mut q = 0;
    while rl.try_acquire(t, Priority::Query) {
        q += 1;
    }
    assert_eq!(q, 32 - cfg.query_reserve_sec);
    let mut u = 0;
    while rl.try_acquire(t, Priority::QueryUrgent) {
        u += 1;
    }
    assert_eq!(u, cfg.query_reserve_sec);
}

#[test]
fn pause_after_429_only_lets_cancels_through() {
    let mut rl = RateLimiter::new(RateLimitConfig::default());
    rl.pause_non_cancel(10 * S);
    assert!(!rl.try_acquire(S, Priority::Place));
    assert!(!rl.try_acquire(S, Priority::Query));
    assert!(!rl.try_acquire(S, Priority::Background));
    assert!(rl.try_acquire(S, Priority::Cancel));
    assert!(rl.try_acquire(S, Priority::QueryUrgent), "全撤前列挂单不能被挡");
    assert!(rl.try_acquire(10 * S, Priority::Place));
}

#[test]
fn next_cancel_slot() {
    let mut rl = RateLimiter::new(RateLimitConfig::default());
    let t = 3 * S;
    assert_eq!(rl.next_cancel_slot(t), t);
    while rl.try_acquire(t, Priority::Cancel) {}
    assert_eq!(rl.next_cancel_slot(t + 10 * MS), t + S);
}

#[test]
fn background_query_only_when_general_budget_more_than_half_left() {
    let mut rl = RateLimiter::new(RateLimitConfig::default());
    // 每分钟预算 200:用掉 99 个(分散在 99 秒外的不同秒里,避免碰到每秒上限)后还剩 101 → 尽力查询可以
    let t0 = 1_000 * S;
    for k in 0..99u64 {
        assert!(rl.try_acquire(t0 + k * 500 * MS, Priority::Query));
    }
    let now = t0 + 50 * S;
    assert!(rl.try_acquire(now, Priority::Background), "剩 101/200");
    // 尽力查询 1 个 + 普通查询 1 个 → 剩 99 < 一半:尽力查询跳过,普通查询照发
    assert!(rl.try_acquire(now + S, Priority::Query));
    assert!(!rl.try_acquire(now + S, Priority::Background));
    assert!(rl.try_acquire(now + S, Priority::Query));
}
