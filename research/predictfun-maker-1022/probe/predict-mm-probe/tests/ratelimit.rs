//! 令牌桶(滑动窗口)限额测试:40/秒、500/分钟,留 20% 余量;撤单优先。
use predict_mm_probe::ratelimit::*;

const MS: u64 = 1_000_000;
const S: u64 = 1_000_000_000;

#[test]
fn caps_with_20pct_margin() {
    let rl = RateLimiter::new(RateLimitConfig::default());
    assert_eq!(rl.caps(), (32, 400));
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
fn per_minute_window_never_exceeds_400_in_any_60s() {
    let mut rl = RateLimiter::new(RateLimitConfig::default());
    let mut sent: Vec<u64> = vec![];
    // 每 10ms 请求一次,跑 3 分钟,优先级混合
    let mut t = 0u64;
    while t < 180 * S {
        let prio = match (t / (10 * MS)) % 3 {
            0 => Priority::Cancel,
            1 => Priority::Place,
            _ => Priority::Query,
        };
        if rl.try_acquire(t, prio) {
            sent.push(t);
        }
        t += 10 * MS;
    }
    // 任意 60 秒窗口 ≤ 400、任意 1 秒窗口 ≤ 32
    for (i, &a) in sent.iter().enumerate() {
        let in_min = sent[i..].iter().take_while(|&&b| b < a + 60 * S).count();
        let in_sec = sent[i..].iter().take_while(|&&b| b < a + S).count();
        assert!(in_min <= 400, "60s 窗口 {in_min}");
        assert!(in_sec <= 32, "1s 窗口 {in_sec}");
    }
    // 长期平均接近 400/分钟(不会因保留额度饿死)
    assert!(sent.len() >= 3 * 400 - 50, "3 分钟发了 {}", sent.len());
}

#[test]
fn cancels_have_priority_over_places_and_queries() {
    let cfg = RateLimitConfig::default();
    let mut rl = RateLimiter::new(cfg.clone());
    let t = 5 * S;
    // 新挂最多用到每秒剩 4 个
    let mut places = 0;
    while rl.try_acquire(t, Priority::Place) {
        places += 1;
    }
    assert_eq!(places, 32 - cfg.place_reserve_sec);
    assert!(!rl.try_acquire(t, Priority::Query));
    // 撤单仍能用掉保留额度
    let mut cancels = 0;
    while rl.try_acquire(t, Priority::Cancel) {
        cancels += 1;
    }
    assert_eq!(cancels, cfg.place_reserve_sec);
    // 查询的保留更多
    let mut rl = RateLimiter::new(cfg.clone());
    let mut q = 0;
    while rl.try_acquire(t, Priority::Query) {
        q += 1;
    }
    assert_eq!(q, 32 - cfg.query_reserve_sec);
}

#[test]
fn pause_after_429_only_lets_cancels_through() {
    let mut rl = RateLimiter::new(RateLimitConfig::default());
    rl.pause_non_cancel(10 * S);
    assert!(!rl.try_acquire(S, Priority::Place));
    assert!(!rl.try_acquire(S, Priority::Query));
    assert!(rl.try_acquire(S, Priority::Cancel));
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
fn starving_query_is_promoted_after_5s_but_never_eats_cancel_reserve() {
    let cfg = RateLimitConfig::default();
    let mut rl = RateLimiter::new(cfg.clone());
    let t = 7 * S;
    // 把每分钟预算用到只剩 60(> 新挂保留 40,< 查询保留 120)
    let mut k = 0u64;
    while rl.remaining(t + k * S).1 > 60 {
        let now = t + k * S;
        while rl.remaining(now).0 > 0 && rl.remaining(now).1 > 60 {
            rl.try_acquire(now, Priority::Cancel);
        }
        k += 1;
    }
    let now = t + k * S + S;
    assert!(!rl.try_acquire(now, aged_priority(Priority::Query, 4 * S)), "等 4 秒还不升级");
    assert!(rl.try_acquire(now, aged_priority(Priority::Query, 5 * S)), "等满 5 秒按新挂同级放行");
    assert_eq!(aged_priority(Priority::Cancel, 100 * S), Priority::Cancel);
    // 升级后的查询也只能用到新挂保留额度为止
    while rl.try_acquire(now, aged_priority(Priority::Query, 9 * S)) {}
    let (rs, rm) = rl.remaining(now);
    assert!(rs >= cfg.place_reserve_sec || rm >= cfg.place_reserve_min, "撤单保留还在");
    assert!(rl.try_acquire(now, Priority::Cancel), "撤单仍能发");
}
