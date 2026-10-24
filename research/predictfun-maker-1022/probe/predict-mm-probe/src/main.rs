//! predict-mm-probe:Predict.fun BTC 5m 实盘探针。用法见 README.md。
use clap::Parser;
use predict_mm_probe::config::{Cli, Creds};

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    // 凭证只从环境变量读,绝不打印
    let creds = Creds::from_env();
    let code = match predict_mm_probe::runner::run(cli, creds).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("启动失败: {e:#}");
            1
        }
    };
    std::process::exit(code);
}
