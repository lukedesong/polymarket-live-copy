# 探针程序校验码

`SHA256SUMS` 登记东京允许安装的探针程序。程序文件本身不进 git,由云端编好后经网关上传:

```bash
# 编静态版本(不依赖东京的系统库版本)。C 部分(aws-lc)用 zig 当 musl 编译器:pip install ziglang,
# 包装脚本 musl-cc = `python -m ziglang cc -target x86_64-linux-musl -fno-sanitize=undefined`(去掉 cc-rs 传的 --target=…)
CC_x86_64_unknown_linux_musl=musl-cc AR_x86_64_unknown_linux_musl=musl-ar CMAKE_C_COMPILER=musl-cc \
  cargo build --release --target x86_64-unknown-linux-musl
sha256sum target/x86_64-unknown-linux-musl/release/predict-mm-probe   # 登记进 SHA256SUMS,推到 predict-probe 分支
python3 ../gateway/tokyo_client.py deploy target/x86_64-unknown-linux-musl/release/predict-mm-probe --name v4
```

网关安装时:sha256 必须与 predict-probe 分支上这份文件里登记的一致(光有网关口令换不了程序),`--version` 能跑,
同名版本已存在且内容不同就拒绝;装好后 `~/probe/predict-mm-probe.current` 指向新版本,`tokyo_live.sh` 和空跑默认用它。
正在跑的探针不受影响。
