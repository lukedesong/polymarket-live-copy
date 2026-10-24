"""回放器适配器:用 research/predictfun-maker-1022/model/mm_grid.py 的成交规则,对探针实际挂过的单重放。

**尚未实现**:mm_grid.py 不在仓库里(交接卡记录它在本机 model/ 下,sha 3f9e99e2d2b0cd47),规则没读到,不能凭猜测写。
把 mm_grid.py(以及它依赖的 pending_fills.py 和输入数据格式说明:bookx、mkh、fills_pending.parquet)推到 predict-probe 分支后,
在这里实现 replay_orders,其余对比流程(replay_compare.py compare)不用改。

接口约定:
  replay_orders(live_orders, model_dir, extra_args) -> [ {order_id, rp_filled_shares, rp_first_fill_ms, rp_fill_price}, ... ]
  live_orders:replay_compare.py export 写出的 live_orders.tsv 的行(字符串字典),关键列:
    market_id、book_side(bid/ask,在 Up 盘口上)、up_level(Up 盘口价位)、shares、post_ms(开始挂着)、remove_ms(不再挂着)
  规则要求:对每张单,只在 [post_ms, remove_ms) 区间内按回放器的排队与成交逻辑判断是否成交、何时成交、成交价。
"""
import os


def replay_orders(live_orders, model_dir, extra_args=None):
    path = os.path.join(model_dir, "mm_grid.py")
    if not os.path.exists(path):
        raise SystemExit("找不到 %s:mm_grid.py 不在仓库里,请先把它推到 predict-probe 分支(或用 --model-dir 指向它所在目录)" % path)
    raise SystemExit("mm_grid_adapter.replay_orders 还没按 mm_grid.py 的成交规则实现(读到文件后补);"
                     "也可以在本机直接跑回放器,按 replay_compare.py 文档里的 replay_fills.tsv 格式写出结果后用 compare 子命令对比")
