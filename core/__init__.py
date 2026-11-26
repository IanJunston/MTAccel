"""多线程下载引擎核心包。

分层：
    probe    -> 能力探测（大小 / Range / ETag / 单连接基线速度）
    planner  -> 分块规划
    writer   -> 预分配 + 偏移写
    resume   -> 断点续传状态
    verify   -> 完整性校验
    scheduler-> 自适应并发（爬坡 + AIMD）
    engine   -> 主调度：worker 池 + 动态队列 + 事件流

引擎层只通过事件（dict）对外通信，不依赖任何 UI。
将来若用 Rust 重写，只需保持事件协议不变，前端可零改动复用。
"""

__version__ = "0.1.0"
