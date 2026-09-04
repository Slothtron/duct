//! 入口桥接：把「已被原语读走请求行」的连接嫁接给进程内 axum/hyper 栈。
//!
//! `aiproxy` 与 `mcp` 两个反向代理分支都复用同一座桥（设计 §4）——语义就是把
//! 「预读字节 + 剩余 socket」双工起来，让 hyper 从另一端看到完整报文。签名收敛为
//! 接收任意 `axum::Router`，两个分支各自传入自己的路由表，行为不变（dispatch 测试兜底）。

use hyper_util::{rt::TokioIo, service::TowerToHyperService};
use tokio::io::{AsyncWriteExt as _, copy_bidirectional, duplex};

/// 内部桥接缓冲容量：仅决定内核态拷贝节奏，不限制报文长度。
const BRIDGE_BUFFER: usize = 64 * 1024;

/// 将「已被手工读走请求行」的连接嫁接给进程内 axum/hyper 栈。
///
/// `app` 为任意 axum Router（aiproxy / mcp 各自传入自己的路由）。预读字节先注入
/// 内部缓冲管道的一端，随后 socket 与该端双向拷贝；hyper 从另一端看到完整报文
/// （请求行 + 余下头部/body），解析与流式语义全部由其接管。覆盖「长请求头跨越
/// 内部缓冲」的场景：duplex 容量仅决定内核态拷贝节奏，不限制报文长度。
pub async fn serve_conn_from_prelude(
    app: axum::Router,
    prelude: &[u8],
    client: tokio::net::TcpStream,
) -> anyhow::Result<()> {
    let service = TowerToHyperService::new(app);
    let (mut client_half, server_half) = duplex(BRIDGE_BUFFER);
    client_half.write_all(prelude).await?;

    let conn = tokio::spawn(async move {
        hyper::server::conn::http1::Builder::new()
            .serve_connection(TokioIo::new(server_half), service)
            .await
    });

    let mut client_half_ref = client_half;
    let mut client = client;
    let _ = copy_bidirectional(&mut client, &mut client_half_ref).await;
    // 连接任一侧结束即收尾；hyper 连接任务随后自行终止
    let _ = conn.await?;
    Ok(())
}
