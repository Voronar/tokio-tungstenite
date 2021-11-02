//! To make connection slow run this:
//!     sudo ./scripts/slow.sh 3g -d lo         # run
//!     sudo ./scripts/slow.sh 3g -d lo reset   # stop
//!
//! You can test this out by running:
//!
//!     RUST_LOG=debug cargo run --example fragment_server 127.0.0.1:12345
//!
//! And then in another window run:
//!
//!     RUST_LOG=debug cargo run --example fragment_client ws://127.0.0.1:12345/
//!

use futures_channel::mpsc::unbounded;
use futures_util::{future::try_join_all, stream::SplitSink, SinkExt, StreamExt};
use std::{env, error::Error, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    select,
    sync::mpsc::{channel, Sender},
    task::JoinHandle,
    time::sleep,
};
use tokio_tungstenite::WebSocketStream;
use tungstenite::{
    protocol::frame::{
        coding::{Data, OpCode},
        Frame,
    },
    Message, Result,
};

pub fn two_frame_fragmentaion(first: &mut Frame, second: &mut Frame, first_opdata: OpCode) {
    let fh = first.header_mut();
    fh.is_final = false;
    fh.opcode = first_opdata;

    let sh = second.header_mut();
    sh.is_final = true;
    sh.opcode = OpCode::Data(Data::Continue);
}

type WsSink = SplitSink<WebSocketStream<TcpStream>, Message>;

struct FragmentedWrite {
    fragment_size: usize,
    ctl_queue: Sender<Message>,
    data_queue: Sender<Message>,
    sending_processor: JoinHandle<()>,
}

impl Drop for FragmentedWrite {
    fn drop(&mut self) {
        self.sending_processor.abort();
    }
}

impl Unpin for FragmentedWrite {}

impl FragmentedWrite {
    fn new(mut sink: WsSink, fragment_size: usize) -> Self {
        let (data_queue, mut data_queue_rx) = channel(1_000_000);
        let (ctl_queue, mut ctl_queue_rx) = channel(1_000_000);
        let sending_processor = tokio::spawn({
            async move {
                loop {
                    let res = async {
                        select! {
                            Some(msg) = data_queue_rx.recv() => {
                                if let Ok(m) = ctl_queue_rx.try_recv() {
                                    log::debug!("ctl try from data queue");
                                    sink.send(m).await?;
                                }

                                sink.send(msg).await?;

                                if let Ok(m) = ctl_queue_rx.try_recv() {
                                    log::debug!("ctl try from data queue");
                                    sink.send(m).await?;
                                }
                            },
                            Some(m) = ctl_queue_rx.recv() => {
                                log::debug!("ctl selected");
                                sink.send(m).await?;
                            },
                        };

                        Ok(()) as Result<()>
                    };

                    if let Err(e) = res.await {
                        log::error!("Error sending fragments: {}", e);
                    }
                }
            }
        });
        Self { sending_processor, fragment_size, ctl_queue, data_queue }
    }
    async fn send(&mut self, msg: Message) -> Result<(), Box<dyn Error + '_>>
    where
        Self: Unpin,
    {
        if !(msg.is_binary() || msg.is_text()) {
            self.ctl_queue.send(msg).await?;
        } else {
            let (mut data, opdata) = match msg {
                Message::Text(d) => (d.into(), Data::Text),
                Message::Binary(d) => (d, Data::Binary),
                _ => return Ok(()),
            };

            let mut frames = vec![];

            while data.len() > 0 {
                let res: Vec<_> = data.drain(..data.len().min(self.fragment_size)).collect();
                let frame = Frame::message(res, OpCode::Data(Data::Continue), false);
                frames.push(frame);
            }

            match frames.as_mut_slice() {
                [] => {}
                [first] => {
                    let fh = first.header_mut();
                    fh.is_final = true;
                    fh.opcode = OpCode::Data(opdata);
                }
                [first, second] => {
                    two_frame_fragmentaion(first, second, OpCode::Data(opdata));
                }
                [first, .., last] => {
                    two_frame_fragmentaion(first, last, OpCode::Data(opdata));
                }
            };

            log::debug!(
                "Queued fragments: {} ({} bytes pre fragment)",
                frames.len(),
                self.fragment_size
            );
            let futs = frames.into_iter().map(Message::Frame).map(|m| self.data_queue.send(m));
            try_join_all(futs).await?;
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();
    let addr = env::args().nth(1).unwrap_or_else(|| "127.0.0.1:8080".to_string());
    let try_socket = TcpListener::bind(&addr).await;
    let listener = try_socket.expect("Failed to bind");
    println!("Listening on: {}", addr);

    while let Ok((stream, addr)) = listener.accept().await {
        let ws_stream = tokio_tungstenite::accept_async(stream)
            .await
            .expect("Error during the websocket handshake occurred");

        println!("WebSocket connection established: {}", addr);

        let (ws_write_tx, mut ws_write_rx) = unbounded::<Message>();
        #[allow(unused_mut)]
        let (mut ws_write, mut ws_read) = ws_stream.split();
        let mut ws_write = FragmentedWrite::new(ws_write, 4906); // comment this line to see difference in ping responses

        tokio::spawn(async move {
            while let Some(msg) = ws_write_rx.next().await {
                let res = ws_write.send(msg).await;
                if let Err(e) = res {
                    log::error!("Error sending ws message: {}", e);
                    break;
                }
            }
        });

        tokio::spawn({
            let ws_write_tx = ws_write_tx.clone();
            async move {
                log::debug!("Start pinger");
                loop {
                    let res = ws_write_tx.unbounded_send(Message::Ping(b"PING".to_vec()));
                    if let Err(e) = res {
                        log::error!("Error making ping: {}", e);
                        break;
                    }
                    sleep(Duration::from_secs(1)).await;
                }
            }
        });

        tokio::spawn(async move {
            while let Some(msg) = ws_read.next().await {
                let res = async {
                    let msg = msg?;
                    match msg {
                        Message::Text(_) => {}
                        Message::Binary(_) => {
                            let data = vec![255; 10_000_001];
                            // log::debug!("Sending {} bytes", data.len());
                            ws_write_tx.unbounded_send(Message::Binary(data))?;
                        }
                        Message::Ping(p) => {
                            log::debug!("{}", String::from_utf8(p)?);
                            ws_write_tx.unbounded_send(Message::Pong(b"PONG".to_vec()))?;
                        }
                        Message::Pong(p) => {
                            log::debug!("{}", String::from_utf8(p)?);
                        }
                        Message::Close(_) => {}
                        Message::Frame(_) => {}
                    }

                    Ok(()) as Result<(), Box<dyn Error>>
                };

                if let Err(e) = res.await {
                    log::error!("Error reading ws message: {}", e);
                    break;
                }
            }
        });
    }

    Ok(())
}
