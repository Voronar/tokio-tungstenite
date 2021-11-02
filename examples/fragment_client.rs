use futures_channel::mpsc::unbounded;
use futures_util::{SinkExt, StreamExt};
use std::{env, error::Error, time::Duration};
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let connect_addr =
        env::args().nth(1).unwrap_or_else(|| panic!("this program requires at least one argument"));

    let url = url::Url::parse(&connect_addr)?;
    let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");
    println!("WebSocket handshake has been successfully completed");

    let (mut ws_write_tx, mut ws_write_rx) = unbounded::<Message>();
    let (mut ws_write, mut ws_read) = ws_stream.split();

    tokio::spawn({
        let ws_write_tx = ws_write_tx.clone();
        async move {
            log::debug!("Start pinger");
            loop {
                let res = ws_write_tx.unbounded_send(Message::Binary(b"HELLO".to_vec()));
                ws_write_tx.unbounded_send(Message::Ping(b"PING".to_vec())).unwrap();
                if let Err(e) = res {
                    log::error!("Error sending Hello: {}", e);
                }
                sleep(Duration::from_millis(1000)).await;
            }
        }
    });

    tokio::spawn(async move {
        while let Some(msg) = ws_write_rx.next().await {
            let res = ws_write.send(msg).await;
            if let Err(e) = res {
                log::error!("Error sending ws message: {}", e);
            }
        }
    });

    loop {
        while let Some(msg) = ws_read.next().await {
            let res = async {
                let msg = msg?;
                match msg {
                    Message::Text(_) => {}
                    Message::Binary(d) => {
                        log::debug!("Recieved data len: {}", d.len());
                    }
                    Message::Ping(_) => {
                        log::debug!("PING");
                        ws_write_tx.start_send(Message::Pong(b"PONG".to_vec()))?;
                    }
                    Message::Pong(p) => {
                        log::debug!("{}", String::from_utf8(p)?);
                    }
                    Message::Close(_) => {},
                    Message::Frame(_) => {},
                }

                Ok(()) as Result<(), Box<dyn Error>>
            };

            if let Err(e) = res.await {
                log::error!("Error reading ws message: {}", e)
            }
        }
    }
}
