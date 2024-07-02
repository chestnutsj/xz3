use crate::download::file_handler::{
    checkout_filename, download_save_file, md5_check, verify_content_md5, DataChunk,
};
use crate::download::status::Status;
use log::{debug, info, trace, warn};
use reqwest::Response;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use anyhow::{anyhow, Result};
use futures_util::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::header::{HeaderValue, USER_AGENT};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use tokio::sync::Semaphore;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

pub struct TaskCtrl {
    pub stop :Arc<Notify>,
    pub exit :Arc<Notify>,
    pub resume :Arc<Notify>
}

pub struct HttpTask {
    pub url: String,
    pub file_name: String,
    pub total_size: u64,
    pub chunk_size: u64,
    pub num_threads: usize,
    pub retry_count: usize,
    pub ctrl: Arc<TaskCtrl>,
}

pub async fn simple_download(
    client: ClientWithMiddleware,
    url: String,
    sender: mpsc::Sender<DataChunk>,
    paused: Arc<AtomicBool>,
    exit: Arc<AtomicBool>,
) -> Result<(), anyhow::Error> {
    let resp = client.get(&url.to_string()).send().await?;

    if !resp.status().is_success() {
        return Err(anyhow::format_err!(
            "Request failed with status: {}",
            resp.status()
        ));
    }
    let mut stream = resp.bytes_stream();
    let mut offset_start = 0;

    while let Some(item) = stream.next().await {
        let data = item?;

        let data_chunk = DataChunk::new(offset_start, data.to_vec(), 0);
        sender.send(data_chunk).await?;
        let last_offset = offset_start;
        offset_start += data.len() as u64;
        let marker_chunk = DataChunk::new(last_offset, vec![], offset_start);
        sender.send(marker_chunk).await?;

        if exit.load(Ordering::SeqCst) {
            return Ok(());
        }
        while paused.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    Ok(())
}

impl HttpTask {
    pub async fn download_per_chunk(self,  client: ClientWithMiddleware,  ) {
        client.get(&url.to_string())
    }

    async fn stream_download(resp: Response,mut offset:u64 ,sender: mpsc::Sender<DataChunk>,ctrl: Arc<TaskCtrl> ) -> Result<()> {
        if resp.status().is_success() {
            let start = offset;
            let mut stream = resp.bytes_stream();
            loop {
                tokio::select! {
                    Some(item) = stream.next() => {
                        match item {
                            Ok(data) => {
                                let data_chunk = DataChunk::new(offset, data.to_vec(), 0);
                                sender.send( data_chunk ).await?;
                                offset +=  data.len() as u64;
                            },
                            Err(e) => {
                                if offset > start {
                                    sender.send( DataChunk::new(start, vec![], offset)).await?;
                                }
                                return Err(anyhow::anyhow!("Error reading response: {}", e));
                            }
                        };
                    },
                    _ = ctrl.stop.notified() => {
                        info!("stop download");
                        tokio::select!{
                            _ =  ctrl.resume.notified() =>{
                                continue;
                            }
                            _ =  ctrl.exit.notified() =>{
                                break;
                            }
                        }
                    },
                    _=  ctrl.exit.notified() => {
                        info!("exit download {}",offset);
                        break;
                    },
                    else => {
                       break;
                    }   
                }
            }
            sender.send(  DataChunk::new(start, vec![], offset) ).await?;
            return Ok(())
        } else {
            Err(anyhow::anyhow!("Error: {}", resp.status()))
        }
    }
    async fn download_resp(self, client: ClientWithMiddleware,  sender: mpsc::Sender<DataChunk>,opt_range: Option<(u64,u64)>) -> Result<()> {
       let mut req= client.get(self.url.clone());
       let mut offset =0;
       let mut retries = 0;
       let retry_count = self.retry_count;

        if let Some((start,end)) = opt_range {
            let range = format!("bytes={}-{}", start, end);
            debug!("Downloading chunk from {} with range {}", self.url, range);
            // 尝试发送请求并获取响应
            req =req.header("Range", range) ;
            offset = start;
        }
        let real_start = offset;
        let ctrl = self.ctrl.clone();

        loop {
            tokio::select! {
                _ = ctrl.stop.notified() => {
                    info!("stop download");
                    tokio::select!{
                        _ = ctrl.resume.notified() =>{
                            continue;
                        }
                        _ = ctrl.exit.notified() =>{
                           return Ok(());
                        }
                    }
                },
                _= ctrl.exit.notified() => {
                    return Ok(());
                },
                result = req.send() => { 
                    match result {
                        Ok(resp) => {
                            offset = real_start;
                            match Self::stream_download(resp, offset,sender.clone(), self.ctrl.clone()).await {
                                Ok(_) => {
                                    return Ok(());
                                },
                                Err(e) => {
                                      // 数据读取错误，检查是否已达到最大重试次数
                                    if retries >= retry_count {
                                        return Err(anyhow!(
                                            "Failed to read response bytes after {} retries: {}",
                                            retry_count,
                                            e
                                        ));
                                    }
                                    retries += 1;
                                    warn!("Data read error, retrying... ({}/{})", retries, retry_count);
                                    tokio::time::sleep(Duration::from_secs(1)).await;
                                    continue;
                                }
                            } 
                        },
                        Err(e) => {
                            if retries >= retry_count {
                                return Err(anyhow!(
                                    "Failed to read response bytes after {} retries: {}",
                                    retry_count,
                                    e
                                ));
                            }
                            warn!("Data read error, retrying... ({}/{})", retries, retry_count);
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            continue;
                        }
                    }
                },
                else => {
                    return Ok(());
                }
            }
        }
    } 
}