use log::{debug, info, warn};
use std::time::{Duration, Instant};

use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use reqwest::Client;

use std::sync::{Arc, Mutex};
 
use tokio::sync::Semaphore;
use reqwest::redirect::Policy;
use futures_util::StreamExt;
use anyhow::{anyhow, Result};
use reqwest::header::{  HeaderValue, USER_AGENT};
use indicatif::{ProgressBar,ProgressStyle};

use crate::download::file_handler::FileHandler;
use crate::download::file_handler::DataChunk;


const CHUNK_SIZE_LIMIT: u64 = 1024 * 1024;
const MAX_RETRIES: usize = 5; // 定义最大重试次数


#[derive(Debug)]
pub struct DownloadTask {
    client: Client,
    url: String,
    total_size: u64,
    chunk_size :u64,
    num_threads: usize,
    retry_count: usize,
}



impl DownloadTask {
    pub fn new(
        client: Client,
        url: &str, 
        total_size: u64, 
        num_threads: usize, 
        chunk_size_limit: Option<u64>,
        retry_cont: Option<usize>,
        ) -> Self {
        DownloadTask {
            client,
            url: url.to_string(),
            total_size,
            chunk_size: chunk_size_limit.unwrap_or(CHUNK_SIZE_LIMIT),
            retry_count: retry_cont.unwrap_or(MAX_RETRIES),
            num_threads,
        }
    }


    async fn download_chunk(
        semaphore: Arc<Semaphore>,
        current_start: u64,
        end: u64,
        url: String,
        retry_count: usize,
        client: Client,
        sender: mpsc::Sender<DataChunk>,
        exit: Arc<Mutex<bool>>,
        paused: Arc<Mutex<bool>>,
    ) -> Result<(), anyhow::Error> {
        let permit = semaphore.acquire_owned().await.unwrap();
       
        let mut offset_start = current_start;
        
        while offset_start < end {
            if *exit.lock().unwrap() {
                break;
            }
    
            while *paused.lock().unwrap() {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
    
            let mut retries = 0;
            loop {
                let range = format!("bytes={}-{}", offset_start, end);
                debug!("Downloading chunk from {} with range {}", &url, range);
                // 尝试发送请求并获取响应
                let resp_result = client.get(&url).header("Range", range).send().await;
                match resp_result {
                    Ok(resp) => {
                        let data_result = resp.bytes().await;
                        match data_result {
                            Ok(data) => {
                                // 数据读取成功，跳出内部循环
                                debug!("Chunk downloaded successfully {} {}" ,offset_start , data.len());
                                sender.send( DataChunk::new(offset_start, data.to_vec(), None )).await?;
                                offset_start += data.len() as u64;
                                break;
                            },
                            Err(e) => {
                                // 数据读取错误，检查是否已达到最大重试次数
                                if retries >= retry_count {
                                    return Err(anyhow!("Failed to read response bytes after {} retries: {}", retry_count, e));
                                }
                                retries += 1;
                                warn!("Data read error, retrying... ({}/{})", retries, retry_count);
                                tokio::time::sleep(Duration::from_secs(1)).await; // 等待一段时间后重试
                            }
                        }
                    },
                    Err(e) => {
                        // 请求发送错误，检查是否已达到最大重试次数
                        if retries >= retry_count {
                            return Err(anyhow!("Failed to send request after {} retries: {}", retry_count, e));
                        }
                        retries += 1;
                        warn!("Request send error, retrying... ({}/{})", retries, retry_count);
                        tokio::time::sleep(Duration::from_secs(1)).await; // 等待一段时间后重试
                    }
                }
            }
        }
        
        drop(permit); // 任务结束时释放许可
        Ok(())
    }
    pub async fn run(&self, sender: mpsc::Sender<DataChunk>, paused: Arc<Mutex<bool>>, exit: Arc<Mutex<bool>>) -> Result<()> {
        let mut tasks = vec![];
        let semaphore = Arc::new(Semaphore::new(self.num_threads as usize)); // 初始化信号量
        let mut current_start = 0;

        while current_start < self.total_size {
            let end = std::cmp::min(current_start + self.chunk_size, self.total_size);
            let task = tokio::spawn( 
               Self::download_chunk(
                    semaphore.clone(),
                    current_start,
                    end,
                    self.url.clone(),
                    self.retry_count.clone(),
                    self.client.clone(),
                    sender.clone(),
                    exit.clone(),
                    paused.clone(),
                )
            );
            tasks.push(task);
            current_start += self.chunk_size;
        }
        info!("Starting download task size {}", tasks.len());

       

        for task in tasks {
            task.await??;    
        }
        info!("Download completed");
        Ok(())
    }

}

fn format_file_size(size: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if size >= GB {
        format!("{:.2} GB/s", size as f64 / GB as f64)
    } else if size >= MB {
        format!("{:.2} MB/s", size as f64 / MB as f64)
    } else if size >= KB {
        format!("{:.2} KB/s", size as f64 / KB as f64)
    } else {
        format!("{} b/s", size)
    }
}

pub async  fn simple_download(client:Client,url: &str, output_path: &str) ->Result<()> {
     let pb = ProgressBar::new(100);
    pb.set_style(
        ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {msg} ")?
        .progress_chars("#>-"),
    );
    let start_time = Instant::now();

    let mut f = tokio::fs::OpenOptions::new()
    .write(true)
    .create(true)
    .open(output_path).await?;
    let resp = client.get(&url.to_string()).send().await?;
    let mut stream = resp.bytes_stream();
    let mut read_bytes = 0;
    while let Some(item) = stream.next().await {
        let data = item?;
        f.write_all(&data).await?;
        read_bytes+= data.len();
        let elapsed = start_time.elapsed();
        let speed  = if elapsed.as_secs() > 0 {
            read_bytes as usize / elapsed.as_secs() as usize / 8
        } else {
            0
        };

        pb.set_message( format_file_size(speed as u64));
        pb.tick()
    }
    f.flush().await?;
    pb.finish_with_message("download success!");
    Ok(())
}

pub async fn start_single_task(url: &str, output_path: &str,  num_threads: usize,  chunk_size_limit: Option<u64>,
    retry_cont: Option<usize>,) ->Result<()> {
    

    let client = Client::builder()
    .redirect(Policy::limited(50)) // 设置重定向次数上限为10次
    .build()?;

    let resp = client.head(url)
    .header(USER_AGENT, HeaderValue::from_static("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.81 Safari/537.36"))
    .send().await?;
    if resp.status().is_success() {
        debug!("url {}", resp.url());
        debug!("get resp {:?}", resp.headers());
        let mut file_size = resp.content_length().unwrap_or(0);
        if file_size == 0 {
            if let Some(content_length) = resp.headers().get(reqwest::header::CONTENT_LENGTH) {
                debug!("Content-Length header: {:?}", content_length);
                let file_size_str= content_length.to_str()?;
                file_size = file_size_str.parse::<u64>()?;
            } else {
                warn!("No Content-Length header found {}", url);
                simple_download(client.clone(), url, output_path).await?;
                return Ok(());
            }
        }
        let pb =  Arc::new(Mutex::new( ProgressBar::new(file_size)));
        pb.lock().unwrap()
            .set_style(
            ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {msg} {pos}/{len} ({eta})")?
            .progress_chars("#>-"),
        );
        let (tx, mut rx) = mpsc::channel(10);
    
        info!("download file {} File size: {}", output_path, file_size);
        let accept_ranges = resp.headers().get(reqwest::header::ACCEPT_RANGES);
        if let Some(value) = accept_ranges {
            if value != "bytes" {
                return Err(anyhow::anyhow!("Failed to get accept ranges"));
            }
        } else {
            return Err(anyhow::anyhow!("Accept-Ranges not found"));
        }

        let file_handler = Arc::new(FileHandler::new(output_path));
        let (sender, receiver):(mpsc::Sender<DataChunk> ,mpsc::Receiver<DataChunk>) = mpsc::channel(100);
        let paused = Arc::new(Mutex::new(false));
        let exit = Arc::new(Mutex::new(false));
        let task= DownloadTask::new(client.clone(),url, file_size, num_threads,chunk_size_limit,retry_cont);
        
        let file_handler_clone = file_handler.clone();
        let writer_handle = tokio::spawn(async move {
            file_handler_clone.start(receiver, Some(tx)).await?;
            Ok::<(), anyhow::Error>(())
        });

        // 同时启动下载任务 task
        let task_handle = tokio::spawn(async move {
            task.run(sender, paused.clone(), exit.clone()).await?;
            Ok::<(), anyhow::Error>(())
        });
        let procces_handle = tokio::spawn(async move {
            while let Some(progress) = rx.recv().await {
               {
                pb.lock().unwrap().set_position(progress);
               }
            }
            pb.lock().unwrap().finish_and_clear();// ("download success!");
            Ok::<(), anyhow::Error>(())
        });

        // 等待 writer_handle 和 task_handle 完成
        let (writer_result, task_result,p_result) = tokio::try_join!(writer_handle, task_handle , procces_handle)?;

        // 检查任务的结果
        writer_result?;
        task_result?;
        p_result?
    }
    Ok(())
}