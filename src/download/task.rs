use crate::download::file_handler::{
    checkout_filename, download_save_file, md5_check, verify_content_md5, DataChunk,
};
use crate::download::status::Status;
use log::{debug, info, trace, warn};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use anyhow::{anyhow, Result};
use futures_util::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::header::{HeaderValue, USER_AGENT};
use reqwest::redirect::Policy;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware, Middleware};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use tokio::sync::Semaphore;

use serde::{Deserialize, Serialize};

const CHUNK_SIZE_LIMIT: u64 = 1024 * 1024;
const MAX_RETRIES: usize = 5; // 定义最大重试次数
const SUFFIX: &str = "xz3";

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct DownloadTask {
    pub url: String,
    pub file_name: String,
    pub total_size: u64,
    pub chunk_size: u64,
    pub num_threads: usize,
    pub retry_count: usize,
}

impl DownloadTask {
    pub fn new(
        url: &str,
        file_name: PathBuf,
        total_size: u64,
        num_threads: usize,
        chunk_size_limit: Option<u64>,
        retry_cont: Option<usize>,
    ) -> Self {
        DownloadTask {
            url: url.to_string(),
            file_name: file_name.to_string_lossy().into_owned(),
            total_size,
            chunk_size: chunk_size_limit.unwrap_or(CHUNK_SIZE_LIMIT),
            retry_count: retry_cont.unwrap_or(MAX_RETRIES),
            num_threads,
        }
    }

    async fn download_chunk(
        semaphore: Arc<Semaphore>,
        start: u64,
        end: u64,
        url: String,
        retry_count: usize,
        client: ClientWithMiddleware,
        sender: mpsc::Sender<DataChunk>,
        exit: Arc<AtomicBool>,
        paused: Arc<AtomicBool>,
    ) -> Result<(), anyhow::Error> {
        let permit = semaphore.acquire_owned().await.unwrap();

        let mut offset_start = start;

        while offset_start < end {
            if exit.load(Ordering::SeqCst) {
                break;
            }

            while paused.load(Ordering::SeqCst) {
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
                                trace!(
                                    "Chunk downloaded successfully {} {}",
                                    offset_start,
                                    data.len()
                                );
                                sender
                                    .send(DataChunk::new(offset_start, data.to_vec(), 0))
                                    .await?;
                                offset_start += data.len() as u64;
                                break;
                            }
                            Err(e) => {
                                // 数据读取错误，检查是否已达到最大重试次数
                                if retries >= retry_count {
                                    sender
                                        .send(DataChunk::new(start, vec![], offset_start))
                                        .await?;

                                    return Err(anyhow!(
                                        "Failed to read response bytes after {} retries: {}",
                                        retry_count,
                                        e
                                    ));
                                }
                                retries += 1;
                                warn!("Data read error, retrying... ({}/{})", retries, retry_count);
                                tokio::time::sleep(Duration::from_secs(1)).await;
                                // 等待一段时间后重试
                            }
                        }
                    }
                    Err(e) => {
                        // 请求发送错误，检查是否已达到最大重试次数
                        if retries >= retry_count {
                            sender
                                .send(DataChunk::new(start, vec![], offset_start))
                                .await?;

                            return Err(anyhow!(
                                "Failed to send request after {} retries: {}",
                                retry_count,
                                e
                            ));
                        }
                        retries += 1;
                        warn!(
                            "Request send error, retrying... ({}/{})",
                            retries, retry_count
                        );
                        tokio::time::sleep(Duration::from_secs(1)).await; // 等待一段时间后重试
                    }
                }
            }
        }

        if offset_start >= end {
            debug!(
                "Download chunk offset {} from  {} end {} flush",
                offset_start, start, end
            );
            sender.send(DataChunk::new(start, vec![], end)).await?;
        }
        drop(permit); // 任务结束时释放许可
        Ok(())
    }
    pub async fn muti_download(
        url: String,
        num_threads: usize,
        retry_count: usize,
        file_chunk_per: HashMap<u64, u64>,
        client: ClientWithMiddleware,
        sender: mpsc::Sender<DataChunk>,
        paused: Arc<AtomicBool>,
        exit: Arc<AtomicBool>,
    ) -> Result<()> {
        let mut tasks = vec![];

        debug!("Starting download task size {}", file_chunk_per.len());
        let semaphore = Arc::new(Semaphore::new(num_threads as usize)); // 初始化信号量
        for (start, end) in file_chunk_per {
            let task = tokio::spawn(Self::download_chunk(
                semaphore.clone(),
                start.clone(),
                end.clone(),
                url.clone(),
                retry_count.clone(),
                client.clone(),
                sender.clone(),
                exit.clone(),
                paused.clone(),
            ));
            tasks.push(task);
        }
        for task in tasks {
            task.await??;
        }
        info!("Download completed");
        Ok(())
    }
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
    let mut last_offset = 0;
    while let Some(item) = stream.next().await {
        let data = item?;
        last_offset = offset_start;
        let data_chunk = DataChunk::new(offset_start, data.to_vec(), 0);
        sender.send(data_chunk).await?;

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

pub async fn start_single_task(
    url: &str,
    output_path: &PathBuf,
    num_threads: usize,
    chunk_size_limit: Option<u64>,
    retry_cont: Option<usize>,
) -> Result<()> {
    let paused = Arc::new(AtomicBool::new(false));
    let exit = Arc::new(AtomicBool::new(false));

    let retry_size = retry_cont.unwrap_or(3);
    info!("retry_size: {}", retry_size as u32);
    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(retry_size as u32);
    let cc = reqwest::Client::builder()
        .timeout(Duration::from_secs(4))
        .build()
        .unwrap();
    let client = ClientBuilder::new(cc)
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build();

    info!("client {:?}", client);
    let resp = client.head(url)
    .header(USER_AGENT, HeaderValue::from_static("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.81 Safari/537.36"))
    .send().await?;
    if resp.status().is_success() {
        info!("url {}", resp.url());
        debug!("get resp {:?}", resp.headers());
        let mut accept_range = false;
        let mut file_size = resp.content_length().unwrap_or(0);
        if file_size == 0 {
            if let Some(content_length) = resp.headers().get(reqwest::header::CONTENT_LENGTH) {
                debug!("Content-Length header: {:?}", content_length);
                let file_size_str = content_length.to_str()?;
                file_size = file_size_str.parse::<u64>()?;
            } else {
                warn!("No Content-Length header found {}", url);
            }
        }

        let accept_ranges = resp.headers().get(reqwest::header::ACCEPT_RANGES);
        if let Some(value) = accept_ranges {
            if value == "bytes" {
                accept_range = true
            }
        }
        info!("url header: {:?}", resp.headers());

        let md5 = resp.headers().get("Content-Md5");

        let mut download_file_name = output_path.clone();

        let status_file = output_path.clone().with_extension(SUFFIX);
        let status_file_bak: PathBuf = status_file.clone();
        let mut task_info = DownloadTask::new(
            url,
            download_file_name.clone(),
            file_size,
            num_threads,
            chunk_size_limit,
            retry_cont,
        );

        download_file_name =
            checkout_filename(download_file_name, status_file.clone(), &task_info).await;

        task_info.file_name = download_file_name.to_string_lossy().into_owned();

        let (sender, receiver): (mpsc::Sender<DataChunk>, mpsc::Receiver<DataChunk>) =
            mpsc::channel(100);

        let (sender_status, receiver_status): (
            mpsc::Sender<(u64, u64)>,
            mpsc::Receiver<(u64, u64)>,
        ) = mpsc::channel(100);

        let writer_handle = tokio::spawn(download_save_file(
            download_file_name.clone(),
            receiver,
            Some(sender_status),
        ));

        let download_handle: JoinHandle<Result<(), anyhow::Error>>;
        let status_handle: JoinHandle<Result<(), anyhow::Error>>;

        let pb = Arc::new(Mutex::new(ProgressBar::new(file_size)));
        {
            let pb = pb.lock().unwrap();
            let style_str = format!( "{:?} {{spinner:.green}} [{{elapsed_precise}}] [{{wide_bar:.cyan/blue}}] {{msg}} {{pos}}/{{len}} ({{eta}})",download_file_name.file_name().unwrap());

            pb.set_style(ProgressStyle::with_template(&style_str)?.progress_chars("#>-"));
        }

        if num_threads > 1
            && file_size != 0
            && file_size > CHUNK_SIZE_LIMIT
            && accept_range != false
        {
            info!("Starting multi-threaded download task");

            let task_map = Status::init(&status_file, &task_info).await?;
            let mut skip = task_info.total_size;
            for (start, end) in &task_map {
                skip -= end - start;
            }
            info!("skip {} , task_map: {}", skip, task_map.len());

            {
                let pb = pb.lock().unwrap();
                pb.inc(skip);
            }

            status_handle = tokio::spawn(Status::run(status_file_bak, receiver_status, Some(pb)));

            download_handle = tokio::spawn(DownloadTask::muti_download(
                task_info.url,
                task_info.num_threads,
                task_info.retry_count,
                task_map,
                client.clone(),
                sender,
                paused.clone(),
                exit.clone(),
            ));
        } else {
            info!("Starting download task");
            status_handle = tokio::spawn(Status::show_display(receiver_status, Some(pb)));

            download_handle = tokio::spawn(simple_download(
                client.clone(),
                task_info.url,
                sender,
                paused.clone(),
                exit.clone(),
            ));
        }

        // 等待 writer_handle 和 task_handle 完成
        let (writer_result, download_result, status_result) =
            tokio::try_join!(writer_handle, download_handle, status_handle)?;
        download_result?;
        writer_result?;
        status_result?;

        // check file

        info!("download file {:?} ", download_file_name);

        if let Some(value) = md5 {
            let md5resut = md5_check(download_file_name.clone()).await?;
            // 比较md5 结果是否一致
            verify_content_md5(md5resut, value.to_str().unwrap_or(""))?
        } else {
            info!("not find Content-md5");
        }
    }
    Ok(())
}
