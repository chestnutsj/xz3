use anyhow::{anyhow, Result};
use bincode::deserialize_from;
use log::{debug, info, trace, warn};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::{path::PathBuf, vec};
use tokio::time::{interval, Duration};
use tokio::{
    fs::{remove_file, OpenOptions},
    io::{AsyncSeekExt, AsyncWriteExt, SeekFrom},
    sync::mpsc,
};

use crate::download::task::DownloadTask;
use indicatif::ProgressBar;
use std::convert::TryInto;

type ChunkStatus = HashMap<u64, u64>;

fn merge_chunks(left: &ChunkStatus, right: &ChunkStatus) -> ChunkStatus {
    debug!("merged {} {}", left.len(), right.len());
    let mut merged = HashMap::new();
    for (k, v) in left.iter() {
        if let Some(v1) = right.get(k) {
            if *v1 != *v {
                merged.insert(*k, *v);
            } else {
                debug!("skip chunk {} {} {}", k, v, v1);
            }
        } else {
            merged.insert(*k, *v);
        }
    }

    debug!("merged {}", merged.len());
    merged
}

pub async fn get_task_info<F>(file: &mut F) -> Result<DownloadTask>
where
    F: tokio::io::AsyncReadExt + Unpin,
{
    let mut buf = [0u8; 8];
    file.read_exact(&mut buf).await?;
    let len = u64::from_le_bytes(buf.try_into().unwrap_or_default());
    if len == 0 {
        return Err(anyhow!("status file header len is empty"));
    }
    let mut buffer = vec![0; len as usize];
    file.read_exact(&mut buffer).await?;
    let task_info: DownloadTask = deserialize_from(&*buffer)?;
    Ok(task_info)
}

pub struct Status {}

impl Status {
    async fn write_task_info(file_path: &PathBuf, info: &DownloadTask) -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(file_path)
            .await?;
        let header = bincode::serialize(info)?;
        let x = header.len() as u64;
        file.write_all(&x.to_le_bytes()).await?;
        file.write_all(&header).await?;
        debug!(
            "write task info to file {:?} len {} {}",
            file_path,
            x,
            header.len()
        );
        Ok(())
    }

    async fn read_progress_entries<F>(file: &mut F) -> Result<ChunkStatus>
    where
        F: tokio::io::AsyncReadExt + Unpin,
    {
        let mut tmp = [0u8; 16];
        let mut set: ChunkStatus = HashMap::new();
        debug!("start read file resume chunk ");
        loop {
            let x = file.read_exact(&mut tmp).await;
            match x {
                Ok(_) => {
                    let offset =
                        u64::from_le_bytes(tmp[0..8].try_into().unwrap_or_else(|_| [0; 8]));
                    let end: u64 =
                        u64::from_le_bytes(tmp[8..16].try_into().unwrap_or_else(|_| [0; 8]));
                    if end == 0 {
                        warn!(" status file is corrupted {} {}", offset, end);
                        break;
                    }
                    set.insert(offset, end);
                }
                Err(_) => {
                    break;
                }
            }
        }
        debug!("end  read file resume chunk ");
        Ok(set)
    }

    pub async fn check_status_info(
        status_file: &PathBuf,
        task_info: &DownloadTask,
    ) -> Result<ChunkStatus> {
        let mut file = OpenOptions::new()
            .read(true)
            .create(false)
            .open(status_file)
            .await?;
        let old_task_info = get_task_info(&mut file).await?;

        if *task_info != old_task_info {
            return Err(anyhow!("status file header has change"));
        }
        let per = Self::read_progress_entries(&mut file).await?;
        Ok(per)
    }

    pub async fn init(status_file: &PathBuf, task_info: &DownloadTask) -> Result<ChunkStatus> {
        let mut x: ChunkStatus = HashMap::new();
        let mut offset = 0;
        while offset < task_info.total_size {
            let end = std::cmp::min(offset + task_info.chunk_size, task_info.total_size);
            x.insert(offset, end);
            offset += task_info.chunk_size;
        }
        match Self::check_status_info(status_file, task_info).await {
            Ok(task_list) => {
                let res_list = merge_chunks(&x, &task_list);
                debug!("res list len {}", res_list.len());
                Ok(res_list)
            }
            Err(_) => {
                Self::write_task_info(status_file, task_info).await?;
                Ok(x)
            }
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

    pub async fn run(
        status_file: PathBuf,
        mut recv: mpsc::Receiver<(u64, u64)>,
        pb: Option<Arc<Mutex<ProgressBar>>>,
    ) -> Result<()> {
        let status_path = status_file.clone();
        {
            let mut f = OpenOptions::new()
                .write(true)
                .create(true)
                .append(true)
                .open(status_file)
                .await?;

            f.seek(SeekFrom::End(0)).await?;
            let mut total_inc = 0;

            let mut interval = interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    Some((start, end)) = recv.recv() => {
                            f.write_all(&start.to_le_bytes()).await?;
                            f.write_all(&end.to_le_bytes()).await?;
                            total_inc += end - start;
                            trace!(" update {} {} " ,start, end);
                        }
                        _ = interval.tick() => {
                            f.flush().await?;
                            if let Some(pb) = pb.as_ref() {
                                let pb = pb.lock().unwrap();
                                pb.inc(total_inc);
                                pb.set_message( Self::format_file_size(total_inc as u64));
                                total_inc= 0;
                                pb.tick();
                            }
                            if recv.is_closed() {
                                break;
                            }
                        },
                }
            }
            f.flush().await?;
        }
        if let Some(pb) = pb.as_ref() {
            let pb = pb.lock().unwrap();
            pb.finish_with_message("download complete");
        }
        info!("remove status file {:?}", status_path.clone());
        remove_file(status_path).await?;
        Ok(())
    }

    pub async fn show_display(
        mut recv: mpsc::Receiver<(u64, u64)>,
        pb: Option<Arc<Mutex<ProgressBar>>>,
    ) -> Result<()> {
        let mut interval = interval(Duration::from_secs(1));
        let mut datalen: u64 = 0;
        loop {
            tokio::select! {
                Some((start, end)) = recv.recv() => {
                    datalen +=  end - start;
                },
                _ = interval.tick() => {

                    if let Some(pb) = pb.as_ref() {
                        let pb = pb.lock().unwrap();
                        pb.inc(datalen);
                        pb.set_message( Self::format_file_size(datalen as u64));
                        datalen= 0;
                        pb.tick();
                    }
                    if recv.is_closed() {
                        break;
                    }
                },
                else => {
                    break;
                }
            }
        }
        info!("status finish");
        if let Some(pb) = pb.as_ref() {
            let pb = pb.lock().unwrap();
            pb.finish_and_clear();
        }
        Ok(())
    }
}
