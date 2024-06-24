use std::path::PathBuf; 
use anyhow::{anyhow, Result};
use log::{info,trace};
use tokio::fs::OpenOptions;
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;
use tokio::fs;
use base64::{engine::general_purpose, Engine as _};


 use crate::download::{
    task::DownloadTask,
    status::get_task_info,
};

pub struct DataChunk {
    pub offset: u64,
    pub data: Vec<u8>,
    pub flush: u64
}

impl DataChunk {
 
    pub fn new(offset: u64, data: Vec<u8>, flush:u64) -> Self {
        DataChunk {
            offset,
            data,
            flush
        }
    }
}

async fn  generate_unique_filename(original: &PathBuf, extension: Option<&str>, start_index: u32) -> PathBuf {
    let mut path = original.to_path_buf();
    let mut index = start_index;
    loop {
        let suffix = format!(" ({})", index);
        if let Some(ext) = extension {
            let mut new_ext = String::from(ext);
            new_ext.push_str(&suffix);
            path.set_extension(&new_ext);
        } else {
            let file_name = path.file_name().and_then(|f| f.to_os_string().into_string().ok())
                .unwrap_or_default();
            let new_file_name = format!("{}{}", file_name, suffix);
            path.set_file_name(new_file_name);
        }

        match fs::metadata(&path).await  {
            Ok(_) => {
                index += 1;
                path = original.to_path_buf(); // 重置路径，准备尝试下一个序号
            },
            Err(_) => {
                return  path;
            }
        }
    }
}

pub async fn checkout_filename(output: PathBuf , status_file: PathBuf ,  task_info: &DownloadTask) -> PathBuf {
    let mut output = output;
    match fs::metadata(&output).await  {
        Ok(_) => {
            match  fs::metadata( status_file.clone()).await {
                Ok(_) => {
                    match  
                     OpenOptions::new()
                    .read(true)
                    .create(false)
                    .open( status_file)
                    .await {
                        Ok(mut file) => {

                            let old_task = get_task_info(&mut file).await;
                            match old_task {
                                Ok(t) => {
                                    if t.url == task_info.url {
                                        return PathBuf::from(t.file_name);
                                    } else {
                                        let extension = output.extension().and_then(|e| e.to_str());
                                        let new_output = generate_unique_filename(&output, extension, 1).await;
                                        output = new_output;
                                    }
                                }
                                Err(_) => {
                                    let extension = output.extension().and_then(|e| e.to_str());
                                    let new_output = generate_unique_filename(&output, extension, 1).await;
                                    output = new_output;
                                }
                            }

                        }
                        Err(_) => {
                        }
                    }
      
                },
                Err(_) => {
                    let extension = output.extension().and_then(|e| e.to_str());
                    let new_output = generate_unique_filename(&output, extension, 1).await;
                    output = new_output;
                }
            }
        },
        Err(_) => {
        }
    }
 
    output
}

pub async fn download_save_file(
    output: PathBuf,
    mut receiver: mpsc::Receiver<DataChunk>,
    status: Option<mpsc::Sender<(u64,u64)>>,
) -> Result<(), anyhow::Error> {
    let sender_ref = status.as_ref();
    let filename = output.clone();
    let mut f = OpenOptions::new()
        .write(true)
        .create(true)
        .open(filename)
        .await?;
 
    loop {
        match receiver.recv().await {
            Some(data) => {
                if data.flush != 0 {
                    f.flush().await?;
                    if let Some(sender) = sender_ref {
                        trace!("sync flush data {} {}", data.offset, data.flush);
                        sender.send( (data.offset,data.flush)).await?;
                    }
                }
                if data.data.is_empty() {
                    continue;
                }
                {
                    trace!("sync write data {} ", data.offset);
                    f.seek(tokio::io::SeekFrom::Start(data.offset)).await?;
                    f.write_all(&data.data).await?;
                }
            }
            None => {
                break;
            }
        }
    }
    info!("save file success {:?}", output);
    f.flush().await?;
    Ok(())
}


pub async  fn md5_check(file_name: PathBuf,) -> Result<Vec<u8>> {
    let f = OpenOptions::new().read(true).open(file_name).await?;
    // Find the length of the file
    let meta = f.metadata().await?;
    

    let buf_len = meta.len().min(1_000_000) as usize;
    let mut buf = BufReader::with_capacity(buf_len, f);
    let mut context = md5::Context::new();
    loop {
        // Get a chunk of the file
      
        let part =  buf.fill_buf().await?;
        // If that chunk was empty, the reader has reached EOF
        if part.is_empty() {
            break;
        }
        // Add chunk to the md5
        context.consume(part);
        // Tell the buffer that the chunk is consumed
        let part_len = part.len();
        buf.consume(part_len);
    }
    let digest = context.compute();
    Ok( digest.to_vec())
} 

pub fn verify_content_md5(local_md5: Vec<u8>, content_md5_base64: &str) -> Result<(),anyhow::Error> {
    match general_purpose::STANDARD.decode(content_md5_base64) {
        Ok(decoded_md5) => {           
            if decoded_md5 == local_md5 {
                info!("MD5 verification passed");
                Ok(())
            } else {
                Err(anyhow!("Content-MD5 verification failed {:?} {:?}", decoded_md5, local_md5))
            }

        },
        Err(e) => {
            Err(anyhow!(e))
        },
    }
}