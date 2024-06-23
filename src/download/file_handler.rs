 
use log::info;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::fs::OpenOptions;
use tokio::sync::mpsc;
use anyhow::Result;
 
pub struct DataChunk {
    pub offset: u64,
    pub data: Vec<u8>,
    pub flush: bool,
}

impl DataChunk {
    pub fn default_value() -> Self {
        DataChunk {
            offset: 0,
            data: vec![],
            flush: false,
        }
    }
    pub fn new(offset: u64, data: Vec<u8>, f: Option<bool>) -> Self {
        DataChunk {
            offset,
            data,
            flush: f.unwrap_or(false),
        }
    }
}

pub struct FileHandler {
    output_path: String,
}

impl FileHandler {
    pub fn new(output_path: &str) ->Self  {
        
       FileHandler{
           output_path: output_path.to_string(),
           
        }
    }

    pub async fn start(&self, mut receiver: mpsc::Receiver<DataChunk> , tx  : Option<mpsc::Sender<u64>>) -> Result<(),  anyhow::Error> {

       let sender_ref = tx.as_ref(); // 获取一个可选的引用

       let mut f = OpenOptions::new()
       .write(true)
       .create(true)
       .open(&self.output_path).await?;
        let mut save_date_len = 0;
        loop {
            match  receiver.recv().await {
                Some(data) => {
                    if data.flush {
                        f.flush().await?;
                    }

                    if data.data.is_empty() { 
                        continue;
                    }
                    {
                        f.seek(tokio::io::SeekFrom::Start(data.offset)).await?;
                        f.write_all(&data.data).await?;   
                        save_date_len += data.data.len() ;
                    }

               
                   // 使用 sender_ref 而不是直接解构 Option，避免移动
                    if let Some(sender) = sender_ref {
                        sender.send(save_date_len as u64).await?;
                    }
                },
                None => {
                    break;
                },
            }
        }
        info!("save file success {}" , self.output_path);
        f.flush().await?;
        Ok(())
    }
    /* 
    pub async fn read_progress(&self) -> Result<Vec<(u64, u64)>> {
        if tokio::fs::metadata(&self.progress_path).await.is_ok() {
            let mut file = File::open(&self.progress_path).await?;
            let mut contents = vec![];
            file.read_to_end(&mut contents).await?;
            let progress: Vec<(u64, u64)> = bincode::deserialize(&contents)?;
            Ok(progress)
        } else {
            Ok(vec![])
        }
    }
    */
}
