 
 
use std::path::PathBuf;
use std::sync::mpsc::Receiver;
use std::sync::{Arc,  RwLock};
use tokio::task::JoinHandle;
use log::{debug, info, error};
use std::collections::HashMap;
use anyhow::{Result,anyhow};
use tokio::sync::Notify;
use tokio::sync::Semaphore;
use crate::download::task;
pub struct  TaskCtrl {
    pub exit : Arc<Notify>,
    pub paused: Arc<Notify>,
    pub resume: Arc<Notify>,
    pub handle: JoinHandle<()>,
}
pub struct DownloadManager {
    tasks: Arc<RwLock<HashMap<String, TaskCtrl>>>,
    semaphore: Arc<Semaphore>,
    sender: tokio::sync::mpsc::Sender<String>,
    receiver: tokio::sync::mpsc::Receiver<String>,
    exit: Arc<Notify>,
}

impl DownloadManager {
    pub fn new(num_task : Option<usize>) -> Self {
        let (sender , receiver) = tokio::sync::mpsc::channel(100);
        DownloadManager {
            tasks: Arc::new(RwLock::new( HashMap::new())),
            semaphore : Arc::new(Semaphore::new( num_task.unwrap_or(10))),
            sender,
            receiver,
            exit: Arc::new(Notify::new()),
        }
    }
    pub async fn start(&mut self) {

        loop {
            tokio::select! {
                Some(key) = self.receiver.recv() => {
                   error!("task {} error", key);
                },
                _ = self.exit.notified() => {
                    break;
                }
            }
        }
    }


    pub fn add_task(&mut self,  url: String,
        output_path: &PathBuf,
        num_threads: usize,
        chunk_size_limit: Option<u64>,
        retry_cont: Option<usize>) -> Result<(),anyhow::Error> {
        
        let key = url.clone();
        {   
            match self.tasks.read() {
                Ok(tasks) => {
                    if tasks.contains_key(&key) {
                        return Err(anyhow::anyhow!("Task already exists"));
                    }
                },
                Err(e) => {
                    return Err(anyhow::anyhow!("Error reading tasks: {}", e));
                }
            }
        }


        let exit = Arc::new(Notify::new());
        let exit_clone = exit.clone();
        let paused = Arc::new(Notify::new());
        let paused_clone = paused.clone();
        let resume = Arc::new(Notify::new());
        let resume_clone = resume.clone();
    
        let semaphore = self.semaphore.clone();
        let task_map = self.tasks.clone();
        let file_name =  output_path.clone();
        let task_handle = tokio::spawn( async move {
                let _permit = semaphore.acquire_owned().await.unwrap();
                let task_key = key.clone();
                let res = task::start_single_task(
                    key.clone(),
                    &file_name,
                    num_threads,
                    chunk_size_limit,
                    retry_cont,
                    paused_clone.clone(),
                    resume_clone.clone(),
                    exit_clone.clone(),
                ).await;
                match res {
                    Ok(task_info) => {
                       info!("download task {} success {}", task_info.url, task_info.file_name);
                    },
                    Err(e) => {
                        println!("Task {} failed: {}", key.clone(), e);
                    }
                }

                {
                    let mut locked = task_map.write().unwrap();
                    locked.remove(&task_key);
                }
            }
        );
        let ctrl = TaskCtrl {
            exit,
            paused,
            resume,
            handle:task_handle,
        };

        
        match self.tasks.write() {
            Ok(mut locked) => {
                locked.insert(url, ctrl );
            },
            Err(e) => {
               return Err(anyhow!{"{}",e});
            }
        }
        
        Ok(())
    }

    pub async fn pause_task(&self, id: &str)->Result<(),anyhow::Error> {
        match self.tasks.read() {
            Ok(tasks) =>{
                if let Some(x)= tasks.get(id) {
                    x.paused.notify_waiters()
                }
            }
            Err(e)=>{
                return Err(anyhow!("{}", e));
            }
        }
        Ok(())
    }

    pub async fn resume_task(&self, id: &str)->Result<(),anyhow::Error> {
        match self.tasks.read() {
            Ok(tasks) =>{
                if let Some(x)= tasks.get(id) {
                    x.resume.notify_waiters()
                }
            }
            Err(e)=>{
                return Err(anyhow!("{}", e));
            }
        }
        Ok(())
    }

    pub async fn stop_task(&self, id: &str)->Result<(),anyhow::Error> {
        match self.tasks.read() {
            Ok(tasks) =>{
                if let Some(x)= tasks.get(id) {
                    x.exit.notify_waiters()
                }
            }
            Err(e)=>{
                return Err(anyhow!("{}", e));
            }
        }
        Ok(())
    }
}
