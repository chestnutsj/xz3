use anyhow::{anyhow, Result};
use bincode::deserialize_from;
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use sled::{Db, IVec};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::str;
use std::sync::{Arc, RwLock};
use tokio::sync::Notify;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

use crate::download::task;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum TaskStatus {
    Init,
    Inprogress,
    Completed,
    Failed(String), // Store failure reason
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct InfoStatus {
    pub url: String,
    pub file_name: String,
    pub chunk_size: u64,
    pub num_threads: usize,
    pub retry_count: usize,
    pub status: TaskStatus,
}

impl InfoStatus {
    pub fn new(
        url: String,
        file_name: PathBuf,
        num_threads: usize,
        chunk_size_limit: Option<u64>,
        retry_cont: Option<usize>,
    ) -> Self {
        InfoStatus {
            url: url,
            file_name: file_name.to_string_lossy().into_owned(),
            chunk_size: chunk_size_limit.unwrap_or(task::CHUNK_SIZE_LIMIT),
            retry_count: retry_cont.unwrap_or(task::MAX_RETRIES),
            num_threads,
            status: TaskStatus::Init,
        }
    }
}

pub struct TaskCtrl {
    pub exit: Arc<Notify>,
    pub paused: Arc<Notify>,
    pub resume: Arc<Notify>,
    pub handle: JoinHandle<()>,
    pub info: InfoStatus,
}
pub struct DownloadManager {
    tasks: Arc<RwLock<HashMap<String, TaskCtrl>>>,
    semaphore: Arc<Semaphore>,
    // sender: tokio::sync::mpsc::Sender<String>,
    // receiver: tokio::sync::mpsc::Receiver<String>,
    exit: Arc<Notify>,
    db: Option<Arc<Db>>,
}

const DEFAULT_STATUS: &str = "xz3.db";

impl DownloadManager {
    pub fn check_is_exists<P: AsRef<std::path::Path>>(path: P) -> bool {
        let mut path_buf = path.as_ref().to_path_buf().clone();
        if !path_buf.ends_with(DEFAULT_STATUS) {
            path_buf.set_extension(DEFAULT_STATUS);
        }

        if fs::metadata(&path_buf).is_ok() {
            return true;
        } else {
            false
        }
    }

    pub fn new(num_task: Option<usize>, path: Option<String>) -> Result<Self> {
        //  let (sender , receiver) = tokio::sync::mpsc::channel(100);
        if let Some(path) = path {
            let mut path_buf = PathBuf::from(path.clone());
            if !path_buf.ends_with(DEFAULT_STATUS) {
                path_buf.set_extension(DEFAULT_STATUS);
            }
            info!("open db: {:?}", path_buf);
            let db = sled::open(path_buf)?;
            let mut mgr = DownloadManager {
                tasks: Arc::new(RwLock::new(HashMap::new())),
                semaphore: Arc::new(Semaphore::new(num_task.unwrap_or(10))),

                exit: Arc::new(Notify::new()),
                db: Some(Arc::new(db)),
            };
            match mgr.load_cache() {
                Ok(()) => {
                    return Ok(mgr);
                }
                Err(e) => {
                    return Err(e);
                }
            }
        } else {
            Ok(DownloadManager {
                tasks: Arc::new(RwLock::new(HashMap::new())),
                semaphore: Arc::new(Semaphore::new(num_task.unwrap_or(10))),

                exit: Arc::new(Notify::new()),
                db: None,
            })
        }
    }

    fn Drop(&mut self) {
        self.exit.notify_waiters();
        match self.tasks.write() {
            Ok(mut locked) => {
                for (_, task) in locked.iter_mut() {
                    task.exit.notify_waiters();
                }
            }
            Err(e) => {
                error!("{}", e);
            }
        }
    }

    fn load_cache(&mut self) -> Result<()> {
        let mut tlist = vec![];

        if let Some(db) = &self.db {
            for result in db.iter() {
                let (key, value) = result?;
                info!("find history task: {:?}", key);
                let task: InfoStatus = deserialize_from(value.as_ref())?;
                //  println!("Key: {}, Task: {:?}", str::from_utf8(&key).unwrap(), task);
                tlist.push(task);
            }
        }

        for task in tlist {
            if task.status != TaskStatus::Completed {
                self.add_task(
                    task.url,
                    &PathBuf::from(task.file_name),
                    task.num_threads,
                    Some(task.chunk_size),
                    Some(task.retry_count),
                )?;
            }
        }
        Ok(())
    }

    pub fn get(db: Arc<Db>, key: String) -> Result<Option<InfoStatus>, anyhow::Error> {
        let v = db.get(key)?;
        if let Some(v) = v {
            let info: InfoStatus = deserialize_from(v.as_ref())?;
            return Ok(Some(info));
        }

        Ok(None)
    }

    pub fn set(db: Arc<Db>, key: String, task_info: &InfoStatus) -> Result<()> {
        let x = bincode::serialize(task_info)?;
        db.insert(key.as_bytes(), x)?;
        Ok(())
    }

    pub fn add_task(
        &mut self,
        url: String,
        output_path: &PathBuf,
        num_threads: usize,
        chunk_size_limit: Option<u64>,
        retry_cont: Option<usize>,
    ) -> Result<(), anyhow::Error> {
        let key = url.clone();
        {
            match self.tasks.read() {
                Ok(tasks) => {
                    if tasks.contains_key(&key) {
                        return Err(anyhow::anyhow!("Task already exists"));
                    }
                }
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
        let file_name = output_path.clone();

        let i = InfoStatus::new(
            url.clone(),
            output_path.clone(),
            num_threads,
            chunk_size_limit,
            retry_cont,
        );
        let i_clone = i.clone();
        let db = self.db.clone();

        let task_handle = tokio::spawn(async move {
            let _permit = semaphore.acquire_owned().await.unwrap();
            let mut i = i_clone;
            let task_key = key.clone();
            i.status = TaskStatus::Inprogress;
            if let Some(db) = &db {
                _ = Self::set(db.clone(), task_key.clone(), &i);
            }

            let res = task::start_single_task(
                key.clone(),
                &file_name,
                num_threads,
                chunk_size_limit,
                retry_cont,
                paused_clone.clone(),
                resume_clone.clone(),
                exit_clone.clone(),
            )
            .await;
            match res {
                Ok(task_info) => {
                    info!(
                        "download task {} success {}",
                        task_info.url, task_info.file_name
                    );
                    i.status = TaskStatus::Completed;
                    if let Some(db) = &db {
                        _ = Self::set(db.clone(), task_key.clone(), &i);
                    }
                }
                Err(e) => {
                    println!("Task {} failed: {}", key.clone(), e);
                    i.status = TaskStatus::Failed(e.to_string());
                    if let Some(db) = &db {
                        _ = Self::set(db.clone(), task_key.clone(), &i);
                    }
                }
            }

            {
                let mut locked = task_map.write().unwrap();
                locked.remove(&task_key);
            }
        });
        let i_b = i.clone();
        let ctrl = TaskCtrl {
            exit,
            paused,
            resume,
            handle: task_handle,
            info: i,
        };
        info!("save task info{:?}", i_b.clone().url);
        match self.tasks.write() {
            Ok(mut locked) => {
                locked.insert(url.clone(), ctrl);
                if let Some(db) = &self.db {
                    Self::set(db.clone(), url, &i_b)?;
                }
            }
            Err(e) => {
                return Err(anyhow! {"{}",e});
            }
        }

        Ok(())
    }

    pub async fn pause_task(&self, id: &str) -> Result<(), anyhow::Error> {
        match self.tasks.read() {
            Ok(tasks) => {
                if let Some(x) = tasks.get(id) {
                    x.paused.notify_waiters()
                }
            }
            Err(e) => {
                return Err(anyhow!("{}", e));
            }
        }
        Ok(())
    }

    pub async fn resume_task(&self, id: &str) -> Result<(), anyhow::Error> {
        match self.tasks.read() {
            Ok(tasks) => {
                if let Some(x) = tasks.get(id) {
                    x.resume.notify_waiters()
                }
            }
            Err(e) => {
                return Err(anyhow!("{}", e));
            }
        }
        Ok(())
    }

    pub async fn stop_task(&self, id: &str) -> Result<(), anyhow::Error> {
        match self.tasks.read() {
            Ok(tasks) => {
                if let Some(x) = tasks.get(id) {
                    x.exit.notify_waiters()
                }
            }
            Err(e) => {
                return Err(anyhow!("{}", e));
            }
        }
        Ok(())
    }

    pub async fn wait_task_all_done(&self) {
        loop {
            match self.tasks.read() {
                Ok(tasks) => {
                    if tasks.is_empty() {
                        break;
                    }
                }
                Err(e) => {
                    println!("Error reading tasks: {}", e);
                }
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }
}
