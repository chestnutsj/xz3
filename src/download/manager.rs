use tokio::sync::mpsc;
use tokio::task;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use anyhow::Result;

pub struct DownloadManager {
    tasks: HashMap<String, (DownloadTask, Arc<Mutex<bool>>, Option<task::JoinHandle<()>>, Arc<FileHandler>, mpsc::Sender<(u64, Vec<u8>)>)>,
}

impl DownloadManager {
    pub fn new() -> Self {
        DownloadManager {
            tasks: HashMap::new(),
        }
    }

    pub fn add_task(&mut self, id: &str, url: &str, output_path: &str, progress_path: &str, total_size: u64, num_threads: usize) -> Result<()> {
        let (sender, receiver) = mpsc::channel(100);
        let paused = Arc::new(Mutex::new(false));
        let exit = Arc::new(Mutex::new(false));
        let file_handler = Arc::new(FileHandler::new(output_path, progress_path));
        let task = DownloadTask::new(url, total_size, num_threads, file_handler.clone());

        let paused_clone = paused.clone();
        let exit_clone = exit.clone();
        let handle = tokio::spawn(async move {
            task.run(sender.clone(), paused_clone, exit_clone).await.unwrap();
        });

        let file_handler_clone = file_handler.clone();
        let writer_handle = tokio::spawn(async move {
            file_handler_clone.start(receiver).await.unwrap();
        });

        self.tasks.insert(id.to_string(), (task, paused, Some(handle), file_handler, sender));
        Ok(())
    }

    pub fn pause_task(&self, id: &str) {
        if let Some((_, paused, _, _, _)) = self.tasks.get(id) {
            let mut paused = paused.lock().unwrap();
            *paused = true;
        }
    }

    pub fn resume_task(&self, id: &str) {
        if let Some((_, paused, _, _, _)) = self.tasks.get(id) {
            let mut paused = paused.lock().unwrap();
            *paused = false;
        }
    }

    pub fn stop_task(&self, id: &str) {
        if let Some((_, _, _, file_handler, _)) = self.tasks.get(id) {
            file_handler.stop();
        }
    }
}
