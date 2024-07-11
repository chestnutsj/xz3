use anyhow::{anyhow,Result};
use log4rs::append::file;
use crate::download::manager::DownloadManager;
use core::task;
use std::{fs::File, io::{BufRead, BufReader}};
use url::Url;
use std::path::{Path, PathBuf};
use std::fs;
use log::{info,error,warn};
fn read_non_commented_lines<P: AsRef<std::path::Path>>(file_path: P) -> Result<Vec<String>> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);

    let mut non_commented_lines = Vec::new();

    for line_result in reader.lines() {
        let line = line_result?;
        if !line.starts_with('#') {
            non_commented_lines.push(line);
        }
    }
    return Ok(non_commented_lines) ;
}
 
pub fn Start<P: AsRef<std::path::Path>>(path: P, url : Url)  ->Result<()> {
    let task_path = path.as_ref().clone();
    let mut task_list = vec![];

    if  !DownloadManager::check_is_exists(&path) {
       
        let ts_list = read_non_commented_lines(task_path)?;


        let path_buf = path.as_ref().to_path_buf();
        let mut  hidden_path:String;
        if let Some(parent) =  path_buf.parent() {
            hidden_path = format!("{}/.{}", parent.to_string_lossy(),path_buf.display());
        } else {
            hidden_path = format!(".{}",path_buf.display());
        }

    
        // 将hidden_path转换为PathBuf
        let hidden_path_buf = PathBuf::from(hidden_path);
    
        // 检查并创建隐藏目录
        if let Err(e) = fs::create_dir_all(&hidden_path_buf) {
            error!("create hidden directory failed: {:?}",hidden_path_buf);
        }
        for ts in ts_list {
            let new_file = hidden_path_buf.join( ts.clone());
            let new_url = url.join(&ts)?;
            task_list.push( (new_url,new_file));
        }


    } else {
        warn!("Task already exists {:?}" ,path.as_ref());
    }
    let mut taskm= DownloadManager::new(Some(10), Some(task_path.to_string_lossy().into_owned()))?;

  
     
    for (task_url, task_file) in task_list {
        taskm.add_task( task_url.to_string(), &task_file.to_path_buf(), 2, None,None );
    }


    taskm.wait_task_all_done(); 
    // cargo run -- -n sw-331  https://surrit.com/e84dfa8c-97a6-43b5-ad9b-256123fb0d29/1920x1080/video.m3u8 
    Ok(())
}