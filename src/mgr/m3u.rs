use anyhow::{anyhow,Result};
use log4rs::append::file;
use crate::download::manager::DownloadManager;
use core::task;
use std::{f32::consts::E, fs::File, io::{BufRead, BufReader}};
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
 
pub async  fn Start<P: AsRef<std::path::Path>>(path: P, url : Url)  ->Result<()> {
    let task_path = path.as_ref().clone();
    let mut task_list = vec![];

    if  !DownloadManager::check_is_exists(&path) {
       
        let ts_list = read_non_commented_lines(task_path)?;


        let path_buf = path.as_ref().to_path_buf();

      

        let file_name =   path_buf.file_stem().unwrap().to_string_lossy();
        let mut  hidden_path:String;
        if let Some(parent) =  path_buf.parent() {

            match parent.canonicalize() {
                Ok(f) => {
                    hidden_path = format!("{}/.{}", f.to_string_lossy(),file_name);
                },
                Err(_) => {
                    hidden_path = format!("./{}/.{}", parent.to_string_lossy(),file_name);
                }
            }          
        } else {
            hidden_path = format!(".{}",file_name);
        }
        info!("hidden_path:{}",hidden_path);
    
        // 将hidden_path转换为PathBuf
        let hidden_path_buf = PathBuf::from(hidden_path);
    
        match   fs::create_dir_all(&hidden_path_buf)  {
            Ok(())=>{},
            Err(e) => {
                return Err(anyhow!("create hidden directory failed: {} {:?} ",e,hidden_path_buf));
            }
        }
        info!("check m3u tasks ");
        for ts in ts_list {
            let new_file = hidden_path_buf.join( ts.clone());
            let new_url = url.join(&ts)?;
            task_list.push( (new_url,new_file));
        }


    } else {
        warn!("Task already exists {:?}" ,path.as_ref());
    }
    info!("start a task mgr {:?}", task_path);

    let mut taskm= DownloadManager::new(Some(10), Some(task_path.to_string_lossy().into_owned()))?;
   
    info!("start add task to mgr");
    for (task_url, task_file) in task_list {
        taskm.add_task( task_url.to_string(), &task_file.to_path_buf(), 2, None,None );
    }


    let _ = taskm.wait_task_all_done().await;
    // cargo run -- -n sw-331  https://surrit.com/e84dfa8c-97a6-43b5-ad9b-256123fb0d29/1920x1080/video.m3u8 
    info!("m3u8 task has compilte {:?}", task_path);
    Ok(())
}