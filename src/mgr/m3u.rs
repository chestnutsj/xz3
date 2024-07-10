use anyhow::Result;
use crate::download::manager::DownloadManager;
pub fn start_m3u(file:String,) ->Result<()> {
    
    let taskm= DownloadManager::new(10);


    // cargo run -- -n sw-331  https://surrit.com/e84dfa8c-97a6-43b5-ad9b-256123fb0d29/1920x1080/video.m3u8 
    Ok(())
}