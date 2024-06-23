
mod download;
use anyhow::{Result,anyhow};
use clap::Parser;
use url::Url;

use download::task;
use log::info;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// file name
    #[arg(short, long)]
    name: Option<String>,
    /// Sets the thread
    #[arg(short, long, default_value = "10")]
    threadsize: usize,
    /// Set the retry time
    #[arg(short, long, default_value = "10")]
    max_retry: usize,

    /// Sets the URL to download from
    url: String,
}

fn extract_filename(args: &Args) -> Result<String,anyhow::Error> {
    let name = if let Some(name) = &args.name {
        name.clone()
    } else {
        if let Ok(parsed_url) = Url::parse(&args.url) {
            // 安全地处理URL路径段
            if let Some(path_segments) = parsed_url.path_segments() {
                if let Some(file_name) = path_segments.last() {
                    file_name.to_owned()
                } else {
                    return Err( anyhow!( "can't get url from url"));
                }
            } else {
                return Err(anyhow!("无法解析URL路径段"));
            }
        } else {
            return Err(anyhow!("无效的URL"));
        }
    };
    Ok(name)
}


#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    
    let args = Args::parse();
    info!("download url :{}",args.url);
    let file_name = extract_filename(&args)?; 
    info!("download file name :{}",file_name);

    task::start_single_task(&args.url,&file_name.as_str(),  args.threadsize , None, Some(args.max_retry)).await?;

   // let mut manager = DownloadManager::new();

   //  manager.add_task(file_name.as_str(), &args.url,&file_name.as_str(),  args.threadsize)?;

   // 暂停任务
   // manager.pause_task(file_name.as_str());

    // 恢复任务
   // manager.resume_task(file_name.as_str());

    // 停止任务
   // manager.stop_task(file_name.as_str());

    Ok(())
}