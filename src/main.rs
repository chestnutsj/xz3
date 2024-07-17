mod download;
mod mgr;
use anyhow::{anyhow, Result};
use clap::Parser;
use download::file_handler::is_m3u_or_m3u8_file;
use download::task;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Notify;
use url::Url;
use mgr::m3u;

use log::{error, info, LevelFilter};
use log4rs::{
    append::console::ConsoleAppender,
    append::file::FileAppender,
    config::{Appender, Config, Root},
    encode::pattern::PatternEncoder,
    filter::threshold::ThresholdFilter,
};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// file name
    #[arg(short, long)]
    name: Option<PathBuf>,
    /// Sets the thread
    #[arg(short, long, default_value = "10")]
    threadsize: usize,
    /// Set the retry time
    #[arg(short, long, default_value = "10")]
    retry_max: usize,

    /// Sets the log directory
    #[arg(short, long)]
    log: Option<PathBuf>,

    /// Sets the URL to download from
    #[arg(short,long)]
    m3u8: String,
    /// Sets the URL to download from
    url: Url,
}

fn extract_filename(args: &Args) -> Result<PathBuf, anyhow::Error> {
    let name = if let Some(name) = &args.name {
        name.clone()
    } else {
        let p =   &args.url.path();
        let fll  =  PathBuf::from(p);
        if let Some(n) = fll.file_name() {
            PathBuf::from( n.to_str().unwrap()) 
        } else {
            return Err(anyhow!("Invalid URL"));
        }
    
    };
    Ok(name)
}

#[tokio::main]
async fn main() -> Result<()> {
    // 从环境变量获取日志等级，默认为 `warn`
    let log_level_str = env::var("LOG_LEVEL").unwrap_or_else(|_| "warn".to_string());
    let log_level = match log_level_str.to_lowercase().as_str() {
        "error" => LevelFilter::Error,
        "warn" => LevelFilter::Warn,
        "info" => LevelFilter::Info,
        "debug" => LevelFilter::Debug,
        "trace" => LevelFilter::Trace,
        _ => LevelFilter::Warn,
    };
    let args = Args::parse();
    // 配置控制台输出

    let stdout_appender = ConsoleAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{d} - {l} - {m}{n}")))
        .build();

    let mut log_config = Config::builder().appender(
        Appender::builder()
            .filter(Box::new(ThresholdFilter::new(log_level)))
            .build("stdout", Box::new(stdout_appender)),
    );

    let mut root_build = Root::builder().appender("stdout");
    if let Some(logfile) = args.log.clone() {
        // 配置文件输出
        let file_appender = FileAppender::builder()
            .encoder(Box::new(PatternEncoder::new("{d} - {l} - {m}{n}")))
            .build(logfile)
            .unwrap();
        log_config = log_config.appender(
            Appender::builder()
                .filter(Box::new(ThresholdFilter::new(LevelFilter::Info)))
                .build("file", Box::new(file_appender)),
        );
        root_build = root_build.appender("file");
    }

    let log_build = log_config
        .build(root_build.build(LevelFilter::Trace))
        .unwrap();

    log4rs::init_config(log_build).unwrap();

    info!("download url {}", args.url);
    let file_name = extract_filename(&args)?;
    info!("download file name {:?}", file_name);
    let exit_ctl = Arc::new(Notify::new());
    let paused_ctl = Arc::new(Notify::new());
    let resume_ctl = Arc::new(Notify::new());
    match task::start_single_task(
        args.url.to_string().clone(),
        &file_name,
        args.threadsize,
        None,
        Some(args.retry_max),
        paused_ctl.clone(),
        resume_ctl.clone(),
        exit_ctl.clone(),
    )
    .await
    {
        Ok(task_info) => {
            info!("download success {}", task_info.file_name);
            if is_m3u_or_m3u8_file(task_info.file_name.clone()) {
                info!("m3u file has download");
                m3u::Start( PathBuf::from(&task_info.file_name), args.url).await?;
            }
        }
        Err(e) => {
            error!("{} download failed {} ", args.url, e);
            return Err(e);
        }
    }

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
