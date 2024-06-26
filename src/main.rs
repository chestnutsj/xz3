mod download;
use anyhow::{anyhow, Result};
use clap::Parser;
use download::task;
use std::path::PathBuf;
use url::Url;

use std::env;

use log::{ info, LevelFilter};
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
    max_retry: usize,

    /// Sets the log directory
    #[arg(short, long)]
    log: Option<PathBuf>,

    /// Sets the URL to download from
    url: String,
}

fn extract_filename(args: &Args) -> Result<PathBuf, anyhow::Error> {
    let name = if let Some(name) = &args.name {
        name.clone()
    } else {
        if let Ok(parsed_url) = Url::parse(&args.url) {
            // 安全地处理URL路径段
            if let Some(path_segments) = parsed_url.path_segments() {
                if let Some(file_name) = path_segments.last() {
                    PathBuf::from(file_name)
                } else {
                    return Err(anyhow!("can't get url from url"));
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
    // 从环境变量获取日志等级，默认为 `warn`
    let log_level = env::var("LOG_LEVEL").unwrap_or_else(|_| "warn".to_string());
    let log_level = match log_level.to_lowercase().as_str() {
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
            .filter(Box::new(ThresholdFilter::new(LevelFilter::Debug)))
            .build("stderr", Box::new(stdout_appender)),
    );
    let mut log_build = Root::builder().appender("stderr");
    if let Some(logfile) = args.log.clone() {
        // 配置文件输出
        let file_appender = FileAppender::builder()
            .encoder(Box::new(PatternEncoder::new("{d} - {l} - {m}{n}")))
            .build(logfile)
            .unwrap();

        log_config = log_config.appender(
            Appender::builder()
                .filter(Box::new(ThresholdFilter::new(LevelFilter::Info)))
                .build("logfile", Box::new(file_appender)),
        );
        log_build = log_build.appender("logfile");
    }

    let configbuild = log_config.build(log_build.build(log_level)).unwrap();

    // 初始化 log4rs
    log4rs::init_config(configbuild).unwrap();

    info!("download url :{}", args.url);
    let file_name = extract_filename(&args)?;
    info!("download file name {:?}", file_name);

    task::start_single_task(
        &args.url,
        &file_name,
        args.threadsize,
        None,
        Some(args.max_retry),
    )
    .await?;

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
