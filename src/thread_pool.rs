/*! thread pool */
use std::thread::{self};

use crossbeam::channel::{self, Receiver, Sender};

use crate::Result;
/// thread pool trait
pub trait ThreadPool: Sized {
    /// init naive thread pool
    fn new(threads: u32) -> Result<Self>;

    /// spawn a job on thread pool
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}

/// a naive thread pool, create a thread for each job
pub struct NaiveThreadPool;

impl ThreadPool for NaiveThreadPool {
    fn new(_threads: u32) -> Result<Self> {
        Ok(NaiveThreadPool)
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        thread::spawn(job);
    }
}

/// a shared queue thread pool
pub struct SharedQueueThreadPool {
    sender: Sender<Box<dyn FnOnce() + Send + 'static>>,
}

struct QueueReceiver {
    receiver: Receiver<Box<dyn FnOnce() + Send + 'static>>,
}

impl Drop for QueueReceiver {
    fn drop(&mut self) {
        if thread::panicking() {
            let r = Self {
                receiver: self.receiver.clone(),
            };
            thread::spawn(move || run_job(r));
        }
    }
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self> {
        let (sender, receiver) = channel::unbounded();

        for _ in 0..threads {
            let receiver = QueueReceiver {
                receiver: receiver.clone(),
            };

            thread::spawn(move || run_job(receiver));
        }

        Ok(Self { sender })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.sender
            .send(Box::new(job))
            .expect("send job in thread pool failed");
    }
}

fn run_job(r: QueueReceiver) {
    for job in r.receiver.iter() {
        job()
    }
}

/// a thread pool based on rayon
pub struct RayonThreadPool {
    pool: rayon::ThreadPool,
}

impl ThreadPool for RayonThreadPool {
    fn new(threads: u32) -> Result<Self> {
        Ok(Self {
            pool: rayon::ThreadPoolBuilder::new()
                .num_threads(threads as usize)
                .build()?,
        })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.pool.spawn(job)
    }
}
