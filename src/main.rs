//! An executor with task priorities.

use std::future::Future;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use futures_lite::{future, prelude::*};
use futures_lite::future::block_on;
use num_cpus;
use smol::{Executor, Task, Timer};
use smol::future::FutureExt;
use smol::lock::RwLock;
use tracing::{info, trace};
use tracing::Level;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::fmt::Subscriber;

/// Task priority.
#[repr(usize)]
#[derive(Debug, Clone, Copy)]
enum Priority {
    FinishWithinFrame = 0,
    AcrossFrame = 1,
    IO = 2,
}

/// An executor with task priorities.
///
/// Tasks with lower priorities only get polled when there are no tasks with higher priorities.
struct PriorityExecutor<'a> {
    ex: [Executor<'a>; 3],
}


#[derive(Debug)]
struct TaskWithPriority<T>(Task<T>, Priority);

impl<'a> PriorityExecutor<'a> {
    /// Creates a new executor.
    const fn new() -> PriorityExecutor<'a> {
        PriorityExecutor {
            ex: [Executor::new(), Executor::new(), Executor::new()],
        }
    }

    /// Spawns a task with the given priority.
    fn spawn<T: Send + 'a>(
        &self,
        priority: Priority,
        future: impl Future<Output=T> + Send + 'a,
    ) -> TaskWithPriority<T> {
        TaskWithPriority(self.ex[priority as usize].spawn(future), priority)
    }

    /// Runs the executor forever.
    async fn run(&self, sleep_time: Duration) {
        loop {
            block_on(
                async {
                    loop {
                        if !self.ex[0].try_tick() {
                            break;
                        }
                    }
                    let t1 = self.ex[1].tick();
                    let t2 = self.ex[2].tick();

                    future::yield_now()
                        .or(t1)
                        .or(t2)
                        .await;
                }
            );

            // this prevents worker thread from constantly checking for task
            // TODO: find better alternatives like spmc channel
            // this will make frame rate unstable
            //thread::sleep(sleep_time);
        }
    }
}

fn main() {
    let builder = Subscriber::builder();
    builder.with_max_level(Level::INFO).init();
    let executor: PriorityExecutor<'_> = PriorityExecutor::new();
    let executor = Arc::new(executor);

    let num_cpus = num_cpus::get();

    for x in 0..num_cpus {
        let cloned = executor.clone();
        // Spawn a thread running the executor forever.
        let sleep_times = [
            Duration::new(0, 80_000),
            Duration::new(0, 90_000),
            Duration::new(0, 100_000),
            Duration::new(0, 110_000),
            Duration::new(0, 120_000),
            Duration::new(0, 130_000),
        ];
        thread::spawn(move || {
            future::block_on(cloned.run(sleep_times[x % sleep_times.len()]))
        });
    }

    let mut tasks: Vec<TaskWithPriority<_>> = Vec::new();
    let priority_choice = [Priority::FinishWithinFrame, Priority::AcrossFrame, Priority::IO];

    let frame_time_limit = Duration::new(1, 0) / 60;

    info!("Frame limiter: {:?}", frame_time_limit);
    let time_choice = [
        Duration::new(0, 5_000_000),
        Duration::new(0, 50_000_000),
        Duration::new(0, 500_000_000),
    ];

    let epochs: [u32; 3] = [num_cpus as u32 * 3 / 4, num_cpus as u32 * 1 / 2, 1];
    let frames_per_epoch: [u32; 3] = [1, 6, 36];
    info!("Task epochs {:?}", epochs);
    info!("Task periods {:?}", frames_per_epoch);

    let total_cpu_time_per_frame = frame_time_limit * num_cpus as u32;
    let mut used_cpu_time_per_frame = Duration::new(0, 0);
    for i in 0..3 {
        used_cpu_time_per_frame += time_choice[i] * epochs[i] / frames_per_epoch[i];
    }

    info!(
        "\n\
        Used CPU time per frame: {:?}\n\
        Total Available CPU time per frame: {:?}\n\
        ",
        used_cpu_time_per_frame,
        total_cpu_time_per_frame,
        //used_cpu_time_per_frame.as_secs_f64() / total_cpu_time_per_frame.as_secs_f64()
    );

    let mut frame_counters = [0, 0, 0];
    let mut frame_counter = 0;
    let mut frame_time = Instant::now();

    let mut task_completed: Arc<RwLock<[usize; 3]>> = Arc::new(RwLock::new([0, 0, 0]));
    let mut task_dispatched: [usize; 3] = [0, 0, 0];

    let delay = Duration::new(3, 0);
    println!("Test starts in {:?}", delay);
    std::thread::sleep(delay);
    loop {
        if !(frame_counter < 3600) {
            break;
        }
        frame_time = Instant::now();
        for i in 0..3 {
            frame_counters[i] += 1;
            if frame_counters[i] < frames_per_epoch[i] {
                continue;
            }
            let priority = priority_choice[i];
            let time = time_choice[i];
            let epoch = epochs[i];

            task_dispatched[priority as usize] += epoch as usize;
            for _ in 0..epoch {
                tasks.push(
                    executor.spawn(
                        priority,
                        a_task(time, priority, task_completed.clone()),
                    )
                );
            }


            frame_counters[i] = 0;
        }
        trace!("{}: {:?}", tasks.len(), tasks);

        let mut i = 0;
        while i < tasks.len() {
            match tasks[i].1 {
                Priority::FinishWithinFrame => {
                    let task = tasks.swap_remove(i);
                    future::block_on(task.0);
                    //i += 1;
                }
                Priority::AcrossFrame => {
                    i += 1;
                }
                Priority::IO => { i += 1; }
            }
        }
        trace!("{}", tasks.len());

        // frame limiter
        // only tasks that is Priority::FinishWithinFrame are blocking the main loop,
        // there might be no time for other Priority to finish
        // turn off limiter could have a fluctuated frame rate
        // since Priority::AcrossFrame and Priority::IO might build up and exhaust the thread pool,
        // making Priority::FinishWithinFrame have fewer cores to run on
        while (Instant::now() - frame_time) < frame_time_limit {}

        frame_counter += 1;
        if frame_counter % 30 == 0 {
            let task_completed = block_on(task_completed.read());
            info!("Frame time: {:?}", Instant::now() - frame_time);
            info!("Tasks Dispatched: {:?}", task_dispatched);
            info!("Tasks Completed:  {:?}", task_completed);
        }
    }
}

#[inline(never)]
async fn a_task(duration: Duration, priority: Priority, counters: Arc<RwLock<[usize; 3]>>) {
    trace!("{:?} Start", priority);
    let mut start = Instant::now();
    while Instant::now() - start < duration {}
    counters.write().await[priority as usize] += 1;
    trace!("{:?} End", priority);
}

