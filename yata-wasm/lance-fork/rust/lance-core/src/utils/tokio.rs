// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use crate::Result;
use futures::{Future, FutureExt};

#[cfg(not(target_arch = "wasm32"))]
mod native {
    use super::*;
    use std::time::Duration;
    use tokio::runtime::{Builder, Runtime};

    pub fn get_num_compute_intensive_cpus() -> usize {
        if let Ok(user_specified) = std::env::var("LANCE_CPU_THREADS") {
            return user_specified.parse().unwrap();
        }
        let cpus = num_cpus::get();
        if cpus <= *IO_CORE_RESERVATION {
            if cpus > 2 {
                log::warn!("Number of CPUs <= IO core reservations. Using 1 CPU for compute.");
            }
            return 1;
        }
        num_cpus::get() - *IO_CORE_RESERVATION
    }

    lazy_static::lazy_static! {
        pub static ref IO_CORE_RESERVATION: usize = std::env::var("LANCE_IO_CORE_RESERVATION").unwrap_or("2".to_string()).parse().unwrap();
        pub static ref CPU_RUNTIME: Runtime = Builder::new_multi_thread()
            .thread_name("lance-cpu")
            .max_blocking_threads(get_num_compute_intensive_cpus())
            .worker_threads(1)
            .thread_keep_alive(Duration::from_secs(u64::MAX))
            .build()
            .unwrap();
    }

    pub fn spawn_cpu<F: FnOnce() -> Result<R> + Send + 'static, R: Send + 'static>(
        func: F,
    ) -> impl Future<Output = Result<R>> {
        let (send, recv) = tokio::sync::oneshot::channel();
        let span = tracing::Span::current();
        CPU_RUNTIME.spawn_blocking(move || {
            let _span_guard = span.enter();
            let result = func();
            let _ = send.send(result);
        });
        recv.map(|res| res.unwrap())
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub use native::*;

/// WASM: spawn_cpu runs synchronously inline (no thread pool).
#[cfg(target_arch = "wasm32")]
pub fn spawn_cpu<F: FnOnce() -> Result<R> + Send + 'static, R: Send + 'static>(
    func: F,
) -> impl Future<Output = Result<R>> {
    futures::future::ready(func())
}

#[cfg(target_arch = "wasm32")]
pub fn get_num_compute_intensive_cpus() -> usize { 1 }
