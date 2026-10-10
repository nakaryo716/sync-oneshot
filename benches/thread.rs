use std::sync::mpsc;
use std::{fmt::Debug, hint::black_box};

use criterion::{criterion_group, criterion_main, Criterion};

struct WrappingSender<S>(Option<S>);
struct WrappingReceiver<R>(Option<R>);

trait Sender<T> {
    type Error;

    fn send(&mut self, val: T) -> Result<(), Self::Error>;
}

trait Receiver<T> {
    type Error;

    fn recv(&mut self) -> Result<T, Self::Error>;
}

/*
 *
 * ===== impl sync_oneshot crate =====
 *
 */
impl<T> Sender<T> for WrappingSender<sync_oneshot::Sender<T>> {
    type Error = T;

    fn send(&mut self, val: T) -> Result<(), Self::Error> {
        let sender = self.0.take().unwrap();
        sender.send(val)
    }
}

impl<T> Receiver<T> for WrappingReceiver<sync_oneshot::Receiver<T>> {
    type Error = sync_oneshot::RecvError;

    fn recv(&mut self) -> Result<T, Self::Error> {
        let receiver = self.0.take().unwrap();
        receiver.recv()
    }
}

fn run_bench<S, R, F>(c: &mut Criterion, name: &str, create_chan: F)
where
    S: Sender<i32>,
    R: Receiver<i32> + Send + 'static,
    F: Fn() -> (S, R),
    <S as Sender<i32>>::Error: Debug,
    <R as Receiver<i32>>::Error: Debug,
{
    let (to_worker, from_main) = mpsc::channel::<R>();
    let (to_main, from_worker) = mpsc::channel::<()>();

    std::thread::spawn(move || {
        while let Ok(mut rx) = from_main.recv() {
            let res = rx.recv().unwrap();
            black_box(res);
            to_main.send(()).unwrap();
        }
    });

    c.bench_function(name, |b| {
        b.iter(|| {
            let (mut tx, rx) = create_chan();
            to_worker.send(rx).unwrap();
            tx.send(black_box(5)).unwrap();
            from_worker.recv().unwrap();
        });
    });
}

/*
 *
 *
 *===== each crate function =====
 *
 */

fn sync_oneshot_thread(c: &mut Criterion) {
    run_bench(c, "sync-oneshot-thread", || {
        let (tx, rx) = sync_oneshot::channel::<i32>();
        (WrappingSender(Some(tx)), WrappingReceiver(Some(rx)))
    });
}

criterion_group!(thread, sync_oneshot_thread);
criterion_main!(thread);
