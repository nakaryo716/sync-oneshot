use std::{fmt::Debug, hint::black_box};

use criterion::{Criterion, criterion_group, criterion_main};

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

fn bench_function<S, T, R>(mut tx: S, mut rx: R, value: T)
where
    S: Sender<T>,
    R: Receiver<T> + Send + 'static,
    T: Debug,
    <S as Sender<T>>::Error: Debug,
    <R as Receiver<T>>::Error: Debug,
{
    tx.send(black_box(value)).unwrap();
    let res = rx.recv().unwrap();
    black_box(res);
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

/*
 *
 *
 *===== each crate function =====
 *
 */
fn sync_oneshot(c: &mut Criterion) {
    c.bench_function("sync-oneshot-basic", |b| {
        b.iter(|| {
            let (tx, rx) = sync_oneshot::channel();
            let tx = WrappingSender(Some(tx));
            let rx = WrappingReceiver(Some(rx));

            bench_function(tx, rx, 5);
        });
    });
}

criterion_group!(basic, sync_oneshot);
criterion_main!(basic);
