#[cfg(loom)]
use loom::thread;

#[cfg(not(loom))]
use std::panic;
#[cfg(not(loom))]
use std::time::Instant;
#[cfg(not(loom))]
use std::{thread, time::Duration};

use sync_oneshot::{TryRecvError, channel};

#[cfg(not(loom))]
use sync_oneshot::RecvTimeoutError;

#[test]
fn test_local() {
    let test_inner = || {
        let (tx, rx) = channel();

        tx.send(5).unwrap();

        let result = rx.recv().unwrap();
        assert_eq!(result, 5);
    };

    #[cfg(loom)]
    loom::model(test_inner);

    #[cfg(not(loom))]
    test_inner();
}

#[test]
fn test_thread_tx() {
    let test_inner = || {
        let (tx, rx) = channel();

        thread::spawn(move || {
            thread::yield_now();
            tx.send(5).unwrap();
        });

        let result = rx.recv().unwrap();
        assert_eq!(result, 5);
    };

    #[cfg(loom)]
    loom::model(test_inner);

    #[cfg(not(loom))]
    test_inner();
}

#[test]
fn test_thread_rx() {
    let test_inner = || {
        let (tx, rx) = channel();

        let result = thread::spawn(move || rx.recv().unwrap());

        thread::yield_now();

        tx.send(5).unwrap();
        assert_eq!(result.join().unwrap(), 5);
    };

    #[cfg(loom)]
    loom::model(test_inner);

    #[cfg(not(loom))]
    test_inner();
}

#[test]
fn test_rx_already_closed() {
    let test_inner = || {
        let (tx, rx) = channel();

        drop(rx);

        let result = tx.send(5);
        assert!(result.is_err());
    };

    #[cfg(loom)]
    loom::model(test_inner);

    #[cfg(not(loom))]
    test_inner();
}

#[test]
fn test_tx_already_closed() {
    let test_inner = || {
        let (tx, rx) = channel::<i32>();
        drop(tx);

        assert!(rx.recv().is_err());
    };

    #[cfg(loom)]
    loom::model(test_inner);

    #[cfg(not(loom))]
    test_inner();
}

#[test]
fn test_tx_already_closed_wait() {
    let test_inner = || {
        let (tx, rx) = channel::<i32>();

        thread::spawn(move || {
            thread::yield_now();
            drop(tx);
        });

        assert!(rx.recv().is_err());
    };

    #[cfg(loom)]
    loom::model(test_inner);

    #[cfg(not(loom))]
    test_inner();
}

#[test]
fn test_rx_close() {
    let test_inner = || {
        let (tx, mut rx) = channel::<i32>();

        rx.close();
        assert!(tx.send(5).is_err());
    };

    #[cfg(loom)]
    loom::model(test_inner);

    #[cfg(not(loom))]
    test_inner();
}

#[cfg(not(loom))]
#[test]
fn test_rx_close_thread() {
    let (tx, mut rx) = channel();

    std::thread::spawn(move || {
        rx.close();
    });

    thread::sleep(std::time::Duration::from_millis(500));
    assert!(tx.send(5).is_err());
}

#[test]
fn test_rx_recv_after_close() {
    let test_inner = || {
        let (_tx, mut rx) = channel::<i32>();

        rx.close();
        assert!(rx.recv().is_err());
    };

    #[cfg(loom)]
    loom::model(test_inner);

    #[cfg(not(loom))]
    test_inner();
}

#[test]
fn test_rx_recv_after_send_and_close() {
    let test_inner = || {
        let (tx, mut rx) = channel();

        tx.send(5).unwrap();
        rx.close();

        assert_eq!(5, rx.recv().unwrap());
    };

    #[cfg(loom)]
    loom::model(test_inner);

    #[cfg(not(loom))]
    test_inner();
}

#[test]
fn test_is_closed() {
    let test_inner = || {
        let (tx, mut rx) = channel::<i32>();
        rx.close();

        assert!(tx.is_closed());
    };

    #[cfg(loom)]
    loom::model(test_inner);

    #[cfg(not(loom))]
    test_inner();
}

#[test]
#[should_panic]
fn recv_after_try_recv() {
    let test_inner = || {
        let (tx, mut rx) = channel();

        tx.send(5).unwrap();
        assert_eq!(rx.try_recv().unwrap(), 5);
        rx.recv().unwrap();
    };

    #[cfg(loom)]
    loom::model(test_inner);

    #[cfg(not(loom))]
    test_inner();
}

#[test]
fn try_recv_after_close() {
    let test_inner = || {
        let (tx, mut rx) = channel::<i32>();

        drop(tx);
        assert_eq!(rx.try_recv(), Err(TryRecvError::Closed));
    };

    #[cfg(loom)]
    loom::model(test_inner);

    #[cfg(not(loom))]
    test_inner();
}

#[test]
fn try_recv_empty() {
    let test_inner = || {
        let (_tx, mut rx) = channel::<i32>();

        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    };

    #[cfg(loom)]
    loom::model(test_inner);

    #[cfg(not(loom))]
    test_inner();
}

#[test]
fn send_close_recv() {
    let test_inner = || {
        let (tx, mut rx) = channel();
        tx.send(5).unwrap();
        rx.close();
        assert_eq!(5, rx.try_recv().unwrap());

        let (tx, mut rx) = channel();
        tx.send(5).unwrap();
        rx.close();
        assert_eq!(5, rx.recv().unwrap());
    };

    #[cfg(loom)]
    loom::model(test_inner);

    #[cfg(not(loom))]
    test_inner();
}

#[test]
fn test_race_send_drop() {
    let test_inner = || {
        let (tx, rx) = channel();

        thread::spawn(move || {
            let _ = tx.send(5);
        });

        drop(rx);
    };

    #[cfg(loom)]
    loom::model(test_inner);

    #[cfg(not(loom))]
    test_inner();
}

#[cfg(not(loom))]
#[test]
fn recv_deadline_ok() {
    let (tx, mut rx) = channel::<i32>();
    tx.send(5).unwrap();

    let res = rx.recv_deadline(Instant::now()).unwrap();
    assert_eq!(res, 5);
}

#[cfg(not(loom))]
#[test]
fn recv_deadline_closed() {
    let (_tx, mut rx) = channel::<i32>();

    rx.close();

    match rx.recv_deadline(Instant::now()) {
        Err(RecvTimeoutError::Closed) => {}
        _ => panic!("expected Closed Error"),
    }
}

#[cfg(not(loom))]
#[test]
fn send_never_deadline() {
    let (tx, mut rx) = channel::<i32>();
    std::mem::drop(tx);

    match rx.recv_deadline(Instant::now()) {
        Err(RecvTimeoutError::Closed) => {}
        _ => panic!("expected Closed Error"),
    }
}

#[cfg(not(loom))]
#[test]
fn recv_no_timeout() {
    let (_tx, mut rx) = channel::<i32>();

    match rx.recv_deadline(Instant::now()) {
        Err(RecvTimeoutError::Timeout) => {}
        _ => panic!("expected Timeout Error"),
    }
}

#[cfg(not(loom))]
#[test]
fn recv_timeout_send() {
    let (tx, mut rx) = channel::<i32>();

    match rx.recv_deadline(Instant::now()) {
        Err(RecvTimeoutError::Timeout) => {}
        _ => panic!("expected Timeout Error"),
    }

    tx.send(5).unwrap();
    assert_eq!(rx.recv().unwrap(), 5);
}

#[cfg(not(loom))]
#[test]
fn recv_deadline_pass() {
    let (_tx, mut rx) = channel::<i32>();

    let time = Instant::now();
    let timeout = Duration::from_millis(100);

    match rx.recv_deadline(time + timeout) {
        Err(RecvTimeoutError::Timeout) => {}
        _ => panic!("expected Timeout Error"),
    }

    assert!(time.elapsed() >= timeout);
}

#[cfg(not(loom))]
#[test]
fn recv_timeout_ok() {
    let (tx, mut rx) = channel();

    let timeout = Duration::from_millis(100);
    tx.send(5).unwrap();

    assert_eq!(rx.recv_timeout(timeout).unwrap(), 5);
}

#[cfg(not(loom))]
#[test]
fn recv_timeout_closed() {
    let (_tx, mut rx) = channel::<i32>();

    let timeout = Duration::from_millis(100);

    rx.close();

    match rx.recv_timeout(timeout) {
        Err(RecvTimeoutError::Closed) => {}
        _ => panic!("expected Closed Error"),
    }

    let (tx, mut rx) = channel::<i32>();

    let timeout = Duration::from_millis(100);

    drop(tx);

    match rx.recv_timeout(timeout) {
        Err(RecvTimeoutError::Closed) => {}
        _ => panic!("expected Closed Error"),
    }
}

#[cfg(not(loom))]
#[test]
fn recv_timeout_pass() {
    let (_tx, mut rx) = channel::<i32>();

    let time = Instant::now();
    let timeout = Duration::from_millis(100);

    match rx.recv_timeout(timeout) {
        Err(RecvTimeoutError::Timeout) => {}
        _ => panic!("expected Timeout Error"),
    }

    assert!(time.elapsed() >= timeout);
}

#[cfg(not(loom))]
#[test]
fn recv_timeout_err_send() {
    let (tx, mut rx) = channel();

    let timeout = Duration::from_millis(100);

    match rx.recv_timeout(timeout) {
        Err(RecvTimeoutError::Timeout) => {}
        _ => panic!("expected Timeout Error"),
    }

    tx.send(5).unwrap();

    assert_eq!(rx.recv_timeout(timeout).unwrap(), 5);
}
