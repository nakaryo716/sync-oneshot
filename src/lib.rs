//! A minimal oneshot channel for synchronous Rust.
//!
//! A oneshot channel is used for sending a single message between threads.
//! The [`channel`] function is used to create a [`Sender`] and [`Receiver`]
//! handle pair that form the channel.
//!
//! - The [`Sender`] handle is used by the producer to send the value.
//! - The [`Receiver`] handle is used by the consumer to receive the value.
//!
//! Each handle can be used on other threads.
//!
//! - [`Sender::send`] will no block the calling thread.
//! - [`Receiver::recv`] will **block** the calling thread.
//!
//! # Example
//! ```rust
//! # use std::time::Duration;
//! let (tx, rx) = sync_oneshot::channel();
//!
//! std::thread::spawn(move || {
//!     std::thread::sleep(Duration::from_millis(200));
//!     tx.send(5).unwrap();
//! });
//!
//! // blocking thread until a message available
//! let val = rx.recv().unwrap();
//! assert_eq!(val, 5);
//! ```
#[cfg(loom)]
use loom::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread,
};

use std::{fmt, time::Instant};
#[cfg(not(loom))]
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread,
};

use std::time::Duration;

use crate::{notify::Notify, slot::Slot};

mod error;
mod notify;
mod slot;

pub use error::{RecvError, RecvTimeoutError, TryRecvError};

/// Creates a new oneshot channel, returning the sender/receiver halves.
///
/// The [`Sender`] is used by the producer to send the value.
/// The [`Receiver`] handle is used by the consumer to receive the value.
///
/// [`send`](Sender::send) will no block the calling thread. [`recv`](Receiver::recv)
/// will **block** until a message is available.
#[inline]
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Inner {
        state: AtomicUsize::new(0),
        value: Slot::new(),
        notify: Notify::new(),
    });

    (
        Sender {
            inner: Some(inner.clone()),
        },
        Receiver { inner: Some(inner) },
    )
}

/// Sends a value to the associated [`Receiver`].
///
/// This is created by the [`channel`] function.
/// Messages can be sent using [`send`](Sender::send).
#[derive(Debug)]
pub struct Sender<T> {
    inner: Option<Arc<Inner<T>>>,
}

/// Receive a value from the associated [`Sender`].
///
/// This is created by the [`channel`] function.
/// Messages sent to the channel can be retrieved using [`recv`](Receiver::recv).
/// [`recv`](Receiver::recv) method blocks thread.
#[derive(Debug)]
pub struct Receiver<T> {
    inner: Option<Arc<Inner<T>>>,
}

unsafe impl<T> Send for Sender<T> where T: Send {}
unsafe impl<T> Sync for Sender<T> where T: Send {}

unsafe impl<T> Send for Receiver<T> where T: Send {}
unsafe impl<T> Sync for Receiver<T> where T: Send {}

struct Inner<T> {
    state: AtomicUsize,
    value: Slot<T>,
    notify: Notify,
}

/*
 *
 * ===== impl Sender =====
 *
 */
impl<T> Sender<T> {
    /// Attempts to send a value on this channel, returning it back if it could not be sent.
    ///
    /// A successful send occurs when it is determined that the other end of the
    /// channel has not hung up already. An unsuccessful send would be one where
    /// the corresponding receiver has already been deallocated. Note that a
    /// return value of [`Err`] means that the data will never be received, but
    /// a return value of [`Ok`] does *not* mean that the data will be received.
    /// It is possible for the corresponding receiver to hang up immediately
    /// after this function returns [`Ok`].
    ///
    /// This method will never block the current thread.
    /// # Example
    /// ```rust
    /// let (tx, rx) = sync_oneshot::channel();
    /// std::thread::spawn(move || {
    ///     if let Err(e) = tx.send(5) {
    ///         println!("the receiver dropped");
    ///     }
    /// });
    ///
    /// match rx.recv() {
    ///     Ok(v) => println!("got = {:?}", v),
    ///     Err(_) => println!("the sender dropped"),
    /// }
    /// ```
    #[inline]
    pub fn send(mut self, value: T) -> Result<(), T> {
        // take inner
        // The case inner None is unreachable
        let inner = self.inner.take().unwrap();

        // set value
        unsafe {
            // SAFETY:
            // Receiver don't access inner value until set status as VALUE_SENT
            inner.value.set(value);
        }

        // set state as VALUE_SEND and notify
        let prev_state = inner.set_complete();

        if prev_state.is_closed() {
            // SAFETY:
            // Receiver already has been droped. So can access inner value.
            return Err(unsafe { inner.consume_value().unwrap() });
        }

        if prev_state.is_waiting() {
            unsafe {
                inner.notify();
            }
        }

        Ok(())
    }

    /// Returns true if the associated Receiver handle has been dropped.
    ///
    /// A Receiver is closed by either calling close explicitly or the Receiver value is dropped.
    /// If true is returned, a call to send will always result in an error.
    pub fn is_closed(&self) -> bool {
        let inner = self.inner.as_ref().unwrap();
        State(inner.state.load(Ordering::Acquire)).is_closed()
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            let prev_state = inner.set_complete();

            if prev_state.is_waiting() {
                unsafe {
                    inner.notify.notify();
                }
            }
        }
    }
}

/*
 *
 * ===== impl Receiver =====
 *
 */
impl<T> Receiver<T> {
    /// Attempts to wait for a value on this receiver, returning an error if
    /// the corresponding channel has hung up.
    ///
    /// This function will always block the current thread if there is no data
    /// available. Once a message is sent to the corresponding [`Sender`],
    /// this receiver will wake up and return that message.
    ///
    /// If the corresponding [`Sender`] has disconnected, or it disconnects while
    /// this call is blocking, this call will wake up and return [`Err`] to
    /// indicate that no more messages can ever be received on this channel.
    /// # Example
    /// ```rust
    /// let (tx, rx) = sync_oneshot::channel();
    ///
    /// let th_handle = std::thread::spawn(move || {
    ///     tx.send(5).unwrap();
    /// });
    ///
    /// th_handle.join().unwrap();
    ///
    /// assert_eq!(5, rx.recv().unwrap());
    /// ```
    #[inline]
    pub fn recv(self) -> Result<T, RecvError> {
        self.recv_inner(None).map_err(|_| RecvError)
    }

    /// Attempts to return a pending value on this receiver without blocking.
    ///
    /// This method will never block the caller in order to wait for data to
    /// become available. Instead, this will always return immediately with a
    /// possible option of pending data on the channel.
    ///
    /// This is useful for a flavor of “optimistic check” before deciding to
    /// block on a receiver.
    ///
    /// Compared with recv, this function has two failure cases instead of one (one for disconnection, one for an empty buffer).
    #[inline]
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let result = if let Some(inner) = self.inner.as_ref() {
            let state = State(inner.state.load(Ordering::Acquire));

            if state.is_complete() {
                unsafe {
                    // SAFETY:
                    // When state is complete, Sender no longer access value
                    // Can access value safely
                    match inner.consume_value() {
                        Some(value) => Ok(value),
                        None => Err(TryRecvError::Closed),
                    }
                }
            } else if state.is_closed() {
                Err(TryRecvError::Closed)
            } else {
                return Err(TryRecvError::Empty);
            }
        } else {
            Err(TryRecvError::Closed)
        };

        self.inner = None;
        result
    }

    /// Attempts to wait for a value on this receiver,
    /// returning an error if the corresponding [`Sender`] half of
    /// this channel has been dropped, or if deadline is reached.
    #[cfg(not(loom))]
    pub fn recv_deadline(&mut self, deadline: Instant) -> Result<T, RecvTimeoutError> {
        let result = self.recv_inner(Some(deadline));

        match result {
            Ok(_) | Err(RecvTimeoutError::Closed) => {
                self.inner = None;
            }
            _ => {}
        }
        result
    }

    /// Attempts to wait for a value on this receiver, returning an error if the
    /// corresponding channel has hung up, or if it waits more than `timeout`.
    ///
    /// This function will always block the current thread if there is no data
    /// available and it's possible for more data to be sent.
    /// Once a message is sent to the corresponding [`Sender`]
    /// this receiver will wake up and return that message.
    ///
    /// If the corresponding [`Sender`] has dropped, or it disconnects while
    /// this call is blocking, this call will wake up and return [`Err`] to
    /// indicate that no more messages can ever be received on this channel.
    /// However, since channels are buffered, messages sent before the disconnect
    /// will still be properly received.
    #[cfg(not(loom))]
    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        match Instant::now().checked_add(timeout) {
            Some(deadline) => self.recv_deadline(deadline),
            // waits until Sender sent a value (same Receiver::recv)
            // if return type of checked_add None, it will
            // overflow and wait indefinitely
            None => self.recv_inner(None),
        }
    }

    fn recv_inner(&self, deadline: Option<Instant>) -> Result<T, RecvTimeoutError> {
        if let Some(inner) = self.inner.as_ref() {
            let mut state = inner.state.load(Ordering::Acquire);
            loop {
                if State(state).is_complete() {
                    unsafe { break inner.consume_value().ok_or(RecvTimeoutError::Closed) }
                }

                if State(state).is_closed() {
                    break Err(RecvTimeoutError::Closed);
                }

                if !State(state).is_waiting() {
                    unsafe {
                        inner.notify.set_current();
                    }
                }

                if let Err(actual) = inner.state.compare_exchange_weak(
                    state,
                    state | WAITING,
                    Ordering::Release,
                    Ordering::Acquire,
                ) {
                    state = actual;
                    continue;
                }

                match deadline {
                    None => thread::park(),
                    Some(d) => match d.checked_duration_since(Instant::now()) {
                        Some(dur) => park_for(dur),
                        None => {
                            let prev = inner.state.fetch_and(!WAITING, Ordering::Acquire);
                            if State(prev).is_complete() || State(prev).is_closed() {
                                state = prev;
                                continue;
                            }
                            break Err(RecvTimeoutError::Timeout);
                        }
                    },
                }
                state = inner.state.load(Ordering::Acquire);
            }
        } else {
            Err(RecvTimeoutError::Closed)
        }
    }

    /// Prevents the associated [`Sender`] handle from sending a value.
    ///
    /// Any `send` operation which happens after calling `close` is guaranteed
    /// to fail. After calling `close`, [`try_recv`] should be called to
    /// receive a value if one was sent **before** the call to `close`
    /// completed.
    ///
    /// This function is useful to perform a graceful shutdown and ensure that a
    /// value will not be sent into the channel and never received.
    ///
    /// `close` is no-op if a message is already received or the channel
    /// is already closed.
    ///
    /// [`Sender`]: Sender
    /// [`try_recv`]: Receiver::try_recv
    ///
    /// # Examples
    ///
    /// Prevent a value from being sent
    ///
    /// ```
    /// use sync_oneshot::TryRecvError;
    ///
    /// # fn main() {
    /// let (tx, mut rx) = sync_oneshot::channel();
    ///
    /// assert!(!tx.is_closed());
    ///
    /// rx.close();
    ///
    /// assert!(tx.is_closed());
    /// assert!(tx.send("never received").is_err());
    ///
    /// match rx.try_recv() {
    ///     Err(TryRecvError::Closed) => {}
    ///     _ => unreachable!(),
    /// }
    /// # }
    /// ```
    ///
    /// Receive a value sent **before** calling `close`
    ///
    /// ```
    /// # fn main() {
    /// let (tx, mut rx) = sync_oneshot::channel();
    ///
    /// assert!(tx.send("will receive").is_ok());
    ///
    /// rx.close();
    ///
    /// let msg = rx.try_recv().unwrap();
    /// assert_eq!(msg, "will receive");
    /// # }
    /// ```
    pub fn close(&mut self) {
        if let Some(inner) = self.inner.as_ref() {
            let _ = inner.set_close();
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        // if inner is some, Receiver::recv is not called before drop.
        // Drop value or change state
        if let Some(inner) = self.inner.take() {
            let prev_state = inner.set_close();
            if prev_state.is_complete() {
                unsafe {
                    inner.consume_value();
                }
            }
        }
    }
}

/*
 *
 * ===== impl Inner =====
 *
 */
impl<T> Inner<T> {
    #[inline]
    fn set_complete(&self) -> State {
        let mut state = self.state.load(Ordering::Relaxed);
        loop {
            if State(state).is_closed() {
                break;
            }

            match self.state.compare_exchange_weak(
                state,
                state | VALUE_SENT,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => state = actual,
            }
        }
        State(state)
    }

    #[inline]
    fn set_close(&self) -> State {
        State(self.state.fetch_or(CLOSED, Ordering::AcqRel))
    }

    #[inline]
    unsafe fn notify(&self) {
        unsafe {
            self.notify.notify();
        }
    }

    #[inline]
    unsafe fn consume_value(&self) -> Option<T> {
        unsafe { self.value.take() }
    }
}

impl<T: fmt::Debug> fmt::Debug for Inner<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Inner")
            .field("state", &State(self.state.load(Ordering::Relaxed)))
            .finish()
    }
}

struct State(usize);

const WAITING: usize = 0b0001;
const VALUE_SENT: usize = 0b0010;
const CLOSED: usize = 0b0100;

/*
 *
 * ===== impl State =====
 *
 */
impl State {
    #[inline]
    fn is_closed(&self) -> bool {
        self.0 & CLOSED == CLOSED
    }

    #[inline]
    fn is_waiting(&self) -> bool {
        self.0 & WAITING == WAITING
    }

    #[inline]
    fn is_complete(&self) -> bool {
        self.0 & VALUE_SENT == VALUE_SENT
    }
}

impl fmt::Debug for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("State")
            .field("is_complete", &self.is_complete())
            .field("is_closed", &self.is_closed())
            .field("is_waiting", &self.is_waiting())
            .finish()
    }
}

#[cfg(not(loom))]
#[inline]
fn park_for(dur: Duration) {
    std::thread::park_timeout(dur);
}

// dummy function for passing compile on loom environment
#[cfg(loom)]
#[inline]
fn park_for(_dur: Duration) {
    unreachable!("timed park is not supported under loom");
}
