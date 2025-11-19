//! sqlc.dev gen core library.
//!
//! Provides:
//! - `plugin`: generated proto definitions
//! - `runtime`: helper functions for running sqlc.dev plugins

pub mod plugin;
pub mod runtime;

pub mod prelude {
    pub use crate::plugin::*;
    pub use crate::runtime::*;
    pub use prost::Message;
}
