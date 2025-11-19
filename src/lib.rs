//! sqlc.dev gen core library.
//!
//! Provides:
//! - `plugin`: generated proto definitions
//! - `runtime`: helper functions for running sqlc.dev plugins
//! - `schema`: SQL schema parsing and constraint extraction

pub mod plugin;
pub mod runtime;
pub mod schema;

pub mod prelude {
    pub use crate::plugin::{File, GenerateRequest, GenerateResponse};
    pub use prost::Message;
}
