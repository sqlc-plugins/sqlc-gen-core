pub mod plugin;

pub use prost::Message;

pub mod prelude {
    pub use crate::plugin::*;
    pub use crate::Message;
}
