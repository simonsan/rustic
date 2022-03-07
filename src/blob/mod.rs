mod packer;
mod tree;
pub use crate::backend::node::*;
pub use packer::*;
pub use tree::*;

use binrw::BinWrite;
use derive_more::Constructor;
use serde::{Deserialize, Serialize};

use crate::id::Id;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, BinWrite)]
pub enum BlobType {
    #[serde(rename = "data")]
    #[bw(magic(0u8))]
    Data,
    #[serde(rename = "tree")]
    #[bw(magic(1u8))]
    Tree,
}

#[derive(Debug, PartialEq, Clone, Constructor)]
pub struct Blob {
    tpe: BlobType,
    id: Id,
}
