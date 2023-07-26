pub(crate) mod packer;
pub(crate) mod tree;

use std::ops::Add;

use derive_more::Constructor;
use enum_map::{Enum, EnumMap};
use serde::{Deserialize, Serialize};

use crate::id::Id;

/// All [`BlobType`]s which are supported by the repository
pub const ALL_BLOB_TYPES: [BlobType; 2] = [BlobType::Tree, BlobType::Data];

#[derive(
    Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Enum,
)]
/// The type a `blob` or a `packfile` can have
pub enum BlobType {
    #[serde(rename = "tree")]
    /// This is a tree blob
    Tree,
    #[serde(rename = "data")]
    /// This is a data blob
    Data,
}

impl BlobType {
    #[must_use]
    pub(crate) const fn is_cacheable(self) -> bool {
        match self {
            Self::Tree => true,
            Self::Data => false,
        }
    }
}

pub type BlobTypeMap<T> = EnumMap<BlobType, T>;

/// Initialize is a new trait to define the method init() for a [`BlobTypeMap`]
pub trait Initialize<T: Default + Sized> {
    /// initialize a [`BlobTypeMap`] by processing a given function for each [`BlobType`]
    fn init<F: FnMut(BlobType) -> T>(init: F) -> BlobTypeMap<T>;
}

impl<T: Default> Initialize<T> for BlobTypeMap<T> {
    fn init<F: FnMut(BlobType) -> T>(mut init: F) -> Self {
        let mut btm = Self::default();
        for i in 0..BlobType::LENGTH {
            let bt = BlobType::from_usize(i);
            btm[bt] = init(bt);
        }
        btm
    }
}

/// Sum is a new trait to define the method sum() for a `BlobTypeMap`
pub trait Sum<T> {
    fn sum(&self) -> T;
}

impl<T: Default + Copy + Add<Output = T>> Sum<T> for BlobTypeMap<T> {
    fn sum(&self) -> T {
        self.values().fold(T::default(), |acc, x| acc + *x)
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Constructor)]
pub(crate) struct Blob {
    tpe: BlobType,
    id: Id,
}
