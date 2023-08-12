use std::{fmt, io::Read, ops::Deref, path::Path};

use binrw::{BinRead, BinWrite};
use derive_more::{Constructor, Display};
use rand::{thread_rng, RngCore};
use serde::{Deserialize, Serialize};

use crate::{crypto::hasher::hash, error::IdErrorKind, RusticResult};

pub(super) mod constants {
    pub(super) const LEN: usize = 32;
    pub(super) const HEX_LEN: usize = LEN * 2;
}

#[derive(
    Serialize,
    Deserialize,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Constructor,
    BinWrite,
    BinRead,
    Display,
)]
#[display(fmt = "{}", "&self.to_hex()[0..8]")]
/// `Id` is the hash id of an object. It is used to identify blobs or files saved in the repository
pub struct Id(
    /// The actual hash
    #[serde(serialize_with = "hex::serde::serialize")]
    #[serde(deserialize_with = "hex::serde::deserialize")]
    [u8; constants::LEN],
);

impl Id {
    /// Parse an `Id` from an hexadecimal string
    ///
    /// # Arguments
    ///
    /// * `s` - The hexadecimal string to parse
    ///
    /// # Errors
    ///
    /// If the string is not a valid hexadecimal string
    ///
    /// # Examples
    ///
    /// ```
    /// use rustic_core::id::Id;
    ///
    /// let id = Id::from_hex("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef").unwrap();
    ///
    /// assert_eq!(id.to_hex().as_str(), "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef");
    /// ```
    pub fn from_hex(s: &str) -> RusticResult<Self> {
        let mut id = Self::default();

        hex::decode_to_slice(s, &mut id.0).map_err(IdErrorKind::HexError)?;

        Ok(id)
    }

    #[must_use]
    /// Generate a random `Id`.
    pub fn random() -> Self {
        let mut id = Self::default();
        thread_rng().fill_bytes(&mut id.0);
        id
    }

    #[must_use]
    /// Convert to [`HexId`].
    ///
    /// # Examples
    ///
    /// ```
    /// use rustic_core::id::Id;
    ///
    /// let id = Id::from_hex("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef").unwrap();
    ///
    /// assert_eq!(id.to_hex().as_str(), "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef");
    /// ```
    pub fn to_hex(self) -> HexId {
        let mut hex_id = HexId::EMPTY;
        // HexId's len is LEN * 2
        hex::encode_to_slice(self.0, &mut hex_id.0).unwrap();
        hex_id
    }

    #[must_use]
    /// Checks if the Id is zero
    ///
    /// # Examples
    ///
    /// ```
    /// use rustic_core::id::Id;
    ///
    /// let id = Id::from_hex("0").unwrap();
    ///
    /// assert!(id.is_null());
    /// ```
    pub fn is_null(&self) -> bool {
        self == &Self::default()
    }

    /// Checks if this Id matches the content of a Reader
    ///
    /// # Arguments
    ///
    /// * `length` - The length of the blob
    /// * `r` - The reader to check
    ///
    /// # Returns
    ///
    /// `true` if the SHA256 matches, `false` otherwise
    pub fn blob_matches_reader(&self, length: usize, r: &mut impl Read) -> bool {
        // check if SHA256 matches
        let mut vec = vec![0; length];
        r.read_exact(&mut vec).is_ok() && self == &hash(&vec)
    }
}

impl fmt::Debug for Id {
    /// Format the `Id` as a hexadecimal string
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &*self.to_hex())
    }
}

#[derive(Copy, Clone, Debug)]
/// An [`Id`] in hexadecimal format
pub struct HexId([u8; constants::HEX_LEN]);

impl HexId {
    const EMPTY: Self = Self([b'0'; constants::HEX_LEN]);

    /// Get the string representation of a `HexId`
    pub fn as_str(&self) -> &str {
        // This is only ever filled with hex chars, which are ascii
        std::str::from_utf8(&self.0).unwrap()
    }
}

impl Deref for HexId {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl AsRef<Path> for HexId {
    fn as_ref(&self) -> &Path {
        self.as_str().as_ref()
    }
}
