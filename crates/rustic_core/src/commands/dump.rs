use std::io::Write;

use crate::{
    backend::node::{Node, NodeType},
    blob::BlobType,
    error::{CommandErrorKind, RusticResult},
    index::IndexedBackend,
    repository::{IndexedFull, IndexedTree, Repository},
};

/// Dumps the contents of a file.
///
/// # Arguments
///
/// * `repo` - The repository to read from.
/// * `node` - The node to dump.
/// * `w` - The writer to write to.
///
/// # Errors
///
/// * [`CommandErrorKind::DumpNotSupported`] if the node is not a file.
pub(crate) fn dump<P, S: IndexedFull>(
    repo: &Repository<P, S>,
    node: &Node,
    w: &mut impl Write,
) -> RusticResult<()> {
    if node.node_type != NodeType::File {
        return Err(CommandErrorKind::DumpNotSupported(node.node_type.clone()).into());
    }

    for id in node.content.as_ref().unwrap() {
        // TODO: cache blobs which are needed later
        let data = repo.index().blob_from_backend(BlobType::Data, id)?;
        w.write_all(&data)?;
    }
    Ok(())
}
