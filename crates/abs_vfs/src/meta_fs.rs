use core::error::Error;

use abs_sync::may_cancel::TrMayCancel;

#[derive(Clone, Copy)]
pub enum NodeType {
    Device,

    /// A directory managed by the VFS.
    LocalDir,

    /// A file managed by the VFS.
    LocalFile,

    /// A directory pointed to a storage not managed by the VFS.
    Mount,
}

pub trait TrNodeId
where
    Self: Clone + Eq + Ord,
{
    fn root_id() -> Self;
}

pub trait TrFileNameRef<'a> {
    fn as_bytes(&self) -> &[u8];

    fn try_as_str(&self) -> Option<&str>;
}

/// Dir Entry
pub trait TrDentry {
    type NodeId: TrNodeId;
    type Name<'f>: TrFileNameRef<'f> where Self: 'f;

    fn node_id(&self) -> Self::NodeId;

    fn name(&self) -> Self::Name<'_>;
}

pub trait TrInfoNode {
    type Name<'f>: TrFileNameRef<'f> where Self: 'f;
    type NodeId: TrNodeId;

    fn node_id(&self) -> Self::NodeId;

    fn node_type(&self) -> NodeType;

    fn link_count(&self) -> usize;

    fn name(&self) -> Self::Name<'_>;

    fn parent(&self) -> Self::NodeId;
}

pub trait TrFileSystem {
    type Name<'f>: TrFileNameRef<'f> where Self: 'f;
    type Node: TrInfoNode;
    type NodeId: TrNodeId;
    type Err: Error;

    fn get_inode_async<'a>(
        &'a mut self,
        id: &'a Self::NodeId,
    ) -> impl TrMayCancel<'a, MayCancelOutput = Result<Self::Node, Self::Err>>;

    fn del_inode_async<'a>(
        &'a mut self,
        id: &'a Self::NodeId,
    ) -> impl TrMayCancel<'a, MayCancelOutput = Result<usize, Self::Err>>;

    fn add_local_dir_async<'a>(
        &'a mut self,
        parent: &'a Self::NodeId,
        name: &'a Self::Name<'_>,
    ) -> impl TrMayCancel<'a, MayCancelOutput = Result<Self::Node, Self::Err>>;

    fn add_local_file_async<'a>(
        &'a mut self,
        parent: &'a Self::NodeId,
        name: &'a Self::Name<'_>,
    ) -> impl TrMayCancel<'a, MayCancelOutput = Result<Self::Node, Self::Err>>;
}

impl<'a> TrFileNameRef<'a> for &'a str {
    fn as_bytes(&self) -> &[u8] {
        str::as_bytes(self)
    }

    fn try_as_str(&self) -> Option<&str> {
        Option::Some(self)
    }
}

#[cfg(any(test, feature = "std"))]
impl TrFileNameRef<'_> for std::string::String {
    fn as_bytes(&self) -> &[u8] {
        std::string::String::as_bytes(self)
    }

    fn try_as_str(&self) -> Option<&str> {
        Option::Some(self.as_str())
    }
}
