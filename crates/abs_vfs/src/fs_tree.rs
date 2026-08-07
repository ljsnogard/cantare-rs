//! This crate defines the tree structure of a virtual file system (VFS), and provides
//! common abstraction by defining traits and basic data like `NodeType`, `FileName`
//! and traits like `TrNodeId`, `TrFileNameRef<'a>`, and available operations over a
//! VFS tree.

use core::{error::Error, iter::IntoIterator};

use abs_cancel::TrMayCancel;

#[derive(Clone, Copy)]
pub enum NodeType {
    Device,

    /// A directory managed by the VFS.
    LocalDir,

    /// A file managed by the VFS.
    LocalFile,

    /// A directory pointed to a storage not managed by the VFS.
    MountPoint,
}

pub type FileName<'a, P> = <P as TrFilePathRef>::Segm<'a>;

pub trait TrNodeId
where
    Self: Clone + Eq + Ord + Sized,
{
    fn root_id() -> Self;
}

/// Represents just file name, which is without the path part.
pub trait TrFileNameRef<'a>
where
    Self: Clone + Eq + Sized,
{
    fn as_bytes(&self) -> &[u8];

    fn try_as_str(&self) -> Option<&str>;
}

/// Represents a sequence of names making a file path, without the path separator.
pub trait TrFilePathRef {
    type Segm<'f>: TrFileNameRef<'f> where Self: 'f;

    fn segments(&self) -> impl IntoIterator<Item = Self::Segm<'_>>;
}

/// inode
pub trait TrInfoNode {
    type NodeId: TrNodeId;

    fn node_id(&self) -> Self::NodeId;

    fn node_type(&self) -> NodeType;

    /// 父节点 ID，根节点的父节点是自身
    fn parent(&self) -> Self::NodeId;

    /// 判断是否目录，可以是挂载目录也可以是本地目录
    fn is_dir(&self) -> bool;
}

pub trait TrHardLinkTarget
where
    Self: TrInfoNode,
{
    /// 检索硬连接数
    fn hard_link_count(&self) -> usize;
}

pub trait TrAsyncRetrieveINode {
    type Name<'f>: TrFileNameRef<'f> where Self: 'f;
    type Node: TrInfoNode;
    type NodeId: TrNodeId;
    type Path: TrFilePathRef;
    type Err: Error;

    fn get_inode_async<'a>(
        &'a self,
        id: &'a Self::NodeId,
    ) -> impl TrMayCancel<'a, MayCancelOutput = Result<Self::Node, Self::Err>>;

    fn find_inode_async<'a>(
        &'a self,
        root_id: &'a Self::NodeId,
        path: &'a Self::Path,
    ) -> impl TrMayCancel<'a, MayCancelOutput = Result<Self::Node, Self::Err>>;

    fn get_name_async<'a>(
        &'a self,
        id: &'a Self::NodeId,
    ) -> impl TrMayCancel<'a, MayCancelOutput = Result<Self::Name<'a>, Self::Err>>;

    fn get_path_async<'a>(
        &'a self,
        id: &'a Self::NodeId,
    ) -> impl TrMayCancel<'a, MayCancelOutput = Result<Self::Path, Self::Err>>;
}

pub trait TrAsyncDeleteINode {
    type Name<'f>: TrFileNameRef<'f> where Self: 'f;
    type Node: TrInfoNode;
    type NodeId: TrNodeId;
    type Err: Error;

    fn del_inode_async<'a>(
        &'a mut self,
        id: &'a Self::NodeId,
    ) -> impl TrMayCancel<'a, MayCancelOutput = Result<usize, Self::Err>>;
}

pub trait TrAsyncCreateINode {
    type Name<'f>: TrFileNameRef<'f> where Self: 'f;
    type Node: TrInfoNode;
    type NodeId: TrNodeId;
    type Err: Error;

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
