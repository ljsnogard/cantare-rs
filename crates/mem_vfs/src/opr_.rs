use core::{
    alloc::Allocator,
    pin::Pin,
    ops::Deref,
};
use alloc::{
    collections::BTreeMap,
    sync::Arc,
    vec::Vec,
};

use thiserror::Error;

use abs_sync::{
    may_cancel::TrMayCancel,
    cancellation::TrCancellationToken,
};
use abs_vfs::{fs_tree, x_deps::abs_sync};

use crate::tree_node_::{FsTreeNode, InfoNode, LocalDirNode, NameSegm, NodeId};

type NodeArc<TNodeAlloc, TNameAlloc> = Arc<FsTreeNode<TNodeAlloc, TNameAlloc>, TNodeAlloc>;

type NodeMap<TNodeAlloc, TNameAlloc> = BTreeMap<NodeId, NodeArc<TNodeAlloc, TNameAlloc>, TNodeAlloc>;
type INode<TNodeAlloc, TNameAlloc> = InfoNode<NodeArc<TNodeAlloc, TNameAlloc>, TNodeAlloc, TNameAlloc>;

#[derive(Error, Debug)]
pub enum FsOprError {
    #[error("Operation cannot applied to a directory (id: {0})")]
    NodeIsDir(NodeId),

    #[error("Operation cannot applied to a file (id: {0})")]
    NodeIsFile(NodeId),

    #[error("Invalid node id({0})")]
    InvalidNodeId(NodeId),

    #[error("Invalid path")]
    InvalidPath(usize),
}

pub struct Path<TVecAlloc, TStrAlloc>
where
    TVecAlloc: Allocator,
    TStrAlloc: Allocator + Clone,
{
    segm_: Vec<NameSegm<TStrAlloc>, TVecAlloc>,
}

impl<TVecAlloc, TStrAlloc> Path<TVecAlloc, TStrAlloc>
where
    TVecAlloc: Allocator,
    TStrAlloc: Allocator + Clone,
{
    pub const fn new(segms: Vec<NameSegm<TStrAlloc>, TVecAlloc>) -> Self {
        Path { segm_: segms }
    }

    pub fn segments_vec(&self) -> &Vec<NameSegm<TStrAlloc>, TVecAlloc> {
        &self.segm_
    }
}

impl<TVecAlloc, TStrAlloc> fs_tree::TrFilePathRef for Path<TVecAlloc, TStrAlloc>
where
    TVecAlloc: Allocator,
    TStrAlloc: Allocator + Clone,
{
    type Segm<'f> = NameSegm<TStrAlloc> where Self: 'f;

    fn segments(&self) -> impl IntoIterator<Item = Self::Segm<'_>> {
        self.segm_.iter().cloned()
    }
}

/// File System Tree
pub struct FsTree<TNodeAlloc, TNameAlloc>
where
    TNodeAlloc: Allocator + Clone,
    TNameAlloc: Allocator + Clone,
{
    id_node_map_: NodeMap<TNodeAlloc, TNameAlloc>,
}

impl<TNodeAlloc, TNameAlloc> FsTree<TNodeAlloc, TNameAlloc>
where
    TNodeAlloc: Allocator + Clone,
    TNameAlloc: Allocator + Clone,
{
    pub const fn new_unchecked(id_node_map: NodeMap<TNodeAlloc, TNameAlloc>) -> Self {
        FsTree { id_node_map_: id_node_map }
    }

    pub fn new_in(
        node_alloc: TNodeAlloc,
        name_alloc: TNameAlloc,
    ) -> Self {
        let mut id_node_map = NodeMap::new_in(node_alloc.clone());
        let fs_root = LocalDirNode::<TNodeAlloc, TNameAlloc>::root(node_alloc.clone(), name_alloc);
        let root_id = fs_root.node_id();
        let root_node = Arc::new_in(FsTreeNode::LocalDir(fs_root), node_alloc);
        id_node_map.insert(root_id, root_node);
        Self::new_unchecked(id_node_map)
    }

    pub fn get_inode_by_id<'a>(
        &'a self,
        id: &'a NodeId,
    ) -> Result<INode<TNodeAlloc, TNameAlloc>, FsOprError> {
        let find_result = self.id_node_map_.get(id);
        if let Option::Some(node) = find_result {
            Result::Ok(INode::new(node.clone()))
        } else {
            Result::Err(FsOprError::InvalidNodeId(id.clone()))
        }
    }

    pub fn find_inode_with_path_async<'a>(
        &'a self,
        root_id: &'a NodeId,
        path: &'a Path<TNodeAlloc, TNameAlloc>,
    ) -> FindINodeWithPathAsync<'a, TNodeAlloc, TNameAlloc> {
        FindINodeWithPathAsync(self, root_id, path)
    }

    pub fn get_inode_name<'a>(
        &'a self,
        id: &'a NodeId,
    ) -> Result<&'a NameSegm<TNameAlloc>, FsOprError> {
        let Option::Some(node) = self.id_node_map_.get(id) else {
            return Result::Err(FsOprError::InvalidNodeId(id.clone()))
        };
        Result::Ok(node.as_ref().name())
    }

    pub fn get_path<'a>(
        &'a self,
        id: &'a NodeId,
    ) -> Result<Path<TNodeAlloc, TNameAlloc>, FsOprError> {
        Result::Err(FsOprError::InvalidNodeId(id.clone()))
    }
}

#[gen_mcf_macro::gen_may_cancel_future(GetINodeById)]
async fn get_inode_by_id_async_<'a, TNodeAlloc, TNameAlloc, C>(
    fs: &'a FsTree<TNodeAlloc, TNameAlloc>,
    id: &'a NodeId,
    _c: Pin<&'a mut C>,
) -> Result<INode<TNodeAlloc, TNameAlloc>, FsOprError>
where
    TNodeAlloc: Allocator + Clone,
    TNameAlloc: Allocator + Clone,
    C: TrCancellationToken,
{
    FsTree::get_inode_by_id(fs, id)
}

#[gen_mcf_macro::gen_may_cancel_future(FindINodeWithPath)]
async fn find_inode_with_path_async_<'a, TNodeAlloc, TNameAlloc, C>(
    tree: &'a FsTree<TNodeAlloc, TNameAlloc>,
    root_id: &'a NodeId,
    path: &'a Path<TNodeAlloc, TNameAlloc>,
    _: Pin<&'a mut C>,
) -> Result<INode<TNodeAlloc, TNameAlloc>, FsOprError>
where
    TNodeAlloc: Allocator + Clone,
    TNameAlloc: Allocator + Clone,
    C: TrCancellationToken,
{
    let mut curr_id = root_id;
    let Option::Some(mut curr_node) = tree.id_node_map_.get(&curr_id) else {
        return Result::Err(FsOprError::InvalidNodeId(curr_id.clone()));
    };
    let mut segm_index = 0usize;
    let segm_vec = path.segments_vec();
    let segm_vec_size = segm_vec.len();

    // empty path is always ok
    if segm_index == segm_vec_size {
        return Result::Ok(InfoNode::new(curr_node.clone()));
    }
    while segm_index < segm_vec_size {
        // the root node is not dir, no more search is available.
        if !curr_node.is_dir() {
            return Result::Err(FsOprError::NodeIsFile(curr_id.clone()));
        }
        let segm_name = &segm_vec[segm_index];
        if let FsTreeNode::LocalDir(dir) = curr_node.deref() {
            let Option::Some(dentry) = dir.get_dentry_by_name(segm_name) else {
                return Result::Err(FsOprError::InvalidPath(segm_index));
            };
            let Option::Some(child_node) = tree.id_node_map_.get(dentry.id) else {
                return Result::Err(FsOprError::InvalidNodeId(dentry.id.clone()));
            };
            curr_id = dentry.id;
            curr_node = child_node;
            segm_index += 1usize;
            continue;
        }
        // TODO: check mount point
        unreachable!("[opr_::find_inode_with_path_async_] FsTreeNode exaust guaranteed.")
    }
    Result::Err(FsOprError::InvalidPath(segm_index))
}

#[gen_mcf_macro::gen_may_cancel_future(GetINodeNameById)]
async fn get_inode_name_async_<'a, TNodeAlloc, TNameAlloc, C>(
    fs: &'a FsTree<TNodeAlloc, TNameAlloc>,
    id: &'a NodeId,
    _: Pin<&'a mut C>,
) -> Result<NameSegm<TNameAlloc>, FsOprError>
where
    TNodeAlloc: Allocator + Clone,
    TNameAlloc: Allocator + Clone,
    C: TrCancellationToken,
{
    FsTree::get_inode_name(fs, id).cloned()
}

#[gen_mcf_macro::gen_may_cancel_future(GetINodePathById)]
async fn get_inode_path_async_<'a, TNodeAlloc, TNameAlloc, C>(
    fs: &'a FsTree<TNodeAlloc, TNameAlloc>,
    id: &'a NodeId,
    _: Pin<&'a mut C>,
) -> Result<Path<TNodeAlloc, TNameAlloc>, FsOprError>
where
    TNodeAlloc: Allocator + Clone,
    TNameAlloc: Allocator + Clone,
    C: TrCancellationToken,
{
    FsTree::get_path(fs, id)
}

impl<TNodeAlloc, TNameAlloc> fs_tree::TrAsyncRetrieveINode for FsTree<TNodeAlloc, TNameAlloc>
where
    TNodeAlloc: Allocator + Clone,
    TNameAlloc: Allocator + Clone,
{
    type Name<'f> = NameSegm<TNameAlloc> where Self: 'f;
    type Node = INode<TNodeAlloc, TNameAlloc>;
    type NodeId = NodeId;
    type Path = Path<TNodeAlloc, TNameAlloc>;
    type Err = FsOprError;

    fn get_inode_async<'a>(
        &'a self,
        id: &'a Self::NodeId,
    ) -> impl TrMayCancel<'a, MayCancelOutput = Result<Self::Node, Self::Err>> {
        GetINodeByIdAsync(self, id)
    }

    fn find_inode_async<'a>(
        &'a self,
        root_id: &'a Self::NodeId,
        path: &'a Self::Path,
    ) -> impl TrMayCancel<'a, MayCancelOutput = Result<Self::Node, Self::Err>> {
        FindINodeWithPathAsync(self, root_id, path)
    }

    fn get_name_async<'a>(
        &'a self,
        id: &'a Self::NodeId,
    ) -> impl TrMayCancel<'a, MayCancelOutput = Result<Self::Name<'a>, Self::Err>> {
        GetINodeNameByIdAsync(self, id)
    }

    fn get_path_async<'a>(
        &'a self,
        id: &'a Self::NodeId,
    ) -> impl TrMayCancel<'a, MayCancelOutput = Result<Self::Path, Self::Err>> {
        GetINodePathByIdAsync(self, id)
    }
}
