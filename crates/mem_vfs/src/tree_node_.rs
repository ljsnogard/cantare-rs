use core::{
    alloc::Allocator,
    borrow::Borrow,
    fmt,
    iter::IntoIterator,
    marker::PhantomData,
    ops::Deref,
    ptr,
};
use alloc::{
    collections::BTreeMap,
    sync::Arc,
};

use abs_vfs::fs_tree::{self, TrNodeId};

pub(crate) type DirNameIdMap<TNodeAlloc, TNameAlloc> = BTreeMap<NameSegm<TNameAlloc>, NodeId, TNodeAlloc>;
pub(crate) type DirIdNameMap<TNodeAlloc, TNameAlloc> = BTreeMap<NodeId, NameSegm<TNameAlloc>, TNodeAlloc>;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct NodeId(u64);

impl fs_tree::TrNodeId for NodeId {
    fn root_id() -> Self {
        NodeId(0u64)
    }
}

impl core::fmt::Display for NodeId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone)]
pub struct NameSegm<A>
where
    A: Allocator + Clone,
{
    octets_: Arc<[u8], A>,
}

impl<A> NameSegm<A>
where
    A: Allocator + Clone,
{
    pub(crate) fn root_name(alloc: A) -> Self {
        let octets = unsafe { Arc::new_zeroed_slice_in(0usize, alloc).assume_init() };
        NameSegm { octets_: octets }
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.octets_.deref()
    }

    pub fn try_as_str(&self) -> Option<&str> {
        str::from_utf8(&self.octets_).ok()
    }
}

impl<A> fmt::Display for NameSegm<A>
where
    A: Allocator + Clone,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Option::Some(utf8_str) = self.try_as_str() {
            write!(f, "{}", utf8_str)
        } else {
            write!(f, "[")?;
            for u8 in self.as_bytes() {
                write!(f, "x{:x}", u8)?
            }
            write!(f,"]")
        }
    }
}

impl<A> PartialEq for NameSegm<A>
where
    A: Allocator + Clone,
{
    fn eq(&self, other: &Self) -> bool {
        ptr::eq(self.octets_.deref(), other.octets_.deref())
    }
}

impl<A> Eq for NameSegm<A>
where
    A: Allocator + Clone,
{ }

impl<A> PartialOrd for NameSegm<A>
where
    A: Allocator + Clone,
{
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
        self.octets_.partial_cmp(&other.octets_)
    }
}

impl<A> Ord for NameSegm<A>
where
    A: Allocator + Clone,
{
    fn cmp(&self, other: &Self) -> core::cmp::Ordering {
        self.octets_.cmp(&other.octets_)
    }
}

impl<A> fs_tree::TrFileNameRef<'_> for NameSegm<A>
where
    A: Allocator + Clone,
{
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        NameSegm::as_bytes(self)
    }

    #[inline]
    fn try_as_str(&self) -> Option<&str> {
        NameSegm::try_as_str(self)
    }
}

impl<'a, A> fs_tree::TrFileNameRef<'a> for &'a NameSegm<A>
where
    A: Allocator + Clone,
{
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        NameSegm::as_bytes(self)
    }

    #[inline]
    fn try_as_str(&self) -> Option<&str> {
        NameSegm::try_as_str(self)
    }
}

/// A reference to a node in the fs tree
pub struct InfoNode<B, TNodeAlloc, TNameAlloc>
where
    B: Borrow<FsTreeNode<TNodeAlloc, TNameAlloc>>,
    TNodeAlloc: Allocator + Clone,
    TNameAlloc: Allocator + Clone,
{
    node_: B,
    _use_: PhantomData<FsTreeNode<TNodeAlloc, TNameAlloc>>,
}

impl<B, TNodeAlloc, TNameAlloc> InfoNode<B, TNodeAlloc, TNameAlloc>
where
    B: Borrow<FsTreeNode<TNodeAlloc, TNameAlloc>>,
    TNodeAlloc: Allocator + Clone,
    TNameAlloc: Allocator + Clone,
{
    pub(crate) const fn new(node: B) -> Self {
        InfoNode {
            node_: node,
            _use_: PhantomData,
        }
    }

    #[inline]
    pub fn node_id(&self) -> NodeId {
        self.node_.borrow().as_ref().node_id()
    }

    #[inline]
    pub fn node_type(&self) -> fs_tree::NodeType {
        self.node_.borrow().node_type()
    }

    #[inline]
    pub fn link_count(&self) -> usize {
        self.node_.borrow().as_ref().link_count()
    }

    #[inline]
    pub fn parent(&self) -> NodeId {
        self.node_.borrow().as_ref().parent()
    }

    #[inline]
    pub fn is_dir(&self) -> bool {
        self.node_.borrow().is_dir()
    }
}

impl<B, TNodeAlloc, TNameAlloc> fs_tree::TrInfoNode for InfoNode<B, TNodeAlloc, TNameAlloc>
where
    B: Borrow<FsTreeNode<TNodeAlloc, TNameAlloc>>,
    TNodeAlloc: Allocator + Clone,
    TNameAlloc: Allocator + Clone,
{
    type NodeId = NodeId;

    #[inline]
    fn node_id(&self) -> Self::NodeId {
        InfoNode::node_id(self)
    }

    #[inline]
    fn node_type(&self) -> fs_tree::NodeType {
        InfoNode::node_type(self)
    }

    #[inline]
    fn parent(&self) -> Self::NodeId {
        InfoNode::parent(self)
    }

    #[inline]
    fn is_dir(&self) -> bool {
        InfoNode::is_dir(self)
    }
}

pub enum FsTreeNode<TNodeAlloc, TNameAlloc>
where
    TNodeAlloc: Allocator + Clone,
    TNameAlloc: Allocator + Clone,
{
    LocalDir(LocalDirNode<TNodeAlloc, TNameAlloc>),
    LocalFile(LocalFileNode<TNameAlloc>),
}

impl<TNodeAlloc, TNameAlloc> FsTreeNode<TNodeAlloc, TNameAlloc>
where
    TNodeAlloc: Allocator + Clone,
    TNameAlloc: Allocator + Clone,
{
    pub(crate) const fn node_type(&self) -> fs_tree::NodeType {
        match self {
            FsTreeNode::LocalDir(_) => fs_tree::NodeType::LocalDir,
            FsTreeNode::LocalFile(_) => fs_tree::NodeType::LocalFile,
        }
    }

    pub fn name(&self) -> &NameSegm<TNameAlloc> {
        self.as_ref().name()
    }

    pub const fn is_dir(&self) -> bool {
        matches!(self, FsTreeNode::LocalDir(_))
    }
}

impl<TNodeAlloc, TNameAlloc> AsRef<NodeBase<TNameAlloc>> for FsTreeNode<TNodeAlloc, TNameAlloc>
where
    TNodeAlloc: Allocator + Clone,
    TNameAlloc: Allocator + Clone,
{
    fn as_ref(&self) -> &NodeBase<TNameAlloc> {
        match self {
            Self::LocalDir(dir) => dir.as_ref(),
            Self::LocalFile(file) => file.as_ref(),
        }
    }
}

impl<TNodeAlloc, TNameAlloc> fs_tree::TrInfoNode for FsTreeNode<TNodeAlloc, TNameAlloc>
where
    TNodeAlloc: Allocator + Clone,
    TNameAlloc: Allocator + Clone,
{
    type NodeId = NodeId;

    #[inline]
    fn node_id(&self) -> NodeId {
        self.as_ref().node_id()
    }

    #[inline]
    fn node_type(&self) -> fs_tree::NodeType {
        FsTreeNode::node_type(self)
    }

    #[inline]
    fn parent(&self) -> NodeId {
        self.as_ref().parent()
    }

    #[inline]
    fn is_dir(&self) -> bool {
        FsTreeNode::is_dir(self)
    }
}

impl<TNodeAlloc, TNameAlloc> fs_tree::TrHardLinkTarget for FsTreeNode<TNodeAlloc, TNameAlloc>
where
    TNodeAlloc: Allocator + Clone,
    TNameAlloc: Allocator + Clone,
{
    #[inline]
    fn hard_link_count(&self) -> usize {
        self.as_ref().link_count()
    }
}

pub(crate) struct NodeBase<TNameAlloc>
where
    TNameAlloc: Allocator + Clone,
{
    node_id_: NodeId,
    parent_node_id_: NodeId,
    link_count_: usize,
    name_: NameSegm<TNameAlloc>,
}

impl<TNameAlloc> NodeBase<TNameAlloc>
where
    TNameAlloc: Allocator + Clone,
{
    pub(crate) const fn new(
        id: NodeId,
        parent: NodeId,
        name: NameSegm<TNameAlloc>,
    ) -> Self {
        NodeBase {
            node_id_: id,
            parent_node_id_: parent,
            link_count_: 0usize,
            name_: name,
        }
    }

    #[inline]
    pub const fn node_id(&self) -> NodeId {
        self.node_id_
    }

    #[inline]
    pub const fn link_count(&self) -> usize {
        self.link_count_
    }

    #[inline]
    pub const fn parent(&self) -> NodeId {
        self.parent_node_id_
    }

    #[inline]
    pub const fn name(&self) -> &NameSegm<TNameAlloc> {
        &self.name_
    }
}

pub struct LocalFileNode<TNameAlloc>
where
    TNameAlloc: Allocator + Clone,
{
    base_: NodeBase<TNameAlloc>,
}

impl<TNameAlloc> AsRef<NodeBase<TNameAlloc>> for LocalFileNode<TNameAlloc>
where
    TNameAlloc: Allocator + Clone,
{
    fn as_ref(&self) -> &NodeBase<TNameAlloc> {
        &self.base_
    }
}

pub struct LocalDirNode<TNodeAlloc, TNameAlloc>
where
    TNodeAlloc: Allocator + Clone,
    TNameAlloc: Allocator + Clone,
{
    base_: NodeBase<TNameAlloc>,
    name_id_: DirNameIdMap<TNodeAlloc, TNameAlloc>,
    id_name_: DirIdNameMap<TNodeAlloc, TNameAlloc>,
}

impl<TNodeAlloc, TNameAlloc> LocalDirNode<TNodeAlloc, TNameAlloc>
where
    TNodeAlloc: Allocator + Clone,
    TNameAlloc: Allocator + Clone,
{
    pub fn new(
        id: NodeId,
        parent: NodeId,
        name: NameSegm<TNameAlloc>,
        node_alloc: TNodeAlloc,
    ) -> Self {
        LocalDirNode {
            base_: NodeBase::new(id, parent, name),
            name_id_: DirNameIdMap::new_in(node_alloc.clone()),
            id_name_: DirIdNameMap::new_in(node_alloc),
        }
    }

    pub fn root(
        node_alloc: TNodeAlloc,
        name_alloc: TNameAlloc,
    ) -> Self {
        let root_id = NodeId::root_id();
        LocalDirNode::new(root_id, root_id, NameSegm::root_name(name_alloc), node_alloc)
    }

    #[inline]
    pub const fn node_id(&self) -> NodeId {
        self.base_.node_id()
    }

    #[inline]
    pub const fn node_type(&self) -> fs_tree::NodeType {
        fs_tree::NodeType::LocalDir
    }

    #[inline]
    pub const fn link_count(&self) -> usize {
        self.base_.link_count()
    }

    #[inline]
    pub const fn parent(&self) -> NodeId {
        self.base_.parent()
    }

    pub fn iter_dentry(&self) -> impl IntoIterator<Item = Dentry<'_, TNameAlloc>> {
        self.name_id_
            .iter()
            .map(Dentry::new_with_name_id)
    }

    pub fn get_dentry_by_id<'a>(
        &'a self,
        id: &NodeId,
    ) -> Option<Dentry<'a, TNameAlloc>> {
        let name = self.id_name_.get(id)?;
        let id = self.name_id_.get(name)?;
        Option::Some(Dentry::new_with_id_name((id, name)))
    }

    pub fn get_dentry_by_name<'a>(
        &'a self,
        name: &NameSegm<TNameAlloc>,
    ) -> Option<Dentry<'a, TNameAlloc>> {
        let Option::Some(id) = self.name_id_.get(name) else {
            return Option::None
        };
        if let Option::Some(name) = self.id_name_.get(id) {
            Option::Some(Dentry::new_with_id_name((id, name)))
        } else {
            unreachable!("Broken entry encountered, missing ID key({id}) for name({name})", )
        }
    }

    pub fn remove_child_by_id(&mut self, id: &NodeId) -> bool {
        if let Option::Some(name) = self.id_name_.remove(id) {
            self.name_id_.remove(&name).is_some()
        } else {
            false
        }
    }

    #[inline]
    pub fn insert_child<'a, 'lt_id, 'lt_name>(
        &'a mut self,
        id: &'lt_id NodeId,
        name: &'lt_name NameSegm<TNameAlloc>,
    ) -> Option<Result<&'lt_id NodeId, &'lt_name NameSegm<TNameAlloc>>> {
        let r1 = self.name_id_.insert(name.clone(), id.clone());
        if r1.is_some(){
            let r2 = self.id_name_.insert(id.clone(), name.clone());
            if r2.is_some() {
                Option::None
            } else {
                self.name_id_.remove(name);
                Option::Some(Result::Ok(id))
            }
        } else {
            Option::Some(Result::Err(name))
        }
    }
}

impl<TNodeAlloc, TNameAlloc> AsRef<NodeBase<TNameAlloc>> for LocalDirNode<TNodeAlloc, TNameAlloc>
where
    TNodeAlloc: Allocator + Clone,
    TNameAlloc: Allocator + Clone,
{
    #[inline]
    fn as_ref(&self) -> &NodeBase<TNameAlloc> {
        &self.base_
    }
}

pub struct Dentry<'a, A>
where
    A: Allocator + Clone,
{
    pub name: &'a NameSegm<A>,
    pub id: &'a NodeId,
}

impl<'a, A> Dentry<'a, A>
where
    A: Allocator + Clone,
{
    pub const fn new_with_id_name(t: (&'a NodeId, &'a NameSegm<A>)) -> Self {
        Dentry {
            name: t.1,
            id: t.0,
        }
    }

    pub const fn new_with_name_id(t: (&'a NameSegm<A>, &'a NodeId)) -> Self {
        Dentry {
            name: t.0,
            id: t.1,
        }
    }
}
