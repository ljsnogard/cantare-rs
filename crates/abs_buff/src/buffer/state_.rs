/// Ob
pub trait TrBufferState {
    fn capacity(&self) -> usize;

    fn data_size(&self) -> usize;

    fn free_size(&self) -> usize;

    fn is_producer_closed(&self) -> bool;

    fn is_consumer_closed(&self) -> bool;
}
