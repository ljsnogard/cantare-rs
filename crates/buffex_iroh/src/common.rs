//! 共享的辅助。

/// 把用户给的容量规整到 `circular_buff` 的最小容量（`MIN_CAPACITY = 2`）。
pub(super) fn sanitize_capacity(cap: usize) -> usize {
    cap.max(2)
}
