use core::{
    cmp,
    ops::{Bound, RangeBounds},
};

/// Describes an interval of amount that needed to operate.
#[derive(Clone, Debug)]
pub struct Demand<T>(InclRange<T>)
where
    T: Eq + Ord;

impl Demand<usize> {
    pub fn try_from_usize_range(
        range: &impl RangeBounds<usize>,
    ) -> Result<Self, (Bound<&usize>, Bound<&usize>)> {
        use Bound::*;

        let start_bound = range.start_bound();
        let end_bound = range.end_bound();

        // 将边界转换为 Option<usize>，Excluded 转换为对应的 Included 值
        // 只有手动构造一个 struct 的情况下，才可能为 None
        let start = match start_bound {
            Included(&v) => Option::Some(v),
            Excluded(&v) => v.checked_add(1),
            Unbounded => Option::Some(usize::MIN),
        };
        // 只有当 ..=0usize 的情况下，才为 None
        let end = match end_bound {
            Included(&v) => v.checked_add(1),
            Excluded(&v) => Option::Some(v),
            Unbounded => Option::Some(usize::MAX),
        };

        match (start, end) {
            (Some(l), Some(u)) => {
                if l < u {
                    Ok(Demand::between(l, u))
                } else {
                    Err((start_bound, end_bound))
                }
            },
            _ => Err((start_bound, end_bound)),
        }
    }

    pub fn is_empty(&self) -> bool {
        use InclRange::*;

        match &self.0 {
            Between(l, u) => l == u,
            _ => false,
        }
    }

    pub fn len(&self) -> usize {
        use InclRange::*;

        match &self.0 {
            GtEq(l) => usize::MAX - l,
            Lt(u) => *u,
            Exactly(_) => 1usize,
            Between(l, u) => u - l,
        }
    }
}

impl<T> Demand<T>
where
    T: Eq + Ord,
{
    /// Create zero-sized range with value x.
    ///
    /// ## Example
    /// ```
    /// use abs_buff::Demand;
    ///
    /// let a = Demand::exactly(1);
    /// assert!(matches!(a.min(), Option::Some(1)));
    /// assert!(matches!(a.max(), Option::Some(1)));
    /// ```
    pub const fn exactly(val: T) -> Self {
        Demand(InclRange::Exactly(val))
    }

    /// Create a range between (a, b), and the product could vary according to
    /// the actual value of a, b.
    ///
    /// ## Example
    /// ```
    /// use abs_buff::Demand;
    ///
    /// let a = Demand::between(10, 1);
    /// assert!(matches!(a.min(), Option::Some(1)));
    /// assert!(matches!(a.max(), Option::Some(10)));
    /// let b = Demand::between(1, 10);
    /// assert!(matches!(b.min(), Option::Some(1)));
    /// assert!(matches!(b.max(), Option::Some(10)));
    /// ```
    pub fn between(a: T, b: T) -> Self {
        use InclRange::*;

        Demand(if a < b {
            Between(a, b)
        } else if a == b {
            Exactly(a)
        } else {
            Between(b, a)
        })
    }

    /// Create a range with specified least value
    ///
    /// ## Example
    /// ```
    /// use abs_buff::Demand;
    ///
    /// let a = Demand::no_less_than(2);
    /// assert!(matches!(a.min(), Option::Some(2)));
    /// assert!(a.max().is_none());
    /// ```
    pub const fn no_less_than(val: T) -> Self {
        Demand(InclRange::GtEq(val))
    }

    /// Create a range with specified max value
    ///
    /// ## Example
    /// ```
    /// use abs_buff::Demand;
    ///
    /// let a = Demand::less_than(2);
    /// assert!(matches!(a.max(), Option::Some(2)));
    /// assert!(a.min().is_none());
    /// ```
    pub const fn less_than(val: T) -> Self {
        Demand(InclRange::Lt(val))
    }

    /// Check if a low bound is included in the demand
    pub const fn min(&self) -> Option<&T> {
        use InclRange::*;

        match &self.0 {
            GtEq(l) => Option::Some(l),
            Exactly(x) => Option::Some(x),
            Between(l, _)  => Option::Some(l),
            _ => Option::None,
        }
    }

    pub const fn max(&self) -> Option<&T> {
        use InclRange::*;

        match &self.0 {
            Lt(u) => Option::Some(u),
            Exactly(x) => Option::Some(x),
            Between(_, u) => Option::Some(u),
            _ => Option::None,
        }
    }

    pub const fn as_ref(&self) -> Demand<&T> {
        use InclRange::*;

        match &self.0 {
            GtEq(l) => Demand(GtEq(l)),
            Lt(u) => Demand(Lt(u)),
            Between(l, u) => Demand(Between(l, u)),
            Exactly(v) => Demand(Exactly(v)),
        }
    }

    pub fn compromise(&self, other: &Self) -> Option<Self>
    where
        T: Clone,
    {
        use InclRange::*;

        // 辅助：将 Exactly 统一视为 Between 以便处理
        let left = match &self.0 {
            Exactly(x) => Between(x.clone(), x.clone()),
            other => other.clone(),
        };
        let right = match &other.0 {
            Exactly(x) => Between(x.clone(), x.clone()),
            other => other.clone(),
        };

        match (left, right) {
            // 无上界 + 无上界 → 取较大的下界
            (GtEq(a), GtEq(b)) => Some(Demand::no_less_than(cmp::max(a, b))),

            // 无下界 + 无下界 → 取较小的上界
            (Lt(a), Lt(b)) => Some(Demand::less_than(cmp::min(a, b))),

            // 无下界 + 无上界 → 需满足 a <= b 才构成闭区间
            (GtEq(a), Lt(b)) if a <= b => Some(Demand::between(a, b)),
            (Lt(b), GtEq(a)) if a <= b => Some(Demand::between(a, b)),

            // 无上界 + 闭区间
            (GtEq(a), Between(c, d)) if a <= d => Some(Demand::between(cmp::max(a, c), d)),
            (Between(c, d), GtEq(a)) if a <= d => Some(Demand::between(cmp::max(a, c), d)),

            // 无下界 + 闭区间
            (Lt(b), Between(c, d)) if c <= b => Some(Demand::between(c, cmp::min(b, d))),
            (Between(c, d), Lt(b)) if c <= b => Some(Demand::between(c, cmp::min(b, d))),

            // 闭区间 + 闭区间
            (Between(a, b), Between(c, d)) => {
                let lower = cmp::max(a, c);
                let upper = cmp::min(b, d);
                if lower <= upper {
                    Some(Demand::between(lower, upper))
                } else {
                    None
                }
            }

            // 无下界与无下界、无上界与无上界等已在上面覆盖，其余情况无交集
            _ => None,
        }
    }
}

impl<T> RangeBounds<T> for Demand<T>
where
    T: Eq + Ord,
{
    fn start_bound(&self) -> Bound<&T> {
        match &self.0 {
            InclRange::GtEq(x) => Bound::Included(x),
            InclRange::Lt(_) => Bound::Unbounded,
            InclRange::Exactly(x) => Bound::Included(x),
            InclRange::Between(x, _) => Bound::Included(x),
        }
    }

    fn end_bound(&self) -> Bound<&T> {
        match &self.0 {
            InclRange::GtEq(_) => Bound::Unbounded,
            InclRange::Lt(x) => Bound::Excluded(x),
            InclRange::Exactly(x) => Bound::Included(x),
            InclRange::Between(_, x) => Bound::Excluded(x),
        }
    }
}

/// A left-closed right-closed interval, inclusive range
#[derive(Clone, Debug)]
pub(crate) enum InclRange<T>
where
    T: Eq + Ord,
{
    /// greater or equal
    GtEq(T),

    /// less than
    Lt(T),

    /// equals to
    Exactly(T),

    /// [a, b)
    Between(T, T),
}

#[cfg(test)]
mod try_from_usize_range_tests_ {
    use super::*;
    use core::ops::{Bound, RangeBounds};

    #[test]
    fn demand_factory_test() {
        let demand = Demand::between(1usize, 100usize);
        assert_eq!(demand.len(), 99usize);
        let range = 1usize..100usize;
        let other = Demand::try_from_usize_range(&range).unwrap();
        assert_eq!(other.len(), demand.len());
        let d_min = demand.min().unwrap();
        let o_min = other.min().unwrap();
        assert_eq!(*d_min, *o_min);
        let d_max = demand.max().unwrap();
        let o_max = other.max().unwrap();
        assert_eq!(*d_max, *o_max);
    }

    #[test]
    fn excatly_vs_between() {
        let a = Demand::between(0usize, 1usize);
        let b = Demand::exactly(0usize);
        assert_eq!(a.len(), b.len());
        assert_eq!(a.min().unwrap(), b.min().unwrap());
    }

    // 辅助函数：将 Demand<usize> 转换为 (下界, 上界) 的 Option 对，便于断言
    fn bounds(d: &Demand<usize>) -> (Option<usize>, Option<usize>) {
        (d.min().copied(), d.max().copied())
    }

    #[test]
    fn full_range() {
        let range = ..;
        let demand = Demand::try_from_usize_range(&range).unwrap();
        assert_eq!(bounds(&demand), (Some(usize::MIN), Some(usize::MAX)));
    }

    #[test]
    fn range_from() {
        // 5..   → [5, ∞)
        let range = 5..;
        let demand = Demand::try_from_usize_range(&range).unwrap();
        assert_eq!(bounds(&demand), (Some(5), Some(usize::MAX)));

        // 0..   → [0, ∞)
        let range = 0..;
        let demand = Demand::try_from_usize_range(&range).unwrap();
        assert_eq!(bounds(&demand), (Some(0), Some(usize::MAX)));
    }

    #[test]
    fn range_to() {
        // ..10   → [0, 9]  因为 Excluded(10) → 10-1=9
        let range = ..10;
        let demand = Demand::try_from_usize_range(&range).unwrap();
        assert_eq!(bounds(&demand), (Some(usize::MIN), Some(10)));

        // ..=10  → [0, 10]
        let range = ..=10;
        let demand = Demand::try_from_usize_range(&range).unwrap();
        assert_eq!(bounds(&demand), (Some(usize::MIN), Some(11)));

        // ..0    → (-∞, usize::MAX? 不，Excluded(0) → 0-1 溢出) 应返回 Err
        let range = ..0;
        let result = Demand::try_from_usize_range(&range);
        assert!(result.is_err());
    }

    #[test]
    fn range_inclusive() {
        // 5..=10 → [5, 10]
        let range = 5..=10;
        let demand = Demand::try_from_usize_range(&range).unwrap();
        assert_eq!(bounds(&demand), (Some(5), Some(11)));

        // 5..=5  → Exactly(5)
        let range = 5..=5;
        let demand = Demand::try_from_usize_range(&range).unwrap();
        assert_eq!(bounds(&demand), (Some(5), Some(6)));

        // 10..=5 → start=10, end=5, start > end → Err
        // let range = 10..=5;
        // let result = Demand::try_from_usize_range(&range);
        // assert!(result.is_err());
    }

    #[test]
    fn range_exclusive() {
        let range = 5..10;
        let demand = Demand::try_from_usize_range(&range).unwrap();
        assert_eq!(bounds(&demand), (Some(5), Some(10)));

        // 0..1  → [0, 0]
        let range = 0..1;
        let demand = Demand::try_from_usize_range(&range).unwrap();
        assert_eq!(bounds(&demand), (Some(0), Some(1)));

        // 5..5  → start=5, end Excluded(5) → end=4, start>end → Err
        let range = 5..5;
        let result = Demand::try_from_usize_range(&range);
        assert!(result.is_err());

        // 5..0  → start=5, end Excluded(0) → end=usize::MAX? 不，0-1溢出 → Err
        // let range = 5..0;
        // let result = Demand::try_from_usize_range(&range);
        // assert!(result.is_err());
    }

    #[test]
    fn custom_range_bounds() {
        // 使用 Bound 手动构造一个 (Included(5), Excluded(10)) 的区间
        struct CustomRange;
        impl RangeBounds<usize> for CustomRange {
            fn start_bound(&self) -> Bound<&usize> {
                Bound::Included(&5)
            }
            fn end_bound(&self) -> Bound<&usize> {
                Bound::Excluded(&10)
            }
        }
        let demand = Demand::try_from_usize_range(&CustomRange).unwrap();
        assert_eq!(bounds(&demand), (Some(5), Some(10)));

        // 无效边界：Included(10), Excluded(5) → start=10, end=4 → Err
        struct InvalidRange;
        impl RangeBounds<usize> for InvalidRange {
            fn start_bound(&self) -> Bound<&usize> {
                Bound::Included(&10)
            }
            fn end_bound(&self) -> Bound<&usize> {
                Bound::Excluded(&5)
            }
        }
        let result = Demand::try_from_usize_range(&InvalidRange);
        assert!(result.is_err());
    }

    #[test]
    fn overflow_handling() {
        let range = usize::MAX..;
        let result = Demand::try_from_usize_range(&range);
        assert!(result.is_err());

        let range = ..0usize;
        let result = Demand::try_from_usize_range(&range);
        assert!(result.is_err());
    }

    #[test]
    fn empty_range_returns_err() {
        // 5..=3 (Included(5), Included(3)) → start > end → Err
        // let range = 5..=3;
        // let result = Demand::try_from_usize_range(&range);
        // assert!(result.is_err());

        // // 5..2 (Included(5), Excluded(2)) → end=1, start>end → Err
        // let range = 5..2;
        // let result = Demand::try_from_usize_range(&range);
        // assert!(result.is_err());

        // ..0 已在上面的 range_to 中测试溢出，但这里测试无溢出但空：..0 因溢出已报错；真正的空范围如 1..1 → start=1, end=0 → Err
        let range = 1..1;
        let result = Demand::try_from_usize_range(&range);
        assert!(result.is_err());
    }

    #[test]
    fn error_returns_original_bounds() {
        // 验证错误时返回的元组与传入的边界引用相同
        // let range = 5..2;
        // let err = Demand::try_from_usize_range(&range).unwrap_err();
        // assert!(matches!(err.0, Bound::Included(&5)));
        // assert!(matches!(err.1, Bound::Excluded(&2)));

        // 对于自定义边界
        struct CustomErr;
        impl RangeBounds<usize> for CustomErr {
            fn start_bound(&self) -> Bound<&usize> {
                Bound::Excluded(&10)
            }
            fn end_bound(&self) -> Bound<&usize> {
                Bound::Included(&5)
            }
        }
        let err = Demand::try_from_usize_range(&CustomErr).unwrap_err();
        assert!(matches!(err.0, Bound::Excluded(&10)));
        assert!(matches!(err.1, Bound::Included(&5)));
    }
}

#[cfg(test)]
mod compromise_tests_ {
    use crate::Demand;

    // 辅助函数：将 Demand<usize> 转换为 (下界, 上界) 的 Option 对
    fn bounds(d: &Demand<usize>) -> (Option<usize>, Option<usize>) {
        (d.min().copied(), d.max().copied())
    }

    #[test]
    fn compromise_both_no_less_than() {
        // [5, ∞) ∩ [10, ∞) = [10, ∞)
        let a = Demand::no_less_than(5);
        let b = Demand::no_less_than(10);
        let c = a.compromise(&b).unwrap();
        assert_eq!(bounds(&c), (Some(10), None));

        // 对称性
        let c2 = b.compromise(&a).unwrap();
        assert_eq!(bounds(&c2), (Some(10), None));
    }

    #[test]
    fn compromise_both_no_more_than() {
        // (-∞, 10] ∩ (-∞, 5] = (-∞, 5]
        let a = Demand::less_than(10);
        let b = Demand::less_than(5);
        let c = a.compromise(&b).unwrap();
        assert_eq!(bounds(&c), (None, Some(5)));

        let c2 = b.compromise(&a).unwrap();
        assert_eq!(bounds(&c2), (None, Some(5)));
    }

    #[test]
    fn compromise_no_less_than_and_no_more_than() {
        // [5, ∞) ∩ (-∞, 10] = [5, 10]
        let a = Demand::no_less_than(5);
        let b = Demand::less_than(10);
        let c = a.compromise(&b).unwrap();
        assert_eq!(bounds(&c), (Some(5), Some(10)));

        // 不相交： [15, ∞) ∩ (-∞, 10] = None
        let a2 = Demand::no_less_than(15);
        let b2 = Demand::less_than(10);
        assert!(a2.compromise(&b2).is_none());
    }

    #[test]
    fn compromise_no_less_than_and_between() {
        // [5, ∞) ∩ [1, 8] = [5, 8]
        let a = Demand::no_less_than(5);
        let b = Demand::between(1, 8);
        let c = a.compromise(&b).unwrap();
        assert_eq!(bounds(&c), (Some(5), Some(8)));

        // 不相交： [15, ∞) ∩ [1, 8] = None
        let a2 = Demand::no_less_than(15);
        assert!(a2.compromise(&b).is_none());

        // 对称性
        let c2 = b.compromise(&a).unwrap();
        assert_eq!(bounds(&c2), (Some(5), Some(8)));
    }

    #[test]
    fn compromise_no_more_than_and_between() {
        // (-∞, 10] ∩ [5, 15] = [5, 10]
        let a = Demand::less_than(10);
        let b = Demand::between(5, 15);
        let c = a.compromise(&b).unwrap();
        assert_eq!(bounds(&c), (Some(5), Some(10)));

        // 不相交： (-∞, 4] ∩ [5, 15] = None
        let a2 = Demand::less_than(4);
        assert!(a2.compromise(&b).is_none());
    }

    #[test]
    fn compromise_between_and_between() {
        // [1, 10] ∩ [5, 15] = [5, 10]
        let a = Demand::between(1, 10);
        let b = Demand::between(5, 15);
        let c = a.compromise(&b).unwrap();
        assert_eq!(bounds(&c), (Some(5), Some(10)));

        // 恰好相接： [1, 5] ∩ [5, 10] = [5,5] -> Exactly(5)
        let a2 = Demand::between(1, 5);
        let b2 = Demand::between(5, 10);
        let c2 = a2.compromise(&b2).unwrap();
        assert_eq!(bounds(&c2), (Some(5), Some(5)));

        // 不相交： [1, 4] ∩ [6, 10] = None
        let a3 = Demand::between(1, 4);
        let b3 = Demand::between(6, 10);
        assert!(a3.compromise(&b3).is_none());
    }

    #[test]
    fn compromise_involving_exactly() {
        // Exactly(5) 被视为 [5,5]
        let exact = Demand::exactly(5);

        // Exactly(5) ∩ [3, 8] = Exactly(5)
        let range = Demand::between(3, 8);
        let c1 = exact.compromise(&range).unwrap();
        assert_eq!(bounds(&c1), (Some(5), Some(5)));

        // Exactly(5) ∩ [6, 10] = None
        let range2 = Demand::between(6, 10);
        assert!(exact.compromise(&range2).is_none());

        // Exactly(5) ∩ (-∞, 10] = Exactly(5)
        let upper = Demand::less_than(10);
        let c2 = exact.compromise(&upper).unwrap();
        assert_eq!(bounds(&c2), (Some(5), Some(5)));

        // Exactly(5) ∩ [5, ∞) = Exactly(5)
        let lower = Demand::no_less_than(5);
        let c3 = exact.compromise(&lower).unwrap();
        assert_eq!(bounds(&c3), (Some(5), Some(5)));

        // Exactly(5) ∩ Exactly(5) = Exactly(5)
        let exact2 = Demand::exactly(5);
        let c4 = exact.compromise(&exact2).unwrap();
        assert_eq!(bounds(&c4), (Some(5), Some(5)));

        // Exactly(5) ∩ Exactly(6) = None
        let exact3 = Demand::exactly(6);
        assert!(exact.compromise(&exact3).is_none());
    }

    #[test]
    fn compromise_unbounded_with_unbounded() {
        // (-∞, ∞) 实际上是 no_less_than(0) 和 no_more_than(usize::MAX) 的组合？
        // 但 Demand 没有直接表示全集的构造，但可以通过 between(0, usize::MAX) 表示。
        // 测试全集与任何区间的交集等于该区间本身。
        let full = Demand::between(usize::MIN, usize::MAX);
        let a = Demand::between(10, 20);
        let c = full.compromise(&a).unwrap();
        assert_eq!(bounds(&c), (Some(10), Some(20)));

        let b = Demand::no_less_than(30);
        let c2 = full.compromise(&b).unwrap();
        assert_eq!(bounds(&c2), (Some(30), Some(usize::MAX)));
    }

    #[test]
    fn compromise_empty_result() {
        // 各种不相交情况
        let a = Demand::between(1, 5);
        let b = Demand::between(6, 10);
        assert!(a.compromise(&b).is_none());

        let c = Demand::no_less_than(10);
        let d = Demand::less_than(5);
        assert!(c.compromise(&d).is_none());

        let e = Demand::no_less_than(8);
        let f = Demand::between(1, 5);
        assert!(e.compromise(&f).is_none());

        let g = Demand::less_than(4);
        let h = Demand::between(5, 10);
        assert!(g.compromise(&h).is_none());
    }
}
