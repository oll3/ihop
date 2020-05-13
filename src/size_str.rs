use std::fmt;
use std::ops::{Add, Div, Sub};

pub trait Int:
    Add<Output = Self> + Sub<Output = Self> + Div<Output = Self> + PartialOrd + PartialEq + Copy
{
    fn val(val: usize) -> Self;
}
impl Int for usize {
    fn val(val: usize) -> Self {
        val as Self
    }
}
impl Int for u32 {
    fn val(val: usize) -> Self {
        val as Self
    }
}
impl Int for u64 {
    fn val(val: usize) -> Self {
        val as Self
    }
}

pub fn size_str<T: Int + fmt::Display>(size: T) -> String {
    if size > T::val(1024 * 1024) {
        format!("{} MiB ({} bytes)", size / T::val(1024 * 1024), size)
    } else if size > T::val(1024) {
        format!("{} KiB ({} bytes)", size / T::val(1024), size)
    } else {
        format!("{} bytes", size)
    }
}
