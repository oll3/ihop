use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::ops::Bound;

#[derive(Eq, PartialEq, Debug, Clone)]
pub struct ChunkOffsetSize {
    pub offset: u64,
    pub size: usize,
}

impl ChunkOffsetSize {
    pub fn new(offset: u64, size: usize) -> Self {
        Self { offset, size }
    }
    pub fn end(&self) -> u64 {
        self.offset + self.size as u64
    }
}

impl Ord for ChunkOffsetSize {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.offset.cmp(&other.offset) {
            Ordering::Equal => self.size.cmp(&other.size),
            v => v,
        }
    }
}

impl PartialOrd for ChunkOffsetSize {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Default)]
pub struct ChunkMap<V> {
    btm: BTreeMap<ChunkOffsetSize, V>,
}

impl<V> ChunkMap<V> {
    pub fn new() -> Self {
        Self {
            btm: BTreeMap::new(),
        }
    }
    pub fn insert(&mut self, location: ChunkOffsetSize, value: V) {
        self.btm.insert(location, value);
    }
    pub fn iter_overlapping(
        &self,
        location: ChunkOffsetSize,
    ) -> impl Iterator<Item = (&ChunkOffsetSize, &V)> {
        let location_offset = location.offset;
        self.btm
            .range((
                Bound::Unbounded,
                Bound::Excluded(ChunkOffsetSize::new(location.end(), 0)),
            ))
            .rev()
            .take_while(move |(loc, _v)| location_offset < loc.end())
    }
}
