use crate::common::HasLen;
use stable_deref_trait::{CloneStableDeref, StableDeref};
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Weak};
use std::io::{Read, Seek, Cursor, SeekFrom};
use std::convert::TryInto;
use std::cmp;

pub struct BoxedData(Arc<Box<dyn Deref<Target = [u8]> + Send + Sync + 'static>>);

impl Deref for BoxedData {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl AsRef<[u8]> for BoxedData {
    fn as_ref(&self) -> &[u8] {
        &self.0.as_ref()
    }
}

impl BoxedData {
    pub fn new(data: Arc<Box<dyn Deref<Target = [u8]> + Send + Sync + 'static>>) -> Self {
        BoxedData(data)
    }
    pub(crate) fn downgrade(&self) -> Weak<Box<dyn Deref<Target = [u8]> + Send + Sync + 'static>> {
        Arc::downgrade(&self.0)
    }
}

impl Clone for BoxedData {
    fn clone(&self) -> Self {
        BoxedData(self.0.clone())
    }
}

/// Read object that represents files in tantivy.
///
/// These read objects are only in charge to deliver
/// the data in the form of a constant read-only `&[u8]`.
/// Whatever happens to the directory file, the data
/// hold by this object should never be altered or destroyed.
pub struct ReadOnlySource {
    data: Cursor<BoxedData>,
    start: usize,
    stop: usize,
    pos: usize,
}

impl From<BoxedData> for ReadOnlySource {
    fn from(data: BoxedData) -> Self {
        let len = data.len();
        ReadOnlySource {
            data: Cursor::new(data),
            start: 0,
            stop: len,
            pos: 0,
        }
    }
}

pub struct AdvancingReadOnlySource(ReadOnlySource);

impl AsRef<[u8]> for AdvancingReadOnlySource {
    fn as_ref(&self) -> &[u8] {
        &self.0.as_slice()
    }
}

impl AdvancingReadOnlySource {
    pub fn empty() -> AdvancingReadOnlySource {
        AdvancingReadOnlySource(ReadOnlySource::empty())
    }

    pub fn advance(&mut self, clip_len: usize) {
        self.0.start += clip_len;
        self.0.seek(SeekFrom::Start(0)).expect("Can't seek while advancing");
    }

    pub fn split(self, addr: usize) -> (AdvancingReadOnlySource, AdvancingReadOnlySource) {
        let (left, right) = self.0.split(addr);
        (AdvancingReadOnlySource::from(left), AdvancingReadOnlySource::from(right))
    }

    pub fn get(&mut self, idx: usize) -> u8 {
        self.0.get(idx).expect("Can't get a byte out of the reader")
    }

    pub fn read_all(&mut self) -> std::io::Result<Vec<u8>> {
        self.0.read_all()
    }

    pub fn read_without_advancing(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let current_location = self.0.seek(SeekFrom::Current(0))?;
        let n = self.0.read(buf)?;
        self.0.seek(SeekFrom::Start(current_location))?;
        Ok(n)
    }

    pub fn is_empty(&self) -> bool {
        self.0.start == self.0.stop
    }
}

impl From<ReadOnlySource> for AdvancingReadOnlySource {
    fn from(source: ReadOnlySource) -> AdvancingReadOnlySource {
        AdvancingReadOnlySource(source)
    }
}

impl From<Vec<u8>> for AdvancingReadOnlySource {
    fn from(data: Vec<u8>) -> AdvancingReadOnlySource {
        AdvancingReadOnlySource::from(ReadOnlySource::from(data))
    }
}

impl Clone for AdvancingReadOnlySource {
    fn clone(&self) -> Self {
        AdvancingReadOnlySource(self.0.slice_from(0))
    }
}

impl Read for AdvancingReadOnlySource {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.0.read(buf)?;
        // println!("READ {:?}", buf);
        self.advance(n);
        Ok(n)
    }
}

impl ReadOnlySource {
    pub(crate) fn new<D>(data: D) -> ReadOnlySource
    where
        D: Deref<Target = [u8]> + Send + Sync + 'static,
    {
        let len = data.as_ref().len();
        ReadOnlySource {
            data: Cursor::new(BoxedData(Arc::new(Box::new(data)))),
            start: 0,
            stop: len,
            pos: 0,
        }
    }

    /// Creates an empty ReadOnlySource
    pub fn empty() -> ReadOnlySource {
        ReadOnlySource::new(&[][..])
    }

    /// Returns the data underlying the ReadOnlySource object.
    pub fn as_slice(&self) -> &[u8] {
        &self.data.get_ref()[self.start..self.stop]
    }

    /// Splits into 2 `ReadOnlySource`, at the offset given
    /// as an argument.
    pub fn split(self, addr: usize) -> (ReadOnlySource, ReadOnlySource) {
        let left = self.slice(0, addr);
        let right = self.slice_from(addr);
        (left, right)
    }

    pub fn read_after_skip(&mut self, size: usize) -> std::io::Result<Vec<u8>> {
        let current_location = self.seek(SeekFrom::Current(0))?;
        self.seek(SeekFrom::Start(size.try_into().unwrap()))?;
        let mut ret = Vec::new();
        self.read_to_end(&mut ret)?;
        self.seek(SeekFrom::Start(current_location))?;
        Ok(ret)
    }

    pub fn read_all(&mut self) -> std::io::Result<Vec<u8>> {
        let mut ret = Vec::new();
        self.read_to_end(&mut ret)?;
        self.seek(SeekFrom::Start(0))?;
        Ok(ret)
    }

    pub fn get(&mut self, idx: usize) -> std::io::Result<u8> {
        let current_location = self.seek(SeekFrom::Current(0))?;
        let mut ret = vec![0u8; 1];
        self.seek(SeekFrom::Start(idx as u64))?;
        self.read_exact(&mut ret)?;
        self.seek(SeekFrom::Start(current_location))?;
        Ok(ret[0])
    }

    /// Creates a ReadOnlySource that is just a
    /// view over a slice of the data.
    ///
    /// Keep in mind that any living slice extends
    /// the lifetime of the original ReadOnlySource,
    ///
    /// For instance, if `ReadOnlySource` wraps 500MB
    /// worth of data in anonymous memory, and only a
    /// 1KB slice is remaining, the whole `500MBs`
    /// are retained in memory.
    pub fn slice(&self, start: usize, stop: usize) -> ReadOnlySource {
        assert!(
            start <= stop,
            "Requested negative slice [{}..{}]",
            start,
            stop
        );
        assert!(stop <= self.len());

        let data: BoxedData = self.data.get_ref().clone();
        let mut data = Cursor::new(data);
        data.seek(SeekFrom::Start((self.start + start).try_into().expect("Bla"))).expect("HEllo");

        ReadOnlySource {
            data,
            start: self.start + start,
            stop: self.start + stop,
            pos: self.start + start,
        }
    }

    /// Like `.slice(...)` but enforcing only the `from`
    /// boundary.
    ///
    /// Equivalent to `.slice(from_offset, self.len())`
    pub fn slice_from(&self, from_offset: usize) -> ReadOnlySource {
        self.slice(from_offset, self.len())
    }

    /// Like `.slice(...)` but enforcing only the `to`
    /// boundary.
    ///
    /// Equivalent to `.slice(0, to_offset)`
    pub fn slice_to(&self, to_offset: usize) -> ReadOnlySource {
        self.slice(0, to_offset)
    }
}

impl Read for ReadOnlySource {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let max = cmp::min(buf.len() , self.stop - self.pos);

        let n = self.data.read(&mut buf[..max])?;
        // println!("HELLO READING {} {} {} {} max {} read {}", self.pos, buf.len(), self.stop, self.pos + buf.len(), max, n);
        self.pos += n;
        Ok(n)
    }
}

impl Seek for ReadOnlySource {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let pos = match pos {
            SeekFrom::Start(n) => {
                let n = n.checked_add(self.start.try_into().unwrap()).expect("Can't add");
                SeekFrom::Start(n)
            },
            SeekFrom::End(n) => {
                let n = self.stop.checked_sub(n.wrapping_neg().try_into().unwrap()).expect("Can't substract");
                SeekFrom::End(n.try_into().unwrap())
            },
            SeekFrom::Current(n) => {
                // TODO check that n doesn't leave the bounds of our source.
                SeekFrom::Current(n)
            },
        };

        let pos = self.data.seek(pos)?;
        self.pos = pos as usize;

        Ok(pos)
    }
}

impl HasLen for ReadOnlySource {
    fn len(&self) -> usize {
        self.stop - self.start
    }
}

impl Clone for ReadOnlySource {
    fn clone(&self) -> Self {
        self.slice_from(0)
    }
}

impl From<Vec<u8>> for ReadOnlySource {
    fn from(data: Vec<u8>) -> ReadOnlySource {
        ReadOnlySource::new(data)
    }
}
