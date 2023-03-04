

use memmap::{Mmap, MmapMut};
use std::{collections::BTreeMap, fmt::Debug, io::Write, mem::size_of, slice::from_raw_parts};

pub use crate::error::{Error, Result};

mod error;

pub fn store(map: &mut MmapMut, header: Header) -> Result<()> {
    let mut buf: &mut [u8] = map.as_mut();
    buf.write(header.as_ref())?;
    map.flush()?;
    Ok(())
}


#[repr(C, packed)]
pub struct Header {
    magic : u32,
    version : u32,
    blocklist_size : u32,
}

impl Debug for Header {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let magic = self.magic;
        let version = self.version;
        f.debug_struct("Header")
            .field("Magic", &format_args!("{:x}", magic))
            .field("Version", &version)
            .finish()
    }
}

impl AsRef<[u8]> for Header {
    fn as_ref(&self) -> &[u8] {
        let ptr = self as *const Self as *const u8;
        unsafe {
            from_raw_parts(ptr, size_of::<Self>())
        }
    }
}


impl Header {
    const FILE_MAGIC: u32 = 0x55AA33BB;
    fn new(blocklist_size: usize) -> Self {
        let blocklist_size = blocklist_size as u32;
        Self {
            magic : Self::FILE_MAGIC,
            version : 1,
            blocklist_size,
        }
    }

    fn from_map<'a>(map: &'a Mmap) -> Result<&'a Header> {
        Self::from_buf(map.as_ref())
    }

    fn from_buf<'a>(buf: &'a [u8]) -> Result<&'a Header> {
        let ptr = buf as *const [u8];
        let ptr = ptr.cast::<Header>();
        let header : Option<&'a Header> = unsafe { ptr.as_ref() };
        let header = header.ok_or_else(|| Error::from("Pointer conversion failed"))?;
        header.validate()
    }

    fn write_out<W: Write>(&self, writer: &mut W) -> Result<usize> {
        let mut size = writer.write(&self.magic.to_le_bytes())?;
        size += writer.write(&self.version.to_le_bytes())?;
        size += writer.write(&self.blocklist_size.to_le_bytes())?;
        Ok(size)
    }

    fn validate(&self) -> Result<&Self> {
        if self.magic != Self::FILE_MAGIC {
            return Err(Error::BadMagic);
        }
        if self.version != 1 {
            return Err(Error::InvalidVersion);
        }
        return Ok(self)
    }
}

#[repr(C, packed)]
pub struct BlockDesc {
    block_size: u32,
    count: u32,
    offset: u32,
}

impl BlockDesc {
    fn from_map<'a>(map: &'a Mmap, offset: usize) -> Result<&'a BlockDesc> {
        Self::from_buf(map.as_ref())
    }

    fn from_buf<'a>(buf: &'a [u8]) -> Result<&'a BlockDesc> {
        let ptr = buf as *const [u8];
        let ptr = ptr.cast::<BlockDesc>();
        let blockdesc: Option<&'a BlockDesc> = unsafe { ptr.as_ref() };
        let blockdesc = blockdesc.ok_or_else(|| Error::from("Pointer conversion failed"))?;
        blockdesc.validate(buf.len())
    }

    fn validate(&self, buffer_length: usize) -> Result<&Self> {
        let total_size = self.block_size * self.count;
        let buffer_length = buffer_length as u32;
        if self.offset + total_size > buffer_length {
            let count = self.count;
            let size = self.block_size;
            Err(format!("Blocklist ({count} blocks at {size} bytes each) would overrun buffer ({buffer_length} bytes)").into())
        } else {
            Ok(self)
        } 
    }
}

impl<'a> TryFrom<&'a [u8]> for &'a BlockDesc {
    type Error = Error;
    fn try_from(value: &'a [u8]) -> Result<&'a BlockDesc> {
        BlockDesc::from_buf(value)
    }
}

pub struct Collector {
    max_size: usize,
    shelves: BTreeMap<usize, Shelf>,
}

impl Collector {
    pub const DEFAULT_MAX_SIZE: usize = 40;
    pub fn new() -> Self {
        Collector { 
            max_size: Self::DEFAULT_MAX_SIZE, 
            shelves: BTreeMap::new(),
        }
    }

    pub fn add<T: AsRef<[u8]>>(&mut self, data: T) -> Result<()> {
        let buf = data.as_ref();
        let block_len = buf.len();
        if block_len > self.max_size {
            return Err(Error::TooLarge(block_len));
        }
        let block = Block::new(buf);
        let shelf = self.shelves.entry(block_len).or_insert(Shelf::new(block_len));
        shelf.add_block(block);
        Ok(())
    }

    pub fn press<F: Write>(&self, writer: &mut F) -> Result<()> {
        let header = Header::new(self.shelves.len());
        header.write_out(writer)?;
        for (size, blocks) in self.shelves.iter() {
            println!("{size} {:?}", blocks);
        }
        Ok(())
    }
}

struct Shelf {
    block_size: usize,
    blocks: Vec<Block>,
}

impl Debug for Shelf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Shelf")
            .field("block_size", &self.block_size)
            .field("blocks", &self.blocks.len())
            .finish()
    }
}

impl Shelf {
    fn new(block_size: usize) -> Self {
        Self {
            block_size,
            blocks: Vec::new(),
        }
    }

    fn add_block(&mut self, block: Block) {
        assert_eq!(self.block_size, block.data.len());
        self.blocks.push(block); 
    }
}

struct Block {
    hash: u32,
    data: Vec<u8>,
}

impl Block {
    fn new(data: &[u8]) -> Self {
        Self {
            hash: 0,
            data: data.to_vec(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempfile;
    use rand::{Rng, rngs::SmallRng, SeedableRng};

    fn generate_test_data(size_count: Vec<(usize, usize)>, rng: &mut impl Rng) -> Vec<Vec<u8>> {
        let mut data = Vec::new();
        for (size, count) in size_count {
            for _ in 0..count {
                let mut buffer = vec![0; size];
                rng.fill(buffer.as_mut_slice());
                data.push(buffer);
            }
        }
        data
    }

    #[test]
    fn header_map_test() {
        let mut output = tempfile().unwrap();
        let header = Header::new(0);
        header.write_out(&mut output).unwrap();
        //store(&mut wmap, header).unwrap();
        let rmap = unsafe { memmap::Mmap::map(&output) }.unwrap();
        let rheader = Header::from_map(&rmap).unwrap();
        println!("{rheader:#?}");
    }

    #[test]
    fn random_data() {
        let mut output = tempfile().unwrap();
        let data_size_count = vec![(5, 3), (7, 2), (10, 1), (25, 6), (39, 4)];
        let mut rng = SmallRng::seed_from_u64(42);
        let data = generate_test_data(data_size_count, &mut rng);
        let mut collector = Collector::new();
        for buffer in data {
            collector.add(buffer).unwrap();
        }
        collector.press(&mut output).unwrap(); 
    }
}
