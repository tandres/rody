

use memmap::{Mmap, MmapMut};
use std::{collections::HashMap, fmt::Debug, io::Write, mem::size_of, slice::from_raw_parts};

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
    fn new() -> Self {
        Self {
            magic : Self::FILE_MAGIC,
            version : 1,
        }
    }

    fn from_map<'a>(map: &'a Mmap) -> Result<&'a Header> {
        let ptr = map.as_ref() as *const [u8];
        let ptr = ptr.cast::<Header>();
        let header : Option<&'a Header> = unsafe { ptr.as_ref() };
        let header = header.ok_or_else(|| Error::from("Pointer conversion failed"))?;
        header.validate()
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

pub struct Collector {
    max_size: usize,
    shelves: HashMap<usize, Shelf>,
}

impl Collector {
    pub const DEFAULT_MAX_SIZE: usize = 40;
    pub fn new() -> Self {
        Collector { 
            max_size: Self::DEFAULT_MAX_SIZE, 
            shelves: HashMap::new(),
        }
    }

    pub fn add<T: AsRef<[u8]>>(&mut self, data: T) -> Result<()> {
        let buf = data.as_ref();
        let block_len = buf.len();
        if block_len > self.max_size {
            return Err(Error::TooLarge(block_len));
        }
        let block = Block::new(buf);
        let mut shelf = self.shelves.entry(block_len).or_insert(Shelf::new(block_len));
        shelf.add_block(block);
        Ok(())
    }
}

struct Shelf {
    block_size: usize,
    blocks: Vec<Block>,
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

    fn generate_test_data(size_count: Vec<(usize, usize)>, rng: &mut impl Rng) -> Vec<Vec<Vec<u8>>> {
        let mut data = Vec::new();
        for (size, count) in size_count {
            let mut buffers = Vec::new();
            for _ in 0..count {
                let mut buffer = Vec::with_capacity(size);
                rng.fill(buffer.as_mut_slice());
                buffers.push(buffer);
            }
            data.push(buffers);
        }
        data
    }

    #[test]
    fn header_map_test() {
        let mut wmap = memmap::MmapMut::map_anon(size_of::<Header>()).unwrap();
        let header = Header::new();
        store(&mut wmap, header).unwrap();
        let rmap = wmap.make_read_only().unwrap();
        let rheader = Header::from_map(&rmap).unwrap();
        println!("{rheader:#?}");
    }

    #[test]
    fn random_data() {
        let data_size_count = vec![(5, 3), (7, 2), (10, 1), (25, 6), (39, 4)];
        let mut rng = SmallRng::seed_from_u64(42);
        let data = generate_test_data(data_size_count, &mut rng);
        
    }

    
}
