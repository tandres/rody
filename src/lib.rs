use std::{fmt::Debug, io::Write, mem::size_of, slice::from_raw_parts};

use memmap::{Mmap, MmapMut};


pub fn store(map: &mut MmapMut, header: Header) {
    let mut buf: &mut [u8] = map.as_mut();
    buf.write(header.as_ref()).unwrap();
    map.flush().unwrap()
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

    fn from_map<'a>(map: &'a Mmap) -> &'a Header {
        let ptr = map.as_ref() as *const [u8];
        let ptr = ptr.cast::<Header>();
        unsafe { ptr.as_ref().unwrap() }
    }

    fn validate(&self) -> bool {
        if self.magic != Self::FILE_MAGIC {
            return false;
        }
        if self.version != 1 {
            return false;
        }
        return true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempfile;

    #[test]
    fn header_map_test() {
        let mut wmap = memmap::MmapMut::map_anon(size_of::<Header>()).unwrap();
        let header = Header::new();
        store(&mut wmap, header);
        let rmap = wmap.make_read_only().unwrap();
        let rheader = Header::from_map(&rmap);
        println!("{rheader:#?}");
        assert!(rheader.validate());
    }
}
