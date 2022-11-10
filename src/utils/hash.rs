use murmur3::murmur3_32;

pub fn bloom_hash(key: &[u8]) -> u32 {
    let mut key = key;
    murmur3_32(&mut key, 0xbc9f1d34).unwrap()
}

// pub fn hash(data: &[u8], seed: u32) -> u32 {
//     // Similar to murmur hash
//     let n = data.len();
//     let m: u32 = 0xc6a4a793;
//     let r: u32 = 24;
//     let mut h = seed ^ (m.wrapping_mul(n as u32));
//     let mut buf = data;
//     while buf.len() >= 4 {
//         let w = buf.read_u32::<LittleEndian>().unwrap();
//         h = h.wrapping_add(w);
//         h = h.wrapping_mul(m);
//         h ^= h >> 16;
//     }

//     for i in (0..buf.len()).rev() {
//         h += u32::from(buf[i]) << (i * 8) as u32;
//         if i == 0 {
//             h = h.wrapping_mul(m);
//             h ^= h >> r;
//         }
//     }
//     h
// }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_name() {
        let hash = bloom_hash("liuzhenzhon".as_bytes());
        let hash2 = bloom_hash("liuzhenzhong".as_bytes());
        let hash3 = bloom_hash("liuzhenzhon".as_bytes());

        assert_eq!(hash, hash3);
        assert_ne!(hash, hash2);
        eprintln!("{}", hash);
    }
}
