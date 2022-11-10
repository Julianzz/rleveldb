#![feature(error_generic_member_access)]

use std::str;

use rleveldb::{LogReader, LogWriter, LookupKey, MemTable, ValueType};
fn main() {
    let datas = &[
        "liu",
        "zhenzhong",
        "guojia",
        str::from_utf8(&[b'a'; 32 * 1024 * 2 + 20]).unwrap(),
    ];
    println!("hello");
    
    // let mut v = Vec::new();
    // let mut writer = LogWriter::new(&mut v);
    // for data in datas {
    //     writer.add_record(*data).unwrap();
    // }
    // writer.flush().unwrap();

    // println!("-===: {}", v.len());
    // let c = v.as_slice();
    // let mut reader = LogReader::new(c , true);
    // for (i, data) in datas.iter().enumerate() {
    //     let mut dst  = Vec::new();
    //     reader.read_record(&mut dst).unwrap();
    //     // println!("===={}", String::from_utf8(dst).unwrap());
    //     println!("read: {}",i );
    //     assert_eq!(String::from_utf8(dst).unwrap(),*data);
    // }

    // let mut table = MemTable::new();
    // for (_, data) in datas.iter().enumerate() {
    //     table.add(0u64,ValueType::Value, *data, "");
    // }

    // for (_, data) in datas.iter().enumerate() {
    //     let lkey = LookupKey::new(*data,0u64, ValueType::Value);
    //     table.get(lkey);
    // }
}
