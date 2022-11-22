use std::{fs, io::Read, path::Path};

use regex::Regex;

use crate::error::Result;

#[derive(Default)]
pub struct TestData {
    pos: String,
    cmd: String,
    cmd_args: Vec<CmdArg>,
    input: String,
    expected: String,
}

pub struct CmdArg {
    key: String,
    vals: Vec<String>,
}

impl CmdArg {
    pub fn string(&self, idx: usize) -> String {
        self.vals[idx].clone()
    }
    pub fn int64(&self, idx: usize) -> i64 {
        self.vals[idx].parse().unwrap()
    }
    pub fn uint64(&self, idx: usize) -> u64 {
        self.vals[idx].parse().unwrap()
    }
    pub fn bool(&self, idx: usize) -> bool {
        self.vals[idx].parse().unwrap()
    }
}

impl TestData {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn find_arg(&self, key: &str) -> Option<&CmdArg> {
        self.cmd_args.iter().find(|&f| f.key == key)
    }
    pub fn scan_args(&self, key: &str) -> &CmdArg {
        if let Some(arg) = self.find_arg(key) {
            arg
        } else {
            panic!("missing args: {}", key);
        }
    }
    pub fn has_arg(&self, key: &str) -> bool {
        self.find_arg(key).is_some()
    }
}

pub fn run_test<F: Fn(&TestData) -> String>(path: impl AsRef<Path>, f: F) {
    let path = path.as_ref();
    let mut file = fs::OpenOptions::new().read(true).open(path).unwrap();
    let mut content = String::new();
    file.read_to_string(&mut content).unwrap();
}

pub fn run_test_from_string<F: Fn(&TestData) -> String>(input: impl AsRef<str>, f: F) {
    let input = input.as_ref();
    let datas = parse_test_data(input, "").unwrap();
    for data in datas.iter() {
        let s = f(data);
        assert_eq!(s, data.expected);
    }
}

pub fn parse_test_data(input: &str, source: &str) -> Result<Vec<TestData>> {
    let mut datas = Vec::new();

    let mut iter = input.lines().enumerate();
    loop {
        let mut data = TestData::default();
        if let Some((line_no, line)) = iter.next() {
            if line.starts_with("#") {
                continue;
            }
            let fields = split_directive(line);
            if fields.is_empty() {
                continue;
            }

            data.pos = format!("{}:{}", source, line_no);
            data.cmd = fields[0].clone();
            for arg in &fields[1..] {
                if let Some(idx) = arg.find("=") {
                    let key = arg[0..idx].to_owned();
                    let val = &arg[idx + 1..];

                    let mut vals = Vec::new();
                    if val.len() > 2
                        && val.as_bytes()[0] == b'('
                        && val.as_bytes()[val.len() - 1] == b')'
                    {
                        vals = val[1..val.len() - 1]
                            .split(",")
                            .map(|s| s.trim().to_owned())
                            .collect();
                    } else {
                        vals.push(val.to_owned());
                    }
                    data.cmd_args.push(CmdArg { key, vals });
                } else {
                    data.cmd_args.push(CmdArg {
                        key: arg.clone(),
                        vals: Vec::new(),
                    })
                }
            }
            let mut buf = String::new();
            let mut seperator = false;
            loop {
                if let Some((_, line)) = iter.next() {
                    if line == "----" {
                        seperator = true;
                        break;
                    }
                    buf.push_str(line);
                    buf.push_str("\n");
                } else {
                    break;
                }
            }
            data.input = buf.trim().to_owned();
            if seperator {
                let mut buf = String::new();
                loop {
                    if let Some((_, line)) = iter.next() {
                        let line = line.trim();
                        buf.push_str(line);
                        buf.push_str("\n");
                    } else {
                        break;
                    }
                }
                data.expected = buf;
            }
            datas.push(data);
        } else {
            break;
        }
    }

    Ok(datas)
}
const PATTERN: &str = r"^ *[a-zA-Z0-9_/,-\.]+(|=[-a-zA-Z0-9_@]+|=\([^)]*\))( |$)";

fn split_directive(mut line: &str) -> Vec<String> {
    let p = Regex::new(PATTERN).unwrap();
    let mut results = Vec::new();
    while line != "" {
        let m = p
            .find(line)
            .expect(&format!("cannot parse directive: {}", line));
        let v = m.as_str();
        line = &line[v.len()..];
        results.push(v.trim().to_string());
    }
    results
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_directive() {
        let input = "make argTuple=(1, 🍌) argInt=12 argString=greedily argString=totally_ignored";
        let cmds = split_directive(input);
        assert_eq!(cmds.len(), 5);
        assert_eq!(cmds[0], "make");
        assert_eq!(cmds.last().unwrap(), "argString=totally_ignored");
    }

    #[test]
    fn test_from_string() {
        let input = r"
# NB: we allow duplicate args. It's unclear at this time whether this is useful,
# either way, ScanArgs simply picks the first occurrence.
make argTuple=(1, 🍌) argInt=12 argString=greedily argString=totally_ignored
sentence
----
Did the following: make sentence
1 hungry monkey eats a 🍌
while 12 other monkeys watch greedily
";
        run_test_from_string(input, |t| {
            assert_eq!(t.cmd, "make");
            assert_eq!(t.input, "sentence");
            assert_eq!(t.cmd_args.len(), 4);
            let arg_str = t.scan_args("argString");
            assert_eq!(arg_str.string(0), "greedily");
            let arg_int = t.scan_args("argInt");
            assert_eq!(arg_int.int64(0), 12);
            let arg_tuple = t.scan_args("argTuple");
            assert_eq!(arg_tuple.int64(0), 1);
            format!(
                "Did the following: {} {}\n{} hungry monkey eats a {}\nwhile {} other monkeys watch {}\n",
                t.cmd,
                t.input,
                arg_tuple.int64(0),
                arg_tuple.string(1),
                arg_int.int64(0),
                arg_str.string(0)
            )
        });
    }
}
