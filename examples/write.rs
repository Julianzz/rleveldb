use rleveldb::{LevelDB, Options, PosixEnv};

fn main() {
    let mut options = Options::default();
    options.create_if_missing = true;
    let db_name = "demo";
    let env = PosixEnv {};
    let db = LevelDB::open(options, db_name, env).unwrap();
    for i in 0..20 {
        db.write(format!("liu{}", i).as_bytes(), "zhong".as_bytes())
            .unwrap();
    }
}
