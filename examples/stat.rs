use rleveldb::{LevelDB, Options, PosixEnv};

fn main() {
    let mut options = Options::default();
    options.create_if_missing = true;
    let db_name = "demo";
    let env = PosixEnv {};
    let db = LevelDB::open(options, db_name, env).unwrap();
    db.debug_print();
}
