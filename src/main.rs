use std::fs;
use std::io::Write;

fn main() {
    let mut file = fs::File::create("db.log").expect("Failed to create file");
    file.write_all(b"hello world").expect("Failed to write to file");
    drop(file);
    
    let contents = fs::read_to_string("db.log").expect("Failed to read file");
    println!("{}", contents);
}

