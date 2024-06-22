use std::{env, fs::{create_dir_all, File}, io::Write, str::FromStr};

use polars::{frame::row::Row, lazy::dsl::col, prelude::*};
use getopts;

use std::path::Path;






fn main(){
    let args: Vec<String> = env::args().collect();
    let mut parent_path = String::new();
    let mut table_name = String::new();
    let mut opts = getopts::Options::new();
    opts.optopt("d", "dir", "path to dir", "DIR");
    opts.optopt("t", "table", "name of table", "TABLE");

    let matches = match opts.parse(&args[1..]) {
        Ok(m) => { m }
        Err(f) => { panic!("{}",f.to_string()) }
    };
    if let Some(d) = matches.opt_str("d") {
        parent_path = d;
    }
    else{panic!()}
    if let Some(t) = matches.opt_str("t") {
        table_name = t;
    }else{panic!()}
    
    let index_path = Path::new(&parent_path).join("index.parquet");
    
    let table_existance = if index_path.exists(){
        let df = LazyFrame::scan_parquet(index_path.clone(), ScanArgsParquet::default()).unwrap();
        let df = df.filter(col("table").eq(lit(table_name.clone()))).collect().unwrap();
        // if df.is_empty() {uuid::Uuid::new_v4().to_string()}else{df.get(idx)}
        match df["table"].len(){
            0 => {false},
            1 => {true}
            _ => panic!()
        }}else{
        false            
        };
    let index_existance = index_path.exists();
    let parquet_path = if table_existance{
        let df = LazyFrame::scan_parquet(index_path.clone(), ScanArgsParquet::default()).unwrap();
        let df = df.filter(col("table").eq(lit(table_name.clone()))).collect().unwrap();
        Path::new(&df["path"].get(0).unwrap().to_string()).to_path_buf()}
        else{
            let path = Path::new(&parent_path).join(uuid::Uuid::new_v4().to_string());
            let path_string = path.to_str().unwrap();
            let mut df = df!["table" => [table_name],"path"=>[path_string]].unwrap();
            // println!("{}",index_path.to_str().unwrap());
            if !index_existance{
            create_dir_all(index_path.parent().unwrap().clone());
            let w = File::create_new(index_path).unwrap();
            ParquetWriter::new(w).finish(&mut df);
            path}
            else{
                let ex_df = LazyFrame::scan_parquet(index_path.clone(), ScanArgsParquet::default()).unwrap().collect().unwrap();
                let mut df = df.vstack(&ex_df).unwrap();
                let w = File::create(index_path).unwrap();
                ParquetWriter::new(w).finish(&mut df);
                path
            }
        };

    println!("{}",parquet_path.to_str().unwrap());


    


}
