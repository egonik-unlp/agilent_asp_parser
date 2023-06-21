#![feature(iter_next_chunk)]

use itertools::Itertools;
use itertools_num::linspace;
use polars::prelude::*;
use split_iter::Splittable;
use std::error::Error;
use std::fs;
use std::path::Path;
use walkdir::{DirEntry, WalkDir};

#[derive(Debug)]
struct Spectrum {
    filename: String,
    wavenumber_grid: Vec<f64>,
    transmittance_grid: Vec<f64>,
}

impl Spectrum {
    fn new(filename: String, wng: Vec<f64>, tng: Vec<f64>) -> Spectrum {
        Spectrum {
            filename: filename,
            wavenumber_grid: wng,
            transmittance_grid: tng,
        }
    }
    fn to_csv(&self) -> Result<String, Box<dyn Error>> {
        let mut df: DataFrame = df!(
            "wavenumber" => &self.wavenumber_grid,
            "transmittance" => &self.transmittance_grid
        )?;
        let conv_filename = format!(
            "./exportados/{}.csv",
            &self.filename[..self.filename.len() - 4]
        );
        let mut file = std::fs::File::create(&conv_filename)?;
        CsvWriter::new(&mut file).finish(&mut df)?;
        Ok(String::from(&conv_filename))
    }
}
#[derive(Debug)]
struct Spectra {
    data: Vec<Spectrum>,
}

fn extension_is_asp(filename: &String) -> bool {
    let path = Path::new(filename).extension();
    match path {
        Some(i) => i.to_str().unwrap_or("basura").eq("asp"),
        None => false,
    }
}

impl Spectra {
    fn build_from_path(path: &str) -> Result<Spectra, Box<dyn Error>> {
        let walker = WalkDir::new(path);
        let (files, dirs) = walker
            .into_iter()
            .split(|path| path.as_ref().unwrap().path().is_dir());
        let newly_created_folders = dirs
            .into_iter()
            // .filter(|x| x.as_ref().unwrap().path().is_dir())
            .filter(|dir| {
                dir.as_ref()
                    .unwrap()
                    .path()
                    .into_iter()
                    .any(|path_name| !path_name.eq("exportados"))
            })
            .map(|node| node.unwrap())
            .collect::<Vec<_>>();
        handle_folders(newly_created_folders);
        let spectral_files = files
            .into_iter()
            .map(|x| x.unwrap().path().display().to_string())
            .filter(|x| extension_is_asp(x))
            .collect::<Vec<_>>();
        let spectrum_vector = spectral_files
            .into_iter()
            .map(|x| handle_one_file(&x).unwrap())
            //  .filter(|x| x.is_ok()).
            .collect::<Vec<_>>();
        Ok(Spectra {
            data: spectrum_vector,
        })
    }
    fn export_all(self) -> () {
        for file in self.data.into_iter() {
            println!("leyendo archivo {}", file.filename);
            let filename = file.to_csv();
            match filename {
                Ok(dato) => println!("Exportado como {}", dato),
                Err(e) => println!("error => {:?}", e),
            }
        }
    }
}

fn handle_folders(paths: Vec<DirEntry>) {
    let basepath = Path::new("exportados");
    for foldpath in paths.into_iter() {
        let pth = basepath.join(foldpath.path());
        if pth.ne(basepath) {
            fs::create_dir_all(pth).unwrap();
        }
    }
}

fn main() {
    let spectra = Spectra::build_from_path(".").unwrap();
    spectra.export_all();
}

fn handle_one_file(filename: &str) -> Result<Spectrum, Box<dyn Error>> {
    let contents = fs::read_to_string(filename)?;
    let mut contents = contents.lines();
    //  .into_iter()
    //  .map(|x| x.parse::<f64>())
    //  .collect()?;
    let (ln, hwn, lwn): (f64, f64, f64) = contents
        .next_chunk::<3>()
        .unwrap()
        .into_iter()
        .filter_map(|x| x.parse::<f64>().ok())
        .collect_tuple()
        .unwrap();
    let contents = contents.into_iter().skip(3);

    let wng = linspace::<f64>(hwn, lwn, ln as usize).collect();

    let tnsg: Vec<f64> = contents
        .into_iter()
        .filter_map(|x| x.parse::<f64>().ok())
        .collect();
    let spec = Spectrum::new(filename.to_owned(), wng, tnsg);
    Ok(spec)
}
