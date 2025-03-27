use std::{
    collections::HashMap,
    fs::remove_file,
    io::IsTerminal,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Result, anyhow};
use arrow_csv::{ReaderBuilder, reader::Format};
use arrow_schema::{DataType, Field, Fields, Schema};
use clap::{Parser, ValueHint};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, GzipLevel},
    file::properties::WriterProperties,
};

mod rewindable_reader;
mod tempfile;

use rewindable_reader::RewindableReader;
use tempfile::TempFile;

/// Number of rows to read from csv to infer schema
pub const MAX_READ_RECORDS: usize = 8192;

/// Default integer data type
pub const DEFAULT_INT_TYPE: DataType = DataType::Int64;

/// Default float data type
pub const DEFAULT_FLOAT_TYPE: DataType = DataType::Float32;

#[derive(Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[command(about = "CSV to Apache Parquet converter")]
struct Args {
    /// Input .csv[.gz] files
    #[clap(name = "CSV-FILES", required=true, value_parser, value_hint = ValueHint::AnyPath)]
    input: Vec<PathBuf>,

    /// Comma separated list of Int32 columns. Use "*" or "__all__" to set Int32 as the default
    /// type for integer columns.
    #[clap(long, value_delimiter = ',', value_name = "COLUMNS")]
    i32: Option<Vec<String>>,

    /// Comma separated list of Int64 columns. Int64 is the default type for integer columns.
    #[clap(long, value_delimiter = ',', value_name = "COLUMNS")]
    i64: Option<Vec<String>>,

    /// Comma separated list of Float32 columns. Float32 is the default type for float columns.
    #[clap(long, value_delimiter = ',', value_name = "COLUMNS")]
    f32: Option<Vec<String>>,

    /// Comma separated list of Float64 columns. Use "*" or "__all__" to set Float64
    /// as the default type for float columns
    #[clap(long, value_delimiter = ',', value_name = "COLUMNS")]
    f64: Option<Vec<String>>,

    /// Print the inferred Parquet schema and exit
    #[clap(short, long)]
    print_schema: bool,

    /// Remove input files after conversion
    #[clap(long)]
    rm: bool,
}

/// Consolidate i32, i64, f32 and f32 parameters to a HashMap
fn consolidate_types(args: &mut Args) -> Result<(HashMap<String, DataType>, DataType, DataType)> {
    let mut overrides = HashMap::new();
    let mut default_int_type = DEFAULT_INT_TYPE;
    let mut default_float_type = DEFAULT_FLOAT_TYPE;

    if args.i32.is_none() && args.i64.is_none() && args.f32.is_none() && args.f64.is_none() {
        // There is no need to make any changes to the current scheme
        return Ok((overrides, default_int_type, default_float_type));
    }

    let i32_cols = args.i32.take().unwrap_or_default();
    let i64_cols = args.i64.take().unwrap_or_default();
    let f32_cols = args.f32.take().unwrap_or_default();
    let f64_cols = args.f64.take().unwrap_or_default();

    let all1 = "*".to_string();
    let all2 = "__all__".to_string();
    if (i32_cols.contains(&all1) || i32_cols.contains(&all2))
        && (i64_cols.contains(&all1) || i64_cols.contains(&all2))
    {
        return Err(anyhow!(
            "i32 and i64 can't both be the default data types for integers".to_string(),
        ));
    }
    if (f32_cols.contains(&all1) || f32_cols.contains(&all2))
        && (f64_cols.contains(&all1) || f64_cols.contains(&all2))
    {
        return Err(anyhow!(
            "f32 and f64 can't both be the default data types for floats".to_string()
        ));
    }

    for c in &i32_cols {
        if c == "*" || c == "__all__" {
            default_int_type = DataType::Int32;
        } else {
            overrides.insert(c.clone(), DataType::Int32);
        }
    }
    for c in &i64_cols {
        if c == "*" || c == "__all__" {
            default_int_type = DataType::Int64;
        } else if overrides.insert(c.clone(), DataType::Int64).is_some() {
            return Err(anyhow!(
                "Data type for column `{c}' was specified multiple times",
            ));
        }
    }
    for c in &f32_cols {
        if c == "*" || c == "__all__" {
            default_float_type = DataType::Float32;
        } else if overrides.insert(c.clone(), DataType::Float32).is_some() {
            return Err(anyhow!(
                "Data type for column `{c}' was specified multiple times",
            ));
        }
    }
    for c in &f64_cols {
        if c == "*" || c == "__all__" {
            default_float_type = DataType::Float64;
        } else if overrides.insert(c.clone(), DataType::Float64).is_some() {
            return Err(anyhow!(
                "Data type for column `{c}' was specified multiple times",
            ));
        }
    }
    Ok((overrides, default_int_type, default_float_type))
}

/// Applies user-provided data types to the schema
fn apply_schema_overrides(
    schema: &mut Schema,
    overrides: &HashMap<String, DataType>,
    default_int_type: DataType,
    default_float_type: DataType,
) -> Result<()> {
    let mut new_fields: Vec<Field> = Vec::with_capacity(schema.fields.len());
    for field in &schema.fields {
        let name = field.name();
        if let Some(datatype) = overrides.get(name) {
            new_fields.push(Field::new(name, datatype.clone(), field.is_nullable()));
        } else {
            match field.data_type() {
                &DataType::Int64 => new_fields.push(Field::new(
                    name,
                    default_int_type.clone(),
                    field.is_nullable(),
                )),
                &DataType::Float64 => new_fields.push(Field::new(
                    name,
                    default_float_type.clone(),
                    field.is_nullable(),
                )),
                datatype => {
                    new_fields.push(Field::new(name, datatype.clone(), field.is_nullable()))
                }
            }
        }
    }
    schema.fields = Fields::from(new_fields);
    Ok(())
}

/// Converts a single csv file to parquet
fn process(
    filename: &Path,
    args: &Args,
    overrides: &HashMap<String, DataType>,
    default_int_type: &DataType,
    default_float_type: &DataType,
) -> Result<()> {
    if !filename.is_file() {
        if !filename.exists() {
            eprintln!("{} not found", filename.to_str().unwrap());
        } else {
            eprintln!("{} is not a file -- skipping", filename.to_str().unwrap());
        }
        return Ok(());
    }

    let mut reader = RewindableReader::open(filename)?;

    let format = Format::default().with_header(true).with_delimiter(b',');
    let (mut schema, _size) = format.infer_schema(&mut reader, Some(MAX_READ_RECORDS))?;
    apply_schema_overrides(
        &mut schema,
        overrides,
        default_int_type.clone(),
        default_float_type.clone(),
    )?;
    if args.print_schema {
        let json = serde_json::to_string_pretty(&schema)?;
        let filename = filename.to_str().unwrap();
        println!("{filename}:\n{json}\n");
        return Ok(());
    }

    let basename = filename.file_name().unwrap().to_str().unwrap();
    let mut new_filename: PathBuf = filename.to_path_buf();
    new_filename.pop();
    let mut basename = if let Some(basename) = basename.strip_suffix(".csv") {
        basename.to_string()
    } else if let Some(basename) = basename.strip_suffix(".csv.gz") {
        basename.to_string()
    } else {
        eprintln!(
            "{} is not a csv[.gz] file -- skipping",
            filename.to_str().unwrap()
        );
        return Ok(());
    };
    basename.push_str(".parquet");
    let tmp_basename = String::from(".tmp.") + &basename;
    let mut tmp_filename = new_filename.clone();
    new_filename.push(basename);
    tmp_filename.push(tmp_basename);
    if new_filename.exists() {
        eprintln!(
            "{} is already exists -- skipping",
            new_filename.to_str().unwrap()
        );
        return Ok(());
    }
    if tmp_filename.exists() {
        eprintln!(
            "Temporary filename {} is already exists -- skipping",
            tmp_filename.to_str().unwrap()
        );
        return Ok(());
    }
    let mut output = TempFile::create_new(tmp_filename.into_os_string().into_string().unwrap())?;
    if std::io::stdin().is_terminal() {
        println!("{}", filename.to_str().unwrap());
    }
    let schema_ref = Arc::new(schema);
    let reader = ReaderBuilder::new(schema_ref)
        .with_format(format)
        .build(reader.rewind()?)?;

    let writer_props = WriterProperties::builder()
        .set_compression(Compression::GZIP(GzipLevel::try_new(8).unwrap()));
    let mut writer =
        ArrowWriter::try_new(&mut output, reader.schema(), Some(writer_props.build()))?;
    for batch in reader {
        let batch = batch?;
        writer.write(&batch)?;
    }
    writer.close()?;
    output.flush_and_rename(new_filename)?;
    if args.rm {
        if let Err(err) = remove_file(filename) {
            eprintln!(
                "Can't remove original file {}: {err}",
                filename.to_str().unwrap()
            );
        }
    }
    Ok(())
}

fn main() -> Result<()> {
    let mut args: Args = Args::parse();
    let filenames = args.input;
    args.input = vec![];
    let (overrides, default_int_type, default_float_type) = consolidate_types(&mut args)?;
    for filename in &filenames {
        process(
            filename,
            &args,
            &overrides,
            &default_int_type,
            &default_float_type,
        )?;
    }
    Ok(())
}
