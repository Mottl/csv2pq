# csv2pq — CSV to Apache Parquet converter

## Installation
Install Rust with Cargo and then:

```sh
cargo install csv2pq
```

## Usage examples
```sh
csv2pq somedata.csv.gz
```
produces `somedata.parquet`.

```sh
csv2pq --rm somedata.csv
```
produces `somedata.parquet` and removes the original csv file.

```sh
csv2pq --f64='*' --f32=col1,col2 --i32='*' --i64=col10 --i64=col11
```
sets default float and integer data types to `Float64` and `Int32`. Sets
`col1` and `col2` to `Float32`, `col10` and `col11` to `Int64`.

## Parquet and Arrow underlying implementation
This project is just a CLI for [Apache Arrow implementation in Rust](https://github.com/apache/arrow-rs).

## Other converters
If this utility doesn't fit your needs, try [arrow-tools](https://github.com/domoritz/arrow-tools).

