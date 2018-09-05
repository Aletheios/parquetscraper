# Parquet Scraper

**Simple command line tool to read `.parquet` files**

## Installation

Install a current version of [Node.js](https://nodejs.org). Then run...

```bash
npm install -g parquetscraper
```

## Usage

**`parquetscraper [command] [filename] [options]`**

### Available commands
* `read`: Read file contents.
* `schema`: Get parquet schema.
* `rows`: Get number of contained rows.

Results are printed to the console by default if the `--export` option isn't used.

### Available options
* `--export`: Write extracted JSON to a file (only `read` and `schema` commands).
* `--from=[index]`: Start reading rows at the given index (only `read` command).
* `--to=[index]`: Read rows until the given index (only `read` command).

### Examples
```bash
# Read a parquet file and export its contents to a JSON file.
parquetscraper read path/to/file.parquet --export

# Read a parquet file and print a subset of the contents to the console.
parquetscraper read path/to/file.parquet --from=42 --to=84

# Get the schema of a parquet file and write it to a JSON file.
parquetscraper schema path/to/file.parquet --export

# Get the number of rows contained in the parquet file and print it to the console.
parquetscraper rows path/to/file.parquet
```