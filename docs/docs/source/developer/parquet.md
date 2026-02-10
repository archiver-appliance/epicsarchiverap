# Parquet

## Implementation Details

The Parquet backend is implemented as a
[PlainFileHandler](../_static/javadoc/edu/stanford/slac/archiverappliance/plain/PlainFileHandler.html){.external}
within the
[PlainStoragePlugin](../_static/javadoc/edu/stanford/slac/archiverappliance/plain/PlainStoragePlugin.html){.external}. It uses the same partitioning logic as the PB backend (e.g., hourly, daily, or yearly partitions).

### Schema

The schema used in Parquet files is derived from the Protocol Buffers definitions in `EPICSEvent.proto`. This ensures consistency between the PB and Parquet backends.

## Configuration and Management

For details on how to configure the Parquet backend, including compression settings and data conversion, please see the [Storage Plugins](../../sysadmin/storage_plugins#apache-parquet-backend) page in the Sysadmin guide.

## Tools for Analyzing Parquet Files

One of the main advantages of the Parquet backend is the ability to use standard industry tools to analyze the data.

### Apache Parquet CLI

The [parquet-cli](https://github.com/apache/parquet-java/tree/master/parquet-cli) (or `parquet-tools`) is a command-line utility for inspecting Parquet files. It allows you to view the schema, metadata, and the actual data stored in the files.

#### Viewing Metadata

The `meta` command displays detailed information about row groups, column statistics (min/max values), and compression.

```text
$ parquet meta your_file.parquet

File path:  your_file.parquet
Created by: parquet-mr version 1.17.0
Properties:
           parquet.proto.descriptor: name: "VectorDouble"
...
Schema:
message EPICS.VectorDouble {
  required int32 secondsintoyear = 1;
  required int32 nano = 2;
  repeated double val = 3;
  optional int32 severity = 4;
  optional int32 status = 5;
  optional int32 repeatcount = 6;
  repeated group fieldvalues = 7 {
    required binary name (STRING) = 1;
    required binary val (STRING) = 2;
  }
  optional boolean fieldactualchange = 8;
}

Row group 0:  count: 2667650  25,09 B records  start: 4  total(compressed): 63,833 MB total(uncompressed):1,694 GB
--------------------------------------------------------------------------------
                   type      encodings count     avg size   nulls   min / max
secondsintoyear    INT32     Z   _     2667650   2,68 B     0       "10368000" / "13046399"
nano               INT32     Z   _     2667650   3,33 B     0       "30013" / "999946202"
val                DOUBLE    Z _ R_ F  266765000 0,19 B     0       "-0.0" / "16.818929577945916"
severity           INT32     Z   _     2667650   0,00 B     2667650
status             INT32     Z   _     2667650   0,00 B     2667650
repeatcount        INT32     Z   _     2667650   0,00 B     2667650
fieldvalues.name   BINARY    Z   _     2667692   0,00 B     2667609 "DESC" / "startup"
fieldvalues.val    BINARY    Z   _     2667692   0,00 B     2667609 "" / "true"
fieldactualchange  BOOLEAN   Z   _     2667650   0,00 B     2667613 "false" / "true"
```

#### Viewing the Schema

The `schema` command shows the logical schema of the file.

```bash
parquet schema your_file.parquet
```

Example output:

```json
{
  "type": "record",
  "name": "VectorDouble",
  "namespace": "EPICS",
  "fields": [
    {
      "name": "secondsintoyear",
      "type": "int"
    },
    {
      "name": "nano",
      "type": "int"
    },
    {
      "name": "val",
      "type": {
        "type": "array",
        "items": "double"
      },
      "default": []
    },
    {
      "name": "severity",
      "type": ["null", "int"],
      "default": null
    },
    {
      "name": "status",
      "type": ["null", "int"],
      "default": null
    },
    {
      "name": "repeatcount",
      "type": ["null", "int"],
      "default": null
    },
    {
      "name": "fieldvalues",
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "fieldvalues",
          "namespace": "",
          "fields": [
            {
              "name": "name",
              "type": "string"
            },
            {
              "name": "val",
              "type": "string"
            }
          ]
        }
      },
      "default": []
    },
    {
      "name": "fieldactualchange",
      "type": ["null", "boolean"],
      "default": null
    }
  ]
}
```

#### Viewing Data

The `head` command displays the first few records in JSON format.

```bash
parquet head -n 2 your_file.parquet
```

Example output:

```text
secondsintoyear: 123456
nano: 1000000
val: 10.5
secondsintoyear: 123457
nano: 2000000
val: 10.6
```

### Other Tools

Because Parquet is a standard format, you can also use tools like:

- **Python**: Using [pandas](https://pandas.pydata.org/) with [pyarrow](https://arrow.apache.org/docs/python/) or [fastparquet](https://fastparquet.readthedocs.io/).
- **DuckDB**: Directly querying Parquet files using SQL with [DuckDB](https://duckdb.org/).
- **Polars**: Fast multi-threaded DataFrame library for Rust and Python [Polars](https://pola.rs/).
- **Apache Arrow**: For high-performance in-memory processing with [Apache Arrow](https://arrow.apache.org/).
