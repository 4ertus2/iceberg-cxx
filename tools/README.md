## syncice usage

### Env variables

```
AWS_ENDPOINT_URL=<...>
AWS_DEFAULT_REGION=<...>
AWS_ACCESS_KEY_ID=<...>
AWS_SECRET_ACCESS_KEY=<...>

DST_ENDPOINT_URL=$AWS_ENDPOINT_URL
DST_DEFAULT_REGION=$AWS_DEFAULT_REGION
DST_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
DST_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
```

### Scenario: Copy table as is with dst fixation in remote HMS

```
> syncice --src_host $HMS_HOST --src_db src_db --src_table src_table --dst_host=$HMS_HOST --allow_list='' \
    --dst_db dst_db --dst_table dst_table --dst_path s3://bucket --mode actual --update_hms \
    --chunk_size_mb 16 --compute_threads 8
```

### Scenario: Fix metadata in table copy setting it to actual location

```
> syncice --src_host $HMS_HOST --src_db delta --src_table table --mode meta --allow_list '' --force
```

### Scenario: Copy table with sorting

```
> syncice --src_db delta --src_table table --fix_items 5 --dst_path s3://bucket/table \
    --mode actual --allow_list '' --sort_order_columns "col1,col2,col3"
```

### Scenario: Copy removing columns

```
> syncice --src_db ice --src_table table --dst_path s3://bucket/table --deny_columns_list deny_columns.txt \
    --raw_parquet_rw --compute_threads 16 --io_threads 16
```

deny_columns.txt file format
```
ice.table_1 business_dt
ice.table_1 price_amt_rub
```

### Scenario: Calculate checksums (xxh128)

```
> syncice --src_host $HMS_HOST --src_db src_db --src_table src_table --allow_list='' \
    --mode crc --crc_file ./src_table.xxh128 --compute_threads 40 --io_threads 20
```

```
> syncice --src_host $HMS_HOST --src_db src_db --src_table src_table --allow_list='' \
    --mode checkcrc --crc_file ./src_table.xxh128 --compute_threads 40 --io_threads 20
```

Checksums format is equal to `xxhsum -H2 <file_pattern>`

```
4a02bc18034f7f68cea4a26a45f4e1dd  iceberg/data/x/00002-2168662-bac1dc45-a36a-4b39-bded-fbc2b967c82e-00001.parquet
cabf31aba684acfe78312179ec27bc3e  iceberg/data/x/00003-663949-3a88424e-84c6-494f-b58b-d47d0db2e8c3-00001.parquet
9c8ae580e213ed541965db73ec1b05c8  col1_dttm_month=2020-04/x.parquet
7cb886b58cfc9caf1ce9b7557dd714ae  col1_dttm_month=2020-04/y.parquet
69de63d811b8dac385b3810ef46dc034  col1_dttm_month=2020-04/z.parquet
b13990c764aaea6b790c5f462afac371  col1_dttm_month=2020-04/a.parquet
```
