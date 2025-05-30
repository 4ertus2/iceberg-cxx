#!/bin/bash

TEST_DATA_DIR=${TEST_DATA_DIR:-$HOME/test}
TOOLS_PATH=${TOOLS_PATH:-$HOME/bin}
MINIO_DATA_DIR=${MINIO_DATA_DIR:-$HOME/myminio_data}
MINIO=${MINIO_EXECUTABLE:-minio}
MC=${MC_EXECUTABLE:-mc}
HMS_CLIENT=$TOOLS_PATH/hive_metastore_client
HOST=127.0.0.1
HMS_PORT=9090
S3_PORT=9000
DB_NAME=miniodb

export AWS_ENDPOINT_URL="http://$HOST:$S3_PORT"
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_DEFAULT_REGION=""

export DST_ENDPOINT_URL=$AWS_ENDPOINT_URL
export DST_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
export DST_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
export DST_DEFAULT_REGION=$AWS_DEFAULT_REGION

#export MINIO_ROOT_USER=$AWS_ACCESS_KEY_ID
#export MINIO_ROOT_PASSWORD=$AWS_SECRET_ACCESS_KEY

ALLOW_LIST=$HOME/allow.txt
DENY_COLUMN_LIST=$HOME/deny_columns.txt

if [ "x$NO_DAEMONS" = "x" ] ; then
    $TOOLS_PATH/hive_metastore_server &

    mkdir $MINIO_DATA_DIR
    $MINIO server $MINIO_DATA_DIR &
fi

sleep 4

echo $AWS_ENDPOINT_URL $AWS_ACCESS_KEY_ID $AWS_SECRET_ACCESS_KEY
$MC alias set 'myminio' $AWS_ENDPOINT_URL $AWS_ACCESS_KEY_ID $AWS_SECRET_ACCESS_KEY
$MC mb myminio/warehouse
$MC mirror $TEST_DATA_DIR myminio/warehouse/test
$MC ls myminio/warehouse
$HMS_CLIENT create-table $HOST $HMS_PORT $DB_NAME snap00 s3://warehouse/test/metadata/00000-800cc6aa-5051-47d5-9579-46aafcba1de6.metadata.json
$HMS_CLIENT create-table $HOST $HMS_PORT $DB_NAME snap01 s3://warehouse/test/metadata/00001-6d216ef0-8d58-4f27-a1d9-1cb22c1f3415.metadata.json
$HMS_CLIENT create-table $HOST $HMS_PORT $DB_NAME snap02 s3://warehouse/test/metadata/00002-37c508a5-8a06-4823-845e-889dff066f72.metadata.json
$HMS_CLIENT create-table $HOST $HMS_PORT $DB_NAME snap03 s3://warehouse/test/metadata/00003-ca406d8e-6c7b-4672-87ff-bfd76f84f949.metadata.json
$HMS_CLIENT create-table $HOST $HMS_PORT $DB_NAME snap07 s3://warehouse/test/metadata/00007-2ed28880-9928-4670-934a-bda11c8130c9.metadata.json

echo $DB_NAME.snap02 > $ALLOW_LIST
echo $DB_NAME.snap03 >> $ALLOW_LIST
echo $DB_NAME.snap07 >> $ALLOW_LIST
echo "$DB_NAME.snap03 a" > $DENY_COLUMN_LIST

# 0. Fix location for actual one
echo "[syncice fixlocation]" snap02
$TOOLS_PATH/syncice --mode fixlocation --update_hms --allow_list=$ALLOW_LIST \
    --src_host $HOST --src_port $HMS_PORT --src_db $DB_NAME --src_table snap02

echo "[syncice fixlocation]" snap03, snap07
$TOOLS_PATH/syncice --mode fixlocation --update_hms --allow_list=$ALLOW_LIST \
    --src_host $HOST --src_port $HMS_PORT --src_db $DB_NAME --src_table snap03
$TOOLS_PATH/syncice --mode fixlocation --update_hms --allow_list=$ALLOW_LIST \
    --src_host $HOST --src_port $HMS_PORT --src_db $DB_NAME --src_table snap07

$MC ls -r myminio/warehouse/test
$HMS_CLIENT get-table $HOST $HMS_PORT $DB_NAME snap03

# 1. Copy fixed metadata and data mentioned in metadata + register it in HMS

SRC_TABLE=snap02
DST_TABLE=copy
echo "[syncice copy actual]" $SRC_TABLE $DST_TABLE

$TOOLS_PATH/syncice --mode actual --update_hms --allow_list=$ALLOW_LIST --overwrite_files --verbose \
    --src_host $HOST --src_port $HMS_PORT --src_db $DB_NAME --src_table $SRC_TABLE \
    --dst_host $HOST --dst_port $HMS_PORT --dst_db $DB_NAME --dst_table $DST_TABLE \
    --dst_path s3://warehouse/$DST_TABLE

$MC ls -r myminio/warehouse/$DST_TABLE
$HMS_CLIENT get-tables $HOST $HMS_PORT $DB_NAME
$HMS_CLIENT get-table $HOST $HMS_PORT $DB_NAME $DST_TABLE

#SRC_TABLE=snap03
#DST_TABLE=copy
#echo "[syncice copy actual]" $SRC_TABLE $DST_TABLE

#$TOOLS_PATH/syncice --mode actual --update_hms --allow_list=$ALLOW_LIST --force \
#    --src_host $HOST --src_port $HMS_PORT --src_db $DB_NAME --src_table $SRC_TABLE \
#    --dst_host $HOST --dst_port $HMS_PORT --dst_db $DB_NAME --dst_table $DST_TABLE \
#    --dst_path s3://warehouse/$DST_TABLE

$MC ls -r myminio/warehouse/$DST_TABLE
$HMS_CLIENT get-tables $HOST $HMS_PORT $DB_NAME
OUT=`$HMS_CLIENT get-table $HOST $HMS_PORT $DB_NAME $DST_TABLE`
echo $OUT
echo

EXPECTED="Table(tableName=copy, dbName=miniodb, owner=root, createTime=0, lastAccessTime=0, retention=0, \
sd=StorageDescriptor(cols=[], location=s3://warehouse/copy, inputFormat=, outputFormat=, compressed=0, \
numBuckets=0, serdeInfo=SerDeInfo(name=, serializationLib=, parameters={}), bucketCols=[], sortCols=[], \
parameters={}, skewedInfo=<null>, storedAsSubDirectories=<null>), partitionKeys=[], \
parameters={EXTERNAL: TRUE, \
metadata_location: s3://warehouse/copy/metadata/00002-37c508a5-8a06-4823-845e-889dff066f72.metadata.json, \
previous_metadata_location: s3://warehouse/test/metadata/00002-37c508a5-8a06-4823-845e-889dff066f72.metadata.json, \
table_type: ICEBERG, write.format.default: PARQUET}, \
viewOriginalText=, viewExpandedText=, tableType=EXTERNAL_TABLE, privileges=<null>, temporary=0, rewriteEnabled=<null>)"

if [ "$OUT" != "$EXPECTED" ] ; then
    diff -u <(echo "$EXPECTED") <(echo "$OUT")
    exit 1;
fi

# 2. Copy snapshot via tag

SRC_TABLE=snap07
DST_TABLE=tag
echo "[syncice copy tag MT]" $SRC_TABLE $DST_TABLE

# remove old snapshot file (test ignore_missign_snapshots)
$MC rm myminio/warehouse/test/metadata/snap-1638951453256129678-1-eea762e4-1b7a-4717-b361-eae34da54fd4.avro

$TOOLS_PATH/syncice --mode actual --update_hms --allow_list=$ALLOW_LIST --overwrite_files --ref 'EOW-01' \
    --verbose --compute_threads 4 \
    --src_host $HOST --src_port $HMS_PORT --src_db $DB_NAME --src_table $SRC_TABLE \
    --dst_host $HOST --dst_port $HMS_PORT --dst_db $DB_NAME --dst_table $DST_TABLE \
    --dst_path s3://warehouse/$DST_TABLE

$MC ls -r myminio/warehouse/$DST_TABLE
$HMS_CLIENT get-tables $HOST $HMS_PORT $DB_NAME
$HMS_CLIENT get-table $HOST $HMS_PORT $DB_NAME $DST_TABLE

$MC ls -r myminio/warehouse/$DST_TABLE
$HMS_CLIENT get-tables $HOST $HMS_PORT $DB_NAME
OUT=`$HMS_CLIENT get-table $HOST $HMS_PORT $DB_NAME $DST_TABLE`
echo $OUT
echo

EXPECTED="Table(tableName=tag, dbName=miniodb, owner=root, createTime=0, lastAccessTime=0, retention=0, \
sd=StorageDescriptor(cols=[], location=s3://warehouse/tag, inputFormat=, outputFormat=, compressed=0, \
numBuckets=0, serdeInfo=SerDeInfo(name=, serializationLib=, parameters={}), bucketCols=[], sortCols=[], \
parameters={}, skewedInfo=<null>, storedAsSubDirectories=<null>), partitionKeys=[], \
parameters={EXTERNAL: TRUE, \
metadata_location: s3://warehouse/tag/metadata/00007-2ed28880-9928-4670-934a-bda11c8130c9.metadata.json, \
previous_metadata_location: s3://warehouse/test/metadata/00007-2ed28880-9928-4670-934a-bda11c8130c9.metadata.json, \
table_type: ICEBERG, write.format.default: PARQUET}, \
viewOriginalText=, viewExpandedText=, tableType=EXTERNAL_TABLE, privileges=<null>, temporary=0, rewriteEnabled=<null>)"

ls -R /tmp/syncice/$SRC_TABLE
cat /tmp/syncice/$SRC_TABLE/metadata/00007-2ed28880-9928-4670-934a-bda11c8130c9.metadata.json

if [ "$OUT" != "$EXPECTED" ] ; then
    diff -u <(echo "$EXPECTED") <(echo "$OUT")
    exit 1;
fi

# 3. Copy with projection (by deny_columns_list)

SRC_TABLE=snap03
DST_TABLE=projection
MAX_THREADS=1
echo "[syncice projection]" $SRC_TABLE $DST_TABLE

#gdb -batch -ex "run" -ex "bt" --args \
$TOOLS_PATH/syncice --mode actual --update_hms --allow_list=$ALLOW_LIST --deny_columns_list=$DENY_COLUMN_LIST \
    --compute_threads $MAX_THREADS --overwrite_files --verbose \
    --src_host $HOST --src_port $HMS_PORT --src_db $DB_NAME --src_table $SRC_TABLE \
    --dst_host $HOST --dst_port $HMS_PORT --dst_db $DB_NAME --dst_table $DST_TABLE \
    --dst_path s3://warehouse/$DST_TABLE

$MC ls -r myminio/warehouse/$DST_TABLE
$HMS_CLIENT get-tables $HOST $HMS_PORT $DB_NAME
$HMS_CLIENT get-table $HOST $HMS_PORT $DB_NAME $DST_TABLE

cat /tmp/syncice/snap03/metadata/00003-ca406d8e-6c7b-4672-87ff-bfd76f84f949.metadata.json

$MC ls -r myminio/warehouse/$DST_TABLE
$HMS_CLIENT get-tables $HOST $HMS_PORT $DB_NAME
OUT=`$HMS_CLIENT get-table $HOST $HMS_PORT $DB_NAME $DST_TABLE`
echo $OUT
echo

EXPECTED="Table(tableName=projection, dbName=miniodb, owner=root, createTime=0, lastAccessTime=0, retention=0, \
sd=StorageDescriptor(cols=[], location=s3://warehouse/projection, inputFormat=, outputFormat=, compressed=0, \
numBuckets=0, serdeInfo=SerDeInfo(name=, serializationLib=, parameters={}), bucketCols=[], sortCols=[], \
parameters={}, skewedInfo=<null>, storedAsSubDirectories=<null>), partitionKeys=[], \
parameters={EXTERNAL: TRUE, \
metadata_location: s3://warehouse/projection/metadata/00003-ca406d8e-6c7b-4672-87ff-bfd76f84f949.metadata.json, \
previous_metadata_location: s3://warehouse/test/metadata/00003-ca406d8e-6c7b-4672-87ff-bfd76f84f949.metadata.json, \
table_type: ICEBERG, write.format.default: PARQUET}, \
viewOriginalText=, viewExpandedText=, tableType=EXTERNAL_TABLE, privileges=<null>, temporary=0, rewriteEnabled=<null>)"

if [ "$OUT" != "$EXPECTED" ] ; then
    diff -u <(echo "$EXPECTED") <(echo "$OUT")
    exit 1;
fi
