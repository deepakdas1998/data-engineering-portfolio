import dlt

expectations = {
  "rule_1" : "stream_id is NOT NULL"
}

# Create staging table with data quality validation
@dlt.table
@dlt.expect_all_or_drop(expectations)
def factstream_stg():
    df = spark.readStream.table("spotify_cata.silver.factstream")
    return df

dlt.create_streaming_table(
  name = "factstream",
  expect_all_or_drop = expectations            
)

# Apply Auto CDC with SCD Type 1 to maintain historical records
dlt.create_auto_cdc_flow(
  target = "factstream",
  source = "factstream_stg",
  keys = ["stream_id"],
  sequence_by = "stream_timestamp",
  stored_as_scd_type = "1", 
  track_history_column_list = None,
  track_history_except_column_list = None,
  name = None,
  once = False
)