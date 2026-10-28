import dlt

from pyspark.sql.functions import (
    count,
    countDistinct,
    sum,
    avg,
    max
)


@dlt.view(
    name="daily_streaming_summary_stg"
)
def daily_streaming_summary_stg():

    stream_df = spark.read.table("factstream")

    track_df = (
        spark.read.table("dimtrack")
        .filter("__END_AT IS NULL")
    )

    date_df = (
        spark.read.table("dimdate")
        .filter("__END_AT IS NULL")
    )

    return (
        stream_df
        .join(
            track_df,
            stream_df.track_id == track_df.track_id,
            "left"
        )
        .join(
            date_df,
            stream_df.date_key == date_df.date_key,
            "left"
        )
        .groupBy(
            date_df.date
        )
        .agg(
            count(stream_df.stream_id).alias("total_streams"),
            countDistinct(stream_df.user_id).alias("unique_users"),
            countDistinct(track_df.artist_id).alias("unique_artists"),
            sum(stream_df.listen_duration).alias("total_listen_duration"),
            avg(stream_df.listen_duration).alias("avg_listen_duration"),
            max(stream_df.stream_timestamp).alias("latest_stream_timestamp")
        )
    )


@dlt.table(
    name="daily_streaming_summary"
)
def daily_streaming_summary():

    return spark.read.table(
        "daily_streaming_summary_stg"
    )