import dlt

from pyspark.sql.functions import (
    count,
    countDistinct,
    sum,
    avg,
    max
)


@dlt.view(
    name="artist_performance_stg"
)
def artist_performance_stg():

    stream_df = spark.read.table("factstream")

    track_df = (
        spark.read.table("dimtrack")
        .filter("__END_AT IS NULL")
    )

    artist_df = (
        spark.read.table("dimartist")
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
            artist_df,
            track_df.artist_id == artist_df.artist_id,
            "left"
        )
        .groupBy(
            artist_df.artist_id,
            artist_df.artist_name,
            artist_df.genre,
            artist_df.country
        )
        .agg(
            count(stream_df.stream_id).alias("total_streams"),
            countDistinct(stream_df.user_id).alias("unique_listeners"),
            sum(stream_df.listen_duration).alias("total_listen_duration"),
            avg(stream_df.listen_duration).alias("avg_listen_duration"),
            max(stream_df.stream_timestamp).alias("latest_stream_timestamp")
        )
    )


@dlt.table(
    name="artist_performance"
)
def artist_performance():

    return spark.read.table(
        "artist_performance_stg"
    )