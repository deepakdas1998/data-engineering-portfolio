from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    count,
    sum,
    avg,
    when,
    round,
    max,
    current_timestamp
)
from delta.tables import DeltaTable
import logging


# Configure logger
logger = logging.getLogger("ride_analytics")
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler()

    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s"
    )

    handler.setFormatter(formatter)
    logger.addHandler(handler)



class GoldAnalytics:

    # Calculate daily trip KPIs
    def calculate_daily_trip_kpis(self, df: DataFrame):

        daily_kpis = (
            df
            .groupBy("trip_date")
            .agg(

                count("trip_id").alias("total_trips"),

                sum(
                    when(
                        col("trip_status") == "Completed",
                        1
                    ).otherwise(0)
                ).alias("completed_trips"),

                sum(
                    when(
                        col("trip_status") == "Cancelled",
                        1
                    ).otherwise(0)
                ).alias("cancelled_trips"),

                sum(
                    when(
                        col("trip_status") == "Ongoing",
                        1
                    ).otherwise(0)
                ).alias("ongoing_trips"),

                round(
                    sum(
                        when(
                            col("trip_status") == "Completed",
                            col("fare_amount")
                        ).otherwise(0)
                    ),
                    2
                ).alias("total_revenue"),

                round(
                    avg(
                        when(
                            col("trip_status") == "Completed",
                            col("fare_amount")
                        )
                    ),
                    2
                ).alias("average_fare"),

                round(
                    sum(
                        when(
                            col("trip_status") == "Completed",
                            col("distance_km")
                        ).otherwise(0)
                    ),
                    2
                ).alias("total_distance"),

                round(
                    avg(
                        when(
                            col("trip_status") == "Completed",
                            col("distance_km")
                        )
                    ),
                    2
                ).alias("average_distance"),

                round(
                    avg(
                        when(
                            col("trip_status") == "Completed",
                            col("trip_duration_minutes")
                        )
                    ),
                    2
                ).alias("average_trip_duration"),

                round(
                    avg(
                        when(
                            col("trip_status") == "Completed",
                            col("fare_per_km")
                        )
                    ),
                    2
                ).alias("average_fare_per_km"),
                
                max(
                    "last_updated_timestamp"
                ).alias("last_updated_timestamp")
            )
        
              # Add Gold processing timestamp
            .withColumn(
                    "process_timestamp",
                    current_timestamp()
                ) 
            )   
        logger.info("Daily KPIs calculated")
        return daily_kpis



    # Incremental daily KPI processing
    def incremental_daily_trip_kpis(
        self,
        df: DataFrame,
        gold_table: str
    ):

        spark = df.sparkSession

        # Get latest processed timestamp from Gold
        last_timestamp = (
            spark.read
            .table(gold_table)
            .agg(
                max("last_updated_timestamp")
                .alias("max_timestamp")
            )
            .collect()[0]["max_timestamp"]
        )

        # Find new or updated records
        changed_trips = df.filter(
            col("last_updated_timestamp") > last_timestamp
        )
        

        # Stop if there is nothing new
        if changed_trips.limit(1).count() == 0:

            logger.info("No new or updated records")

            return

        # Find affected dates
        affected_dates = (
            changed_trips
            .select("trip_date")
            .distinct()
        )

        # Get ALL records for affected dates
        affected_trips = (
            df
            .join(
                affected_dates,
                "trip_date",
                "inner"
            )
        )

        # Reuse the KPI calculation function
        updated_kpis = self.calculate_daily_trip_kpis(
            affected_trips
        )

        # Get Gold Delta table
        gold = DeltaTable.forName(
            spark,
            gold_table
        )

        # Merge updated KPIs into Gold
        (
            gold.alias("target")
            .merge(
                updated_kpis.alias("source"),
                "target.trip_date = source.trip_date"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

  

        logger.info("Incremental Gold processing completed")    



    
    def calculate_driver_performance(
        self,
        driver_trip_df: DataFrame
    ):

        # Create driver-level performance analytics
        driver_performance = (
            driver_trip_df

            # Group data at driver level
            .groupBy(
                col("t.driver_id").alias("driver_id"),
                col("d.full_name").alias("driver_name"),
                col("d.city").alias("city"),
                col("d.driver_rating").alias("driver_rating")
            )

            # Calculate driver-level KPIs
            .agg(

                # Total trips handled by driver
                count(
                    "t.trip_id"
                ).alias("total_trips"),

                # Completed trips
                sum(
                    when(
                        col("t.trip_status") == "Completed",
                        1
                    ).otherwise(0)
                ).alias("completed_trips"),

                # Cancelled trips
                sum(
                    when(
                        col("t.trip_status") == "Cancelled",
                        1
                    ).otherwise(0)
                ).alias("cancelled_trips"),

                # Ongoing trips
                sum(
                    when(
                        col("t.trip_status") == "Ongoing",
                        1
                    ).otherwise(0)
                ).alias("ongoing_trips"),

                # Total revenue generated by driver - Completed trips only
                round(
                    sum(
                        when(
                            col("t.trip_status") == "Completed",
                            col("t.fare_amount")
                        ).otherwise(0)
                    ),
                    2
                ).alias("total_revenue"),

                # Average fare - Completed trips only
                round(
                    avg(
                        when(
                            col("t.trip_status") == "Completed",
                            col("t.fare_amount")
                        )
                    ),
                    2
                ).alias("average_fare"),

                # Total distance travelled - Completed trips only
                round(
                    sum(
                        when(
                            col("t.trip_status") == "Completed",
                            col("t.distance_km")
                        ).otherwise(0)
                    ),
                    2
                ).alias("total_distance"),

                # Average trip duration - Completed trips only
                round(
                    avg(
                        when(
                            col("t.trip_status") == "Completed",
                            col("t.trip_duration_minutes")
                        )
                    ),
                    2
                ).alias("average_trip_duration"),
                # Latest Silver update timestamp
                # for this driver
                max(
                    col("t.last_updated_timestamp")
                ).alias(
                    "last_updated_timestamp"
                )
            )

            # Add Gold processing timestamp
            .withColumn(
                "process_timestamp",
                current_timestamp()
            )
        )

        # Return driver performance DataFrame
        return driver_performance


    def incremental_driver_performance(
        self,
        df_trips: DataFrame,
        df_drivers: DataFrame,
        gold_table: str
    ):

        # Get Spark session from the DataFrame
        spark = df_trips.sparkSession


        # Get the latest timestamp already processed in Gold
        last_timestamp = (
            spark.read
            .table(gold_table)
            .select(
                max("last_updated_timestamp")
                .alias("max_timestamp")
            )
            .collect()[0]["max_timestamp"]
        )


        # Find new or updated trips from Silver
        changed_trips = (
            df_trips
            .filter(
                col("last_updated_timestamp")
                > last_timestamp
            )
        )


        # Check whether there are any changed records
        if changed_trips.limit(1).count() == 0:

            logger.info(
                "No new or updated trips found"
            )

            return


        # Find the drivers affected by changed trips
        affected_drivers = (
            changed_trips
            .select("driver_id")
            .distinct()
        )


        # Get ALL Silver trips for affected drivers
        #
        # We don't calculate only the changed trips.
        # We recalculate the complete current performance
        # for the affected drivers.
        affected_trips = (
            df_trips
            .join(
                affected_drivers,
                on="driver_id",
                how="inner"
            )
        )


        # Join affected trips with driver information
        affected_driver_trip_df = (
            affected_trips.alias("t")
            .join(
                df_drivers.alias("d"),
                col("t.driver_id")
                == col("d.driver_id"),
                "left"
            )
        )


        # Reuse the existing KPI calculation function
        #
        # This avoids repeating all the aggregation code.
        updated_driver_performance = (
            self.calculate_driver_performance(
                affected_driver_trip_df
            )
        )


        # Get the Gold Delta table
        gold = DeltaTable.forName(
            spark,
            gold_table
        )


        # MERGE recalculated drivers into Gold
        (
            gold.alias("target")

            .merge(
                updated_driver_performance.alias("source"),

                # Driver ID is the business key
                "target.driver_id = source.driver_id"
            )

            # Existing driver → update the Gold record
            .whenMatchedUpdateAll()

            # New driver → insert new Gold record
            .whenNotMatchedInsertAll()

            .execute()
        )


        # Log completion message
        logger.info(
            "Incremental Driver Performance completed"
        )



    def calculate_customer_analysis(
        self,
        customer_trip_df: DataFrame
    ):

        # Create customer-level analytics
        customer_analysis = (
            customer_trip_df

            # Group data at customer level
            .groupBy(
                col("c.customer_id").alias("customer_id"),
                col("c.full_name").alias("customer_name"),
                col("c.city").alias("city"),
                col("c.signup_date").alias("signup_date"),
                col("c.customer_tenure_years").alias(
                    "customer_tenure_years"
                )
            )

            # Calculate customer-level KPIs
            .agg(

                # Total trips taken by customer — includes Completed, Cancelled and Ongoing trips
                count(
                    "t.trip_id"
                ).alias("total_trips"),

                # Completed trips
                sum(
                    when(
                        col("t.trip_status") == "Completed",
                        1
                    ).otherwise(0)
                ).alias("completed_trips"),

                # Cancelled trips
                sum(
                    when(
                        col("t.trip_status") == "Cancelled",
                        1
                    ).otherwise(0)
                ).alias("cancelled_trips"),

                # Ongoing trips
                sum(
                    when(
                        col("t.trip_status") == "Ongoing",
                        1
                    ).otherwise(0)
                ).alias("ongoing_trips"),

                # Total customer spending — Completed trips only
                round(
                    sum(
                        when(
                            col("t.trip_status") == "Completed",
                            col("t.fare_amount")
                        ).otherwise(0)
                    ),
                    2
                ).alias("total_spend"),

                # Average fare paid — Completed trips only
                round(
                    avg(
                        when(
                            col("t.trip_status") == "Completed",
                            col("t.fare_amount")
                        )
                    ),
                    2
                ).alias("average_fare"),

                # Total distance travelled — Completed trips only
                round(
                    sum(
                        when(
                            col("t.trip_status") == "Completed",
                            col("t.distance_km")
                        ).otherwise(0)
                    ),
                    2
                ).alias("total_distance"),

                # Latest Silver update timestamp
                max(
                    col("t.last_updated_timestamp")
                ).alias("last_updated_timestamp")
            )

            # Add Gold processing timestamp
            .withColumn(
                "process_timestamp",
                current_timestamp()
            )
        )

        # Return customer-level analytics
        return customer_analysis


    def incremental_customer_analysis(
        self,
        df_trips: DataFrame,
        df_customers: DataFrame,
        gold_table: str
    ):

        # Get Spark session
        spark = df_trips.sparkSession


        # Get the latest timestamp already processed in Gold
        last_timestamp = (
            spark.read
            .table(gold_table)
            .select(
                max("last_updated_timestamp")
                .alias("max_timestamp")
            )
            .collect()[0]["max_timestamp"]
        )


        # Find new or updated trips from Silver
        changed_trips = (
            df_trips
            .filter(
                col("last_updated_timestamp")
                > last_timestamp
            )
        )


        # Stop if there are no changes
        if changed_trips.limit(1).count() == 0:

            logger.info(
                "No new or updated trips found"
            )

            return


        # Find customers affected by changed trips
        affected_customers = (
            changed_trips
            .select("customer_id")
            .distinct()
        )


        # Get ALL trips belonging to affected customers
        #
        # We recalculate the complete customer metrics,
        # not just the changed trips.
        affected_trips = (
            df_trips
            .join(
                affected_customers,
                on="customer_id",
                how="inner"
            )
        )


        # Join affected trips with customer information
        affected_customer_trip_df = (
            affected_trips.alias("t")
            .join(
                df_customers.alias("c"),
                col("t.customer_id")
                == col("c.customer_id"),
                "left"
            )
        )


        # Reuse the existing KPI calculation
        updated_customer_analysis = (
            self.calculate_customer_analysis(
                affected_customer_trip_df
            )
        )


        # Get Gold Delta table
        gold = DeltaTable.forName(
            spark,
            gold_table
        )


        # MERGE recalculated customers into Gold
        (
            gold.alias("target")
            .merge(
                updated_customer_analysis.alias("source"),

                # Customer ID is the business key
                "target.customer_id = source.customer_id"
            )

            # Existing customer → update metrics
            .whenMatchedUpdateAll()

            # New customer → insert record
            .whenNotMatchedInsertAll()

            .execute()
        )


        # Completion message
        logger.info(
            "Incremental Customer Analysis completed"
        )




    def calculate_payment_analysis(
        self,
        df_payments: DataFrame
    ):

        # Create payment-level analytics
        payment_analysis = (
            df_payments

            # Group payments by payment dimensions
            .groupBy(
                "payment_method",
                "payment_status",
                "online_payment_status"
            )

            # Calculate payment-level KPIs
            .agg(

                # Number of transactions
                count(
                    "payment_id"
                ).alias("total_transactions"),

                # Total transaction amount
                round(
                    sum("amount"),
                    2
                ).alias("total_amount"),

                # Average transaction amount
                round(
                    avg("amount"),
                    2
                ).alias("average_amount"),

                # Latest Silver payment update
                max(
                    col("last_updated_timestamp")
                ).alias(
                    "last_updated_timestamp"
                )
            )

            # Add Gold processing timestamp
            .withColumn(
                "process_timestamp",
                current_timestamp()
            )
        )

        # Return payment analytics
        return payment_analysis


