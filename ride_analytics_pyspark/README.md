# Ride Analytics PySpark Project

## Project Overview

This project implements an end-to-end data engineering pipeline for a ride-booking analytics use case using PySpark and Databricks.

The pipeline follows the Medallion Architecture:

**Source CSV → Bronze → Silver → Gold**

The project demonstrates batch-style file ingestion using Spark Structured Streaming with `trigger(once=True)`, data validation and quarantine, data cleansing and enrichment, Delta Lake upsert processing, and business-level KPI generation.

## Business Objective

The objective is to transform raw ride-booking data into clean, analytics-ready datasets and generate business KPIs for:

- Daily trip performance
- Driver performance
- Customer behavior
- Payment analysis

The pipeline is designed to handle new records and updates to existing records.

## Architecture

```text
                         Source CSV Files
                               |
                               v
                         Bronze Layer
                    Raw data + ingestion timestamp
                               |
                               v
                         Silver Layer
              Validation + Quarantine + Transformation
                    + Incremental Delta Upsert
                               |
                               v
                           Gold Layer
        -------------------------------------------------
        |                |               |              |
        v                v               v              v
 Daily Trip KPIs   Driver Performance  Customer     Payment
                                      Analysis      Analysis
```

## Technologies Used

- Python
- PySpark
- Databricks
- Delta Lake
- SQL
- Git

## Dataset

The project uses six source entities:

- `customers`
- `drivers`
- `locations`
- `payments`
- `trips`
- `vehicles`

The source data is stored as CSV files and ingested into Databricks.

## Project Structure

```text
ride_analytics_pyspark/
|
├── code/
│   ├── Bronze_ingestion.ipynb
│   ├── Silver_Transformations.ipynb
│   ├── Gold_Analytics.ipynb
│   └── utils/
│       ├── silver_custom_utils.py
│       ├── gold.py
│       └── logger.py
|
├── source data/
│   ├── customers.csv
│   ├── drivers.csv
│   ├── locations.csv
│   ├── payments.csv
│   ├── trips.csv
│   └── vehicles.csv
|
├── results/
│   ├── Bronze/
│   ├── Silver/
│   └── Gold/
|
├── README.md
└── TESTING.md
```

## Data Architecture

The project uses the following Databricks namespace:

```text
ride_analytics.bronze
ride_analytics.silver
ride_analytics.gold
```

### Bronze Tables

```text
ride_analytics.bronze.customers
ride_analytics.bronze.drivers
ride_analytics.bronze.locations
ride_analytics.bronze.payments
ride_analytics.bronze.trips
ride_analytics.bronze.vehicles
```

### Silver Tables

```text
ride_analytics.silver.customers
ride_analytics.silver.drivers
ride_analytics.silver.locations
ride_analytics.silver.payments
ride_analytics.silver.trips
ride_analytics.silver.vehicles
```

Rejected records are stored separately in Silver quarantine tables.

### Gold Tables

```text
ride_analytics.gold.daily_trip_kpis
ride_analytics.gold.driver_performance
ride_analytics.gold.customer_analysis
ride_analytics.gold.payment_analysis
```

## Bronze Layer

The Bronze notebook ingests the source CSV files into Delta tables.

Spark Structured Streaming is used with:

```text
trigger(once=True)
```

This processes the currently available files while retaining the streaming checkpoint mechanism.

An `ingestion_timestamp` is added to the incoming records.

Each source entity has its own checkpoint location.

## Silver Layer

The Silver layer validates, cleans, and enriches the Bronze data.

### Data Validation

Validation rules are applied before transformation.

Examples include:

- Required business keys must not be null
- Email format validation for customers
- Required phone number validation

Invalid records are separated into quarantine datasets and written to dedicated Silver quarantine tables.

### Data Cleaning and Transformation

The Silver layer includes transformations such as:

- Trimming string fields
- Standardizing email values
- Standardizing phone numbers
- Extracting email domains
- Deriving date/year/month fields
- Calculating customer tenure
- Calculating trip duration
- Calculating fare per kilometer
- Creating distance and fare categories
- Creating payment amount categories
- Standardizing payment fields
- Adding processing timestamps

### Deduplication and Incremental Upsert

The Silver processing uses business keys and `last_updated_timestamp` to determine the latest version of a record.

Conceptually:

```text
New business key
      |
      +----> Insert

Existing business key
      |
      +----> Newer timestamp → Update

Existing business key
      |
      +----> Older/equal timestamp → Do not overwrite
```

Delta Lake `MERGE` is used for the upsert behavior.

## Gold Layer

The Gold layer produces business-level analytics from the Silver datasets.

### 1. Daily Trip KPIs

Daily trip metrics are grouped by:

```text
trip_date
```

Metrics include:

- Total trips
- Completed trips
- Cancelled trips
- Ongoing trips
- Total revenue
- Average fare
- Total distance
- Average distance
- Average trip duration
- Average fare per kilometer

Revenue and trip-performance measures that represent completed journeys are calculated using **Completed** trips only.

The Daily Trip KPI table uses incremental processing by identifying changed trips, finding affected dates, recalculating all Silver trips for those dates, and merging the recalculated KPI rows into Gold.

### 2. Driver Performance

Driver performance is grouped by:

```text
driver_id
```

The Gold layer calculates:

- Total trips
- Completed trips
- Cancelled trips
- Ongoing trips
- Total revenue
- Average fare
- Total distance
- Average trip duration

Revenue, fare, distance, and duration metrics are calculated using completed trips only.

For incremental processing, changed trips are used to identify affected drivers. All current Silver trips for those drivers are then recalculated before merging the results into Gold.

### 3. Customer Analysis

Customer analytics are grouped by:

```text
customer_id
```

Metrics include:

- Total trips
- Completed trips
- Cancelled trips
- Ongoing trips
- Total customer spending
- Average fare
- Total distance

Total trips includes all current trip statuses.

Spending, average fare, and distance are calculated using completed trips only.

For incremental processing, changed trips identify affected customers. All current Silver trips for those customers are recalculated before the results are merged into Gold.

### 4. Payment Analysis

Payment analysis is grouped by:

```text
payment_method
payment_status
online_payment_status
```

Metrics include:

- Total transactions
- Total amount
- Average amount

#### Processing Strategy

Payment Analysis uses a **full-refresh / overwrite strategy**.

The reason is that `payment_method`, `payment_status`, and `online_payment_status` are themselves part of the aggregation key. If a payment changes from one status or method to another, the payment can move from one Gold aggregation group to another.

Recalculating the complete Silver payment dataset ensures that all Gold payment groups are rebuilt correctly and prevents stale aggregation groups.

## Incremental Processing Strategy

The project uses incremental processing where the business grain remains stable after a source record changes.

For example:

```text
Trip updated
     |
     v
Find affected trip_date
     |
     v
Read all current trips for that date
     |
     v
Recalculate daily KPI
     |
     v
MERGE into Gold
```

Similarly:

```text
Trip updated
     |
     +----> driver_id → recalculate affected driver
     |
     +----> customer_id → recalculate affected customer
```

This avoids recalculating the entire Gold dataset for every trip update.

## Business Logic

The project applies the following business rules:

- Cancelled trips are counted as cancelled but are not treated as completed revenue-generating trips.
- Ongoing trips are counted separately and are not treated as completed revenue-generating trips.
- Revenue is calculated from completed trips.
- Average fare is calculated from completed trips.
- Completed distance is used for total and average distance metrics.
- Completed trip duration is used for average trip duration.
- Fare-per-kilometer metrics are based on completed trips.
- Customer total trips includes Completed, Cancelled, and Ongoing trips.

## Testing and Validation

Testing was performed manually by modifying records in the source/Silver tables, running the relevant pipeline, and comparing the resulting Gold values with the expected results.

Detailed test scenarios are documented in [`TESTING.md`](TESTING.md).

## Key Design Decisions

### Medallion Architecture

Bronze, Silver, and Gold layers separate raw ingestion, data preparation, and business analytics.

### Delta Lake

Delta tables provide reliable storage and support `MERGE` operations for incremental upserts.

### Business-Grain Recalculation

For Daily Trip KPIs, Driver Performance, and Customer Analysis, only affected business groups are recalculated.

### Full Refresh for Payment Analysis

Payment Analysis is overwritten because mutable payment attributes are part of the aggregation key.

### Quarantine

Invalid records are separated from valid Silver data instead of being silently discarded.

## Limitations

- Testing is currently manual rather than automated.
- Payment Analysis uses full refresh rather than incremental aggregation.
- SCD Type 2 historical tracking is not implemented in this project.
- Production orchestration, monitoring, alerting, and CI/CD are not included.

## Future Enhancements

Potential future enhancements include:

- Automated data quality testing
- SCD Type 2 historical tracking
- Workflow orchestration
- Pipeline monitoring and alerting
- CI/CD integration
- Streaming use cases with Kafka
- More advanced business analytics

## How to Run

1. Upload/source the CSV files in the configured Databricks Volume path.
2. Run `Bronze_ingestion.ipynb`.
3. Run `Silver_Transformations.ipynb`.
4. Run `Gold_Analytics.ipynb`.
5. Validate the resulting Bronze, Silver, and Gold tables.
6. For update scenarios, modify a test record and rerun the appropriate pipeline to validate incremental processing.

## Author

This project was created as a hands-on Data Engineering portfolio project to demonstrate practical PySpark, Databricks, Delta Lake, incremental processing, data quality, and analytical KPI development.
