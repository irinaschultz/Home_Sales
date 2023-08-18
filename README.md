This project encompassed the utilization of Big Data and SparkSQL to analyze and query home sales data. The data analysis process encompassed the following steps:
    Package Importation: The project initiation involved the importation of essential packages, including findspark for Spark initialization and pyspark.sql for SparkSQL operations.
    SparkSession Creation: A SparkSession was established using SparkSession.builder.appName("SparkSQL").getOrCreate() to establish a connection with Spark.
    Data Reading: Home sales data was extracted from an AWS S3 bucket and loaded into a DataFrame using the provided URL.
    Temporary View Creation: A temporary view named "home_sales" was generated for the DataFrame using createOrReplaceTempView() method, enabling SQL queries to be executed on the DataFrame.
    Data Querying: Various SQL queries were executed to analyze home sales data. This encompassed computations such as the average price of a four-bedroom house sold annually and the average price of homes based on different attributes like bedrooms, bathrooms, and square footage.
    Data Caching: The temporary table "df" was cached using spark.catalog.cacheTable() to enhance query performance by storing data in memory.
    Query Runtime Comparison: The execution time of a specific query was compared between the cached version and the version using Parquet data. Timing data was captured using time.time() prior to query execution, facilitating runtime measurement.
    Parquet Data Writing: Processed home sales data was written in Parquet format with partitioning based on field using the partitionBy().parquet() method.
    Parquet Data Reading: Parquet data was loaded into a DataFrame for further analysis.
    Temporary Parquet Table Creation: A temporary table was established for the Parquet DataFrame using createOrReplaceTempView() method.
    Parquet Data Querying: SQL queries were conducted on the Parquet DataFrame to filter view ratings with an average price greater than or equal to $350,000. Query runtime was measured and compared to the cached version.

The following inquiries were addressed through the application of SparkSQL:
    Calculation of the average price for a four-bedroom house sold annually (rounded to two decimal places).
    Determination of the average price of homes for each year of construction, possessing three bedrooms and three bathrooms (rounded to two decimal places).
    Computation of the average price of homes for each year of construction with three bedrooms, three bathrooms, two floors, and an area greater than or equal to 2,000 square feet (rounded to two decimal places).
    Evaluation of the "view" rating for homes priced at or above $350,000, including the determination of query runtime (rounded to two decimal places).# Home_Sales