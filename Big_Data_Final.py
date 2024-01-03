# Setup
from pyspark.sql import SparkSession

# Create a Spark session with the resources necessary to handle our datasize consistently
spark = SparkSession.builder.appName("ChicagoTaxi").config("spark.executor.memory", "4g").config("spark.executor.cores", "2").getOrCreate()

# Configure the session to allow our Python code (specifically, the Google API) to run

%%configure -f
{ "conf":{
          "spark.pyspark.python": "python3",
          "spark.pyspark.virtualenv.enabled": "true",
          "spark.pyspark.virtualenv.type":"native",
          "spark.pyspark.virtualenv.bin.path":"/usr/bin/virtualenv",
          "spark.jars.packages": "com.amazon.deequ:deequ:1.0.3",
          "spark.jars.excludes": "net.sourceforge.f2j:arpack_combined_all"
         }
}

# install the necessary Python packages for our code

sc.install_pypi_package("requests==2.30.0")
sc.install_pypi_package("pyOpenSSL==20.0.1")
sc.install_pypi_package("urllib3==1.26.15")
sc.install_pypi_package("googlemaps")
sc.install_pypi_package("pandas")

# Create table of central lat/long for each neighborhood
def central_table(taxi_trips):
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import mean, col, dayofweek

    # Select relevant columns
    reduced_data = taxi_trips.select("pickup_longitude", "pickup_latitude", "name", "trip_start_timestamp")

    # Group by 'name' and calculate the average longitude and latitude for each group
    central_neighborhoods = reduced_data.groupBy('name').agg(
        mean('pickup_longitude').alias('average_longitude'),
        mean('pickup_latitude').alias('average_latitude')
    )

    # convert the table to a pandas dataframe for our Python code in the next step
    import pandas as pd
    central_neighborhoods = central_neighborhoods.toPandas()
    return central_neighborhoods

# Step 1: Create function that gives the trip time in minutes from the starting location to each of the other neighborhoods
def calculate_travel_times(origin_area, central_neighborhoods):
    import googlemaps
    from datetime import datetime
    import pandas as pd
    
    gmaps = googlemaps.Client(key='')
    
    # Extract origin latitude and longitude from the DataFrame
    origin_location = (central_neighborhoods.loc[central_neighborhoods['name'] == origin_area, 'average_latitude'].values[0],
                       central_neighborhoods.loc[central_neighborhoods['name'] == origin_area, 'average_longitude'].values[0])

    # Initialize a dictionary to store travel times
    travel_times = {}

    for index, row in central_neighborhoods.iterrows():
        destination_area = row['name']
        destination_location = (row['average_latitude'], row['average_longitude'])

        # Convert latitude and longitude to string format
        origin = f"{origin_location[0]},{origin_location[1]}"
        destination = f"{destination_location[0]},{destination_location[1]}"

        # Make Distance Matrix API request
        result = gmaps.distance_matrix(origin, destination, mode="driving", departure_time="now")

        # Extract numerical travel time from the API response and convert to integer
        travel_time = int(result['rows'][0]['elements'][0]['duration']['text'].split()[0])

        # Store travel time in the dictionary
        travel_times[destination_area] = travel_time

    # Create a DataFrame from the travel_times dictionary
    travel_times_df = pd.DataFrame(list(travel_times.items()), columns=['Destination', 'Travel Time'])

    return travel_times_df

# Step 3: Compute average metrics for each time period:
def avg_metrics_per_period(df, central_neighborhoods, travel_times):
    # Step #2: Create tables to store the historical data we need for analysis later
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, date_add, hour
    from datetime import datetime, timedelta
    
    spark = SparkSession.builder.appName("ChicagoTaxi").config("spark.executor.memory", "4g").config("spark.executor.cores", "2").getOrCreate()
    
    # Get the current timestamp
    current_timestamp = datetime.now()

    # Calculate the timestamp for four weeks ago
    four_weeks_ago = current_timestamp - timedelta(weeks=10)

    # Extract the current hour
    current_hour = current_timestamp.hour

    # Filter data for the past 4 weeks and the same hour
    table_1 = df.filter(
        (col("trip_start_timestamp") >= four_weeks_ago) &
        (hour(col("trip_start_timestamp")) == current_hour)
    )

    # Save the result as a table
    spark.sql("DROP TABLE IF EXISTS spark_catalog.default.past_4_weeks")
    table_1.write.saveAsTable("past_4_weeks", mode="overwrite")

    # Filter data for the same day of the week during this month in previous years
    day_of_week = current_timestamp.weekday()  # Monday is 0 and Sunday is 6
    same_month_last_years = df.filter(
        (col("day_of_week") == day_of_week) &
        (col("trip_start_timestamp").substr(6, 2) == current_timestamp.strftime("%m")) &
        (col("trip_start_timestamp").substr(1, 4) < current_timestamp.strftime("%Y"))
    )

    table_2 = same_month_last_years.write.saveAsTable("same_month_last_years", mode="overwrite")

    # Filter data for the same day prior to this year
    same_day_prior_years = df.filter(
        (col("trip_start_timestamp").substr(6, 2) == current_timestamp.strftime("%m")) &
        (col("trip_start_timestamp").substr(9, 2) == current_timestamp.strftime("%d")) &
        (col("trip_start_timestamp").substr(1, 4) < current_timestamp.strftime("%Y"))
    )

    table_3 = same_day_prior_years.write.saveAsTable("same_day_prior_years", mode="overwrite")
    
    
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, sum, count

    # Load the tables
    past_4_weeks = spark.table("past_4_weeks")
    same_month_last_years = spark.table("same_month_last_years")
    same_day_prior_years = spark.table("same_day_prior_years")

    # Function to calculate average paid per minute
    def calculate_avg_paid_per_minute(df):
        return df.groupBy("name").agg(
            (sum(col("trip_total")) / (sum(col("trip_seconds")) / 60)).alias("avg_paid_per_minute")
        )

    # Function to calculate average number of rides per minute
    def calculate_avg_rides_per_minute(df):
        return df.groupBy("name").agg(
            (count("*") / (sum(col("trip_seconds")) / 60)).alias("avg_rides_per_minute")
        )
    
    # Calculate metrics for the past 4 weeks table
    avg_rides_per_minute_past_4_weeks = calculate_avg_rides_per_minute(past_4_weeks)
    avg_rides_per_minute_same_month_last_years = calculate_avg_rides_per_minute(same_month_last_years)
    avg_rides_per_minute_same_day_prior_years = calculate_avg_rides_per_minute(same_day_prior_years)

    # Calculate metrics for the same day last years table
    avg_paid_per_minute_past_4_weeks = calculate_avg_paid_per_minute(past_4_weeks)
    avg_paid_per_minute_same_month_last_years = calculate_avg_paid_per_minute(same_month_last_years)
    avg_paid_per_minute_same_month_last_years = calculate_avg_paid_per_minute(same_month_last_years)

    # Calculate metrics for the same day prior years table
    avg_paid_per_minute_past_4_weeks = calculate_avg_paid_per_minute(past_4_weeks)
    avg_paid_per_minute_same_day_prior_years = calculate_avg_paid_per_minute(same_day_prior_years)
    avg_paid_per_minute_same_day_prior_years = calculate_avg_paid_per_minute(same_day_prior_years)

    # Save the results
    avg_paid_per_minute_past_4_weeks = avg_paid_per_minute_past_4_weeks.write.saveAsTable("avg_paid_per_minute_past_4_weeks", mode="overwrite")
    avg_rides_per_minute_past_4_weeks = avg_rides_per_minute_past_4_weeks.write.saveAsTable("avg_rides_per_minute_past_4_weeks", mode="overwrite")
    avg_paid_per_minute_same_month_last_years = avg_paid_per_minute_same_month_last_years.write.saveAsTable("avg_paid_per_minute_same_month_last_years", mode="overwrite")
    avg_rides_per_minute_same_month_last_years = avg_rides_per_minute_same_month_last_years.write.saveAsTable("avg_rides_per_minute_same_month_last_years", mode="overwrite")
    avg_paid_per_minute_same_day_prior_years = avg_paid_per_minute_same_day_prior_years.write.saveAsTable("avg_paid_per_minute_same_day_prior_years", mode="overwrite")
    avg_rides_per_minute_same_day_prior_years = avg_rides_per_minute_same_day_prior_years.write.saveAsTable("avg_rides_per_minute_same_day_prior_years", mode="overwrite")
    
    # Step 4: Aggregate the avg_paid_per_minute tables with a weighted average
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col
    
    # Load the three tables
    avg_paid_per_minute_past_4_weeks = spark.table("avg_paid_per_minute_past_4_weeks")
    avg_paid_per_minute_same_month_last_years = spark.table("avg_paid_per_minute_same_month_last_years")
    avg_paid_per_minute_same_day_prior_years = spark.table("avg_paid_per_minute_same_day_prior_years")

    # Multiply the values by the specified weights
    avg_paid_per_minute_past_4_weeks = avg_paid_per_minute_past_4_weeks.withColumn("weighted_past_4_weeks", col("avg_paid_per_minute") * 0.20)
    avg_paid_per_minute_same_month_last_years = avg_paid_per_minute_same_month_last_years.withColumn("weighted_same_month_last_years", col("avg_paid_per_minute") * 0.40)
    avg_paid_per_minute_same_day_prior_years = avg_paid_per_minute_same_day_prior_years.withColumn("weighted_same_day_prior_years", col("avg_paid_per_minute") * 0.40)

    # Select relevant columns for the final table
    weighted_avg_paid_per_minute = avg_paid_per_minute_past_4_weeks.select("name", "weighted_past_4_weeks") \
        .join(avg_paid_per_minute_same_month_last_years.select("name", "weighted_same_month_last_years"), "name", "outer") \
        .join(avg_paid_per_minute_same_day_prior_years.select("name", "weighted_same_day_prior_years"), "name", "outer")

    # Fill null values with 0
    weighted_avg_paid_per_minute = weighted_avg_paid_per_minute.na.fill(0)

    # Add the weighted columns to get the total weighted paid per minute
    weighted_avg_paid_per_minute = weighted_avg_paid_per_minute.withColumn("total_weighted_paid_per_minute", 
        col("weighted_past_4_weeks") + col("weighted_same_month_last_years") + col("weighted_same_day_prior_years"))

    # Drop intermediate columns if needed
    weighted_avg_paid_per_minute = weighted_avg_paid_per_minute.drop("weighted_past_4_weeks", "weighted_same_month_last_years", "weighted_same_day_prior_years")

    # Show the resulting DataFrame
    weighted_avg_paid_per_minute.show()


    
    # Step 5: Aggregate the avg_rides_per_minute tables with a weighted average
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    # Load the three tables
    avg_rides_past_4_weeks = spark.table("avg_rides_per_minute_past_4_weeks")
    avg_rides_last_years = spark.table("avg_rides_per_minute_same_month_last_years")
    avg_rides_prior_years = spark.table("avg_rides_per_minute_same_day_prior_years")

    # Multiply the numerical column by the specified weights
    weighted_avg_rides_past_4_weeks = avg_rides_past_4_weeks.withColumn("weighted_avg", F.col("avg_rides_per_minute") * 0.20)
    weighted_avg_rides_last_years = avg_rides_last_years.withColumn("weighted_avg", F.col("avg_rides_per_minute") * 0.40)
    weighted_avg_rides_prior_years = avg_rides_prior_years.withColumn("weighted_avg", F.col("avg_rides_per_minute") * 0.40)

    # Union the three tables
    all_weighted_rides = weighted_avg_rides_past_4_weeks.union(weighted_avg_rides_last_years).union(weighted_avg_rides_prior_years)

    # Group by neighborhood and sum the weighted averages
    weighted_avg_rides_per_minute = all_weighted_rides.groupBy("name").agg(F.sum("weighted_avg").alias("total_weighted_rides_per_minute"))


    # Create a function that computes a revenue potential per minute using formulas we developed
    # Join the two DataFrames on the "name" column
    final_table = weighted_avg_rides_per_minute.join(
        weighted_avg_paid_per_minute,
        weighted_avg_rides_per_minute["name"] == weighted_avg_paid_per_minute["name"],
        "inner"
    ).select(
        weighted_avg_rides_per_minute["name"],
        (weighted_avg_rides_per_minute["total_weighted_rides_per_minute"] * weighted_avg_paid_per_minute["total_weighted_paid_per_minute"]).alias("revenue_potential")
    )
    
    print("Weighted revenue potential")
    final_table.show()
    
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col
    
    # Convert pandas DataFrame to PySpark DataFrame
    travel_times_spark = spark.createDataFrame(travel_times)

    # Join final_table with travel_times_spark on the "name" column
    joined_table = final_table.join(
        travel_times_spark,
        final_table["name"] == travel_times_spark["Destination"],
        "inner"
    ).select(
        final_table["name"],
        (final_table["revenue_potential"] * (60 - travel_times_spark["Travel Time"])).alias("final_potential")
    )

    # Show the result
    print("Final Potential Table:")
    joined_table.show()
    
    # Print resulting neighborhood:
    from pyspark.sql.functions import desc

    # Find the neighborhood with the highest final_potential
    highest_potential_neighborhood = joined_table.select("name", "final_potential").orderBy(desc("final_potential")).first()

    # Extract the values
    neighborhood_name = highest_potential_neighborhood["name"]
    highest_potential_value = highest_potential_neighborhood["final_potential"]
    
    # Print the results
    print(f"Based on the time and your current location, the neighborhood with the highest final potential revenue per minute is {neighborhood_name} with a value of ${highest_potential_value:.2f}.")

def function(origin_area):
    from pyspark.sql.functions import mean, col, dayofweek
    # Read static data
    taxi_trips = spark.read.parquet("s3://mbd-dataset/chicago_taxi_trips.parquet")

    # Convert timestamp columns to timestamp type
    taxi_trips = taxi_trips.withColumn("trip_start_timestamp", col("trip_start_timestamp").cast("timestamp"))
    taxi_trips = taxi_trips.withColumn("trip_end_timestamp", col("trip_end_timestamp").cast("timestamp"))

    # Drop unnecessary columns
    columns_to_drop = ["geometry", "index_right", "cartodb_id", "created_at", "updated_at", "__index_level_0__"]
    taxi_trips = taxi_trips.drop(*columns_to_drop)

    # add a column to store the day of the week as an integer
    taxi_trips = taxi_trips.withColumn("day_of_week", dayofweek(col("trip_start_timestamp")))
    
    
    central_neighborhoods = central_table(taxi_trips)
    travel_times = calculate_travel_times(origin_area, central_neighborhoods)
    avg_metrics_per_period(taxi_trips, central_neighborhoods, travel_times)

function("Museum Campus")
