from pyspark.sql import SparkSession
import matplotlib.pyplot as plt


# sample query
def query_transform():
    """
    Run a predefined SQL query on a Spark DataFrame.

    Returns:
        DataFrame: Result of the SQL query.
    """
    spark = SparkSession.builder.appName("Query").getOrCreate()
    query = (
        "SELECT t1.major_category as major, "
        "SUM(grad_total) as num_students, "
        "SUM(grad_employed) / SUM(grad_total) as employ_rate "
        "FROM major_delta t1 "
        "JOIN students_delta t2 ON t1.major = t2.major "
        "GROUP BY t1.major_category "
        "ORDER BY num_students DESC, major "
        "LIMIT 10"
    )
    query_result = spark.sql(query)
    return query_result


# sample viz for project
def vis():
    query = query_transform()
    count = query.count()
    if count > 0:
        print(f"Data validation passed. {count} rows available.")
    else:
        print("No data available. Please investigate.")
    
    plt.figure(figsize=(15, 8))  # Adjusted figure size

    pandas_df = query.select("major", 
                             "num_students",
                             "employ_rate").toPandas()

    # Plot a bar plot
    plt.figure(figsize=(15, 8))
    plt.bar(pandas_df["major"], pandas_df["num_students"], color='skyblue')
    plt.title("Top 10 Popular Major Categories")
    plt.xlabel("Major")
    plt.ylabel("Number of Students")
    plt.xticks(rotation=45)
    plt.show()


    # Plot a bar plot
    plt.figure(figsize=(15, 8))
    plt.bar(pandas_df["major"], pandas_df["employ_rate"], color='skyblue')
    plt.title("Employment Rate of Top 10 Popular Major Categories")
    plt.xlabel("Major")
    plt.ylabel("Employment Rate")
    plt.xticks(rotation=45)
    plt.show()

    # Plot a single histogram for all groups
    # plt.figure(figsize=(15, 8))
    # plt.hist(pandas_df["avg_soccer_power_in_613"], bins=20, edgecolor='black')  
    # # Adjust the number of bins as needed
    # plt.title("Avg Soccer Power in Group by Group on 613")
    # plt.xlabel("Group")
    # plt.ylabel("Avg Soccer Power in Group")
    # plt.show()


if __name__ == "__main__":
    query_transform()
    vis()