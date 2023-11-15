from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

def load(dataset="dbfs:/FileStore/IDS706-Databricks-ETL-YuhanXue/grad-students.csv", 
         dataset2="dbfs:/FileStore/IDS706-Databricks-ETL-YuhanXue/majors-list.csv"):
    spark = SparkSession.builder.appName("Read CSV").getOrCreate()
    # load csv and transform it by inferring schema 
    students_df = spark.read.csv(dataset, header=True, inferSchema=True)
    majors_df = spark.read.csv(dataset2, header=True, inferSchema=True)

    # add unique IDs to the DataFrames
    students_df = students_df.withColumn("id", monotonically_increasing_id())
    majors_df = majors_df.withColumn("id", monotonically_increasing_id())

    # transform into a delta lakes table and store it 
    students_df.write.format("delta").mode("overwrite").saveAsTable("students_delta")
    majors_df.write.format("delta").mode("overwrite").saveAsTable("major_delta")
    
    num_rows = students_df.count()
    print(num_rows)
    num_rows = majors_df.count()
    print(num_rows)
    
    return "finished transform and load"

if __name__ == "__main__":
    load()