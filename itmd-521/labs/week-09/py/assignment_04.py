from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, col, when
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = (SparkSession.builder.appName("Assignment04")
             .config("spark.sql.catalogImplementation", "hive").getOrCreate())
    jdbcDF = (spark
              .read
              .format("jdbc")
              .option("url", "jdbc:mysql://localhost:3306/employees")
              .option("driver", "com.mysql.jdbc.Driver")
              .option("dbtable", "employees")
              .option("user", "worker")
              .option("password", "cluster")
              .load())
    
    print(jdbcDF.count())
    jdbcDF.printSchema()
    #PART I
    salaries_df = (spark
              .read
              .format("jdbc")
              .option("url", "jdbc:mysql://localhost:3306/employees")
              .option("driver", "com.mysql.jdbc.Driver")
              .option("dbtable", "salaries")
              .option("user", "worker")
              .option("password", "cluster")
              .load())
    
    top_salaries_DF = salaries_df.orderBy(salaries_df["salary"].desc()).limit(10000)
    
    
    (top_salaries_DF.write.format("jdbc") 
                .option("url", "jdbc:mysql://localhost:3306/employees") 
                .option("driver", "com.mysql.jdbc.Driver") 
                .option("dbtable", "aces") 
                .option("user", "worker") 
                .option("password", "cluster") 
                .mode("overwrite") 
                .save())
    
    top_salaries_DF.write.format('csv').option('compression','snappy').mode('overwrite').save('top_salaries.csv.snappy')
    
    #PART II
    
    # Useing a SQL query to directly create a DataFrame.
    se_df = (spark.read.format("jdbc") 
            .option("url", "jdbc:mysql://localhost:3306/employees") 
            .option("driver", "com.mysql.jdbc.Driver") 
            .option("user", "worker") 
            .option("password", "cluster") 
            .option("query", "SELECT * FROM titles WHERE title = 'Senior Engineer'") 
            .load())

    # Including a temporary column indicating whether the worker is still employed by the firm or has departed.
    se_df = se_df.withColumn("status", 
                             when(col("to_date") == "9999-01-01", "current")
                            .when(col("to_date") < current_date(), "left")
                            .otherwise("NA"))
    
 
    # Count the number of senior engineers who have left
    left_count = se_df.filter(col("status") == "left").count()

    # Count the number of senior engineers who are current
    current_count = se_df.filter(col("status") == "current").count()

    print("Number of Senior Engineers who have left:", left_count)
    print("Number of Senior Engineers who are still with the company:", current_count)  
    
    # Filter  the dataframe where status of the employee is "left"
    left_se_df = se_df.filter(col("status") == "left")

    # Create a PySpark SQL table of just the Senior Engineers information that have left the company
    left_se_df.write.saveAsTable("left_table", mode="overwrite")
    
    # Create a PySpark SQL tempView of just the Senior Engineers information that have left the company
    left_se_df.createOrReplaceTempView("left_tempview")
    
    # Create a PySpark DataFrame of just the Senior Engineers information that have left the company
    left_se_df.write.saveAsTable("left_df", mode="overwrite")
    #PART III
    # Writing these back to the database
    left_table_df = spark.table("left_table")
    left_tempview_df = spark.table("left_tempview")
    left_df_df = spark.table("left_df")
    
    (left_table_df.write.format("jdbc") 
                .option("url", "jdbc:mysql://localhost:3306/employees") 
                .option("driver", "com.mysql.jdbc.Driver") 
                .option("dbtable", "left_table") 
                .option("user", "worker") 
                .option("password", "cluster") 
                .mode("overwrite") 
                .save())
    
    (left_tempview_df.write.format("jdbc") 
                .option("url", "jdbc:mysql://localhost:3306/employees") 
                .option("driver", "com.mysql.jdbc.Driver") 
                .option("dbtable", "left_tempview") 
                .option("user", "worker") 
                .option("password", "cluster") 
                .mode("overwrite") 
                .save())
    
    (left_df_df.write.format("jdbc") 
                .option("url", "jdbc:mysql://localhost:3306/employees") 
                .option("driver", "com.mysql.jdbc.Driver") 
                .option("dbtable", "left_df") 
                .option("user", "worker") 
                .option("password", "cluster") 
                .mode("overwrite") 
                .save())
        
    # Write the DataFrame to the database, and set the mode type to errorifexists
    (left_table_df.write.format("jdbc") 
                .option("url", "jdbc:mysql://localhost:3306/employees") 
                .option("driver", "com.mysql.jdbc.Driver") 
                .option("dbtable", "left_df") 
                .option("user", "worker") 
                .option("password", "cluster") 
                .mode("errorifexists") 
                .save())