import pyspark

spark = pyspark.sql.SparkSession \
    .builder \
    .appName("Python spark sql basic example") \
    .config('spark.driver.extraClassPath', "/Users/Insaf/Downloads/postgresql-42.7.4.jar") \
    .getOrCreate()


def extract_movies_to_df():
    movies_df = spark.read \
        .format("jdbc") \
        .option("url","jdbc:postgresql://localhost:5432/etl_pipeline") \
        .option("dbtable", "movies") \
        .option("user", "Insaf") \
        .option("password", "insaf") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    return movies_df

def extract_users_to_df():

    users_df = spark.read \
        .format("jdbc") \
        .option("url","jdbc:postgresql://localhost:5432/etl_pipeline") \
        .option("dbtable", "users") \
        .option("user", "Insaf") \
        .option("password", "insaf") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    return users_df

def transform_avg_ratings(movies_df, users_df):

    avg_rating = users_df.groupBy("movie_id").mean("rating")

    df = movies_df.join(avg_rating, movies_df.id == avg_rating.movie_id)

    return df


def load_df_to_db(df):
    mode="overwrite"
    url="jdbc:postgresql://localhost:5432/etl_pipeline"
    properties={"user":"Insaf",
                "password":"insaf",
                "driver":"org.postgresql.Driver"}
    df.write.jdbc(url=url,
                  table="avg_ratings",
                  mode=mode,
                  properties=properties)

movies_df = extract_movies_to_df()
users_df = extract_users_to_df()
ratings_df = transform_avg_ratings(movies_df, users_df)
load_df_to_db(ratings_df)
    
