# from src.TsvData import TsvData
# import pyspark.sql.types as t
# from pyspark import SparkConf
# from pyspark.sql import SparkSession, Window
# import pyspark.sql.types as t
# import pyspark.sql.functions as f
# import os
#
# from src.schemas.title_akas_schema import title_akas_schema
# from src.schemas.title_crew_schema import title_crew_schema
# from src.schemas.title_basics_schema import title_basics_schema
# from src.schemas.title_ratings_schema import title_ratings_schema
# from src.schemas.title_episode_schema import title_episode_schema
# from src.schemas.title_principals_schema import title_principals_schema
# from src.schemas.name_basics_schema import name_basics_schema
#
# spark_session = (SparkSession.builder
#                  .master("local[2]")
#                  .appName("task app")
#                  .config(conf=SparkConf())
#                  .getOrCreate())
#
# title_ratings = TsvData(path='./data/title.ratings.tsv', spark_session=spark_session, schema=title_ratings_schema).data
# title_basics = TsvData(path='./data/title.basics.tsv', spark_session=spark_session, schema=title_basics_schema).data
# title_basics = title_basics.withColumn("genres", f.split(f.col("genres"), ",").cast("array<string>"))
# title_crew = TsvData(path='./data/title.crew.tsv', spark_session=spark_session, schema=title_crew_schema).data
# title_principals = TsvData(path='./data/title.principals.tsv', spark_session=spark_session, schema=title_principals_schema).data
# name_basics = TsvData(path='./data/name.basics.tsv', spark_session=spark_session, schema=name_basics_schema).data
# name_basics = name_basics.withColumn("primaryProfession", f.split(f.col("primaryProfession"), ",").cast("array<string>"))
#
# # Question #1 ------------------------------------------------------------------
# # Які топ-10 найпопулярніших фільмів за останні два роки?
#
# movies_with_ratings = title_basics.join(title_ratings, "tconst")
#
# current_year = 2023
# last_two_years = movies_with_ratings.filter(movies_with_ratings.startYear >= current_year - 2)
# top_10_movies = last_two_years.orderBy(f.desc("numVotes")).limit(10)
#
# top_10_movies.show()
#
# # Question #2 ------------------------------------------------------------------
# # Які телесеріали були випущені після 2020 року і мають середній рейтинг вище 8?
#
# movies_with_ratings = title_basics.join(title_ratings, "tconst")
#
# result_2 = movies_with_ratings.filter((movies_with_ratings.startYear > 2020) & (movies_with_ratings.averageRating > 8))
# result_2.show()
#
# # Question #3 ------------------------------------------------------------------
# # Які топ 10 режисерів за середньою оцінкою усіх їх фільмів?
#
# movies_with_crew_ratings = title_ratings.join(title_crew, "tconst")
# movies_with_crew_ratings.show()
#
# directors_avg_rating = movies_with_crew_ratings.groupBy("directors").agg(f.avg("averageRating").alias("avgRating"))
# directors_with_names = directors_avg_rating.join(name_basics, directors_avg_rating["directors"] == name_basics["nconst"])
#
# top_10_directors = directors_with_names.select("primaryName", "avgRating").orderBy(f.desc("avgRating")).limit(10)
#
# top_10_directors.show()
#
# # Question #4 ------------------------------------------------------------------
# # Які актори знімалися разом у найбільшій кількості фільмів, і які фільми це були?
#
# actors_with_names = title_principals.join(name_basics, title_principals["nconst"] == name_basics["nconst"])
#
# actors_movies = actors_with_names.join(title_basics, actors_with_names["tconst"] == title_basics["tconst"])
#
# actors_movies_count = actors_movies.groupBy("primaryName", "primaryTitle").count()
#
# top_actors_movies = actors_movies_count.orderBy(f.col("count").desc()).select("primaryName", "primaryTitle", "count")
# top_actors_movies.show()
# # Question #4 ------------------------------------------------------------------
# # Яка середня тривалість фільмів за жанрами за останні п'ять років?
#
# current_year = 2023
# films_last_five_years = title_basics.filter((title_basics.startYear >= current_year - 5) & (title_basics.titleType == "movie"))
#
# films_with_genres = films_last_five_years.withColumn("genre", f.explode(f.col("genres")))
#
# average_duration_by_genre = films_with_genres.groupBy("genre").agg({"runtimeMinutes": "avg"})
#
# average_duration_by_genre.orderBy("avg(runtimeMinutes)", ascending=False).show()
# # Question #5 ------------------------------------------------------------------
# # Які професії найчастіше зустрічаються серед акторів/режисерів?
#
# filtered_professions = name_basics.filter(
#     (f.array_contains(f.col("primaryProfession"), "actor")) |
#     (f.array_contains(f.col("primaryProfession"), "actress")) |
#     (f.array_contains(f.col("primaryProfession"), "producer"))
# )
#
# profession_counts = filtered_professions.select(f.explode("primaryProfession").alias("profession")) \
#     .groupBy("profession").count()
#
# profession_counts.orderBy("count", ascending=False).show()
# # Question #6 ------------------------------------------------------------------
# # Топ-5 фільмів з найбільшою кількістю голосів у кожному році?
#
# title_basics_with_ratings = title_basics.join(title_ratings, 'tconst')
# window_spec = Window.partitionBy("startYear").orderBy(f.col("numVotes").desc())
#
# ranked_movies = title_basics_with_ratings.withColumn("rank", f.row_number().over(window_spec))
#
# top_5_movies_per_year = ranked_movies.filter(f.col("rank") <= 5).orderBy('startYear', ascending=False)
# top_5_movies_per_year.show()
# # Question #7 ------------------------------------------------------------------
# # Як змінюється рейтинг фільмів в порівнянні з середнім рейтингом жанру в кожному році?
#
# title_basics_with_ratings = title_basics.join(title_ratings, 'tconst')
# window_spec = Window.partitionBy("genres", "startYear").orderBy("startYear")
#
# films_with_genre_avg = title_basics_with_ratings.withColumn("avg_genre_rating", f.avg("averageRating").over(window_spec))
#
# rating_deviation = films_with_genre_avg.withColumn("rating_deviation", f.col("averageRating") - f.col("avg_genre_rating"))
#
# rating_deviation.select("tconst", "primaryTitle", "startYear", "genres", "averageRating", "avg_genre_rating", "rating_deviation").show()

from src.Question import Question

questions = Question()

questions.top_10_movies_in_2_years()
questions.series_released_since_2020_and_rating_higher_than_8()
questions.top_10_directors_by_average_rating()
questions.most_frequent_co_actors()
questions.most_common_professions()
questions.top_5_movies_by_votes_per_year()
questions.compare_movie_rating_to_genre_mean_by_year()