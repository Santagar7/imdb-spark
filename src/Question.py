import pyspark.sql.functions as f
from pyspark import SparkConf
from pyspark.sql import SparkSession, Window

from src.TsvData import TsvData
from src.schemas.name_basics_schema import name_basics_schema
from src.schemas.title_basics_schema import title_basics_schema
from src.schemas.title_crew_schema import title_crew_schema
from src.schemas.title_principals_schema import title_principals_schema
from src.schemas.title_ratings_schema import title_ratings_schema
from src.constants.datasets_directories import DatasetsDirectories
from src.constants.column_names import ColumnNames


class Question:
    def __init__(self):
        self.spark_session = (SparkSession.builder
                              .master("local[2]")
                              .appName("task app")
                              .config(conf=SparkConf())
                              .getOrCreate())

        self.title_ratings = TsvData(path=DatasetsDirectories.ratings, spark_session=self.spark_session,
                                     schema=title_ratings_schema).data
        self.title_basics = TsvData(path=DatasetsDirectories.basics, spark_session=self.spark_session,
                                    schema=title_basics_schema).data
        self.title_basics = self.title_basics.withColumn(ColumnNames.genres,
                                                         f.split(f.col(ColumnNames.genres), ",").cast("array<string>"))
        self.title_crew = TsvData(path=DatasetsDirectories.crew, spark_session=self.spark_session,
                                  schema=title_crew_schema).data
        self.title_principals = TsvData(path=DatasetsDirectories.principals, spark_session=self.spark_session,
                                        schema=title_principals_schema).data
        self.name_basics = TsvData(path=DatasetsDirectories.name_basics, spark_session=self.spark_session,
                                   schema=name_basics_schema).data
        self.name_basics = self.name_basics.withColumn(ColumnNames.primaryProfession,
                                                       f.split(f.col(ColumnNames.primaryProfession), ",").cast(
                                                           "array<string>"))

    def top_10_movies_in_2_years(self):
        # Question #1 ------------------------------------------------------------------
        # Які топ-10 найпопулярніших фільмів за останні два роки?
        movies_with_ratings = self.title_basics.join(self.title_ratings, ColumnNames.tconst)

        current_year = 2023
        last_two_years = movies_with_ratings.filter(movies_with_ratings.startYear >= current_year - 2)
        top_10_movies = last_two_years.orderBy(f.desc(ColumnNames.numVotes)).limit(10)

        top_10_movies.show()

    def series_released_since_2020_and_rating_higher_than_8(self):
        # Question #2 ------------------------------------------------------------------
        # Які телесеріали були випущені після 2020 року і мають середній рейтинг вище 8?
        movies_with_ratings = self.title_basics.join(self.title_ratings, ColumnNames.tconst)

        result_2 = movies_with_ratings.filter(
            (movies_with_ratings.startYear > 2020) & (movies_with_ratings.averageRating > 8))
        result_2.show()

    def top_10_directors_by_average_rating(self):
        # Question #3 ------------------------------------------------------------------
        # Які топ 10 режисерів за середньою оцінкою усіх їх фільмів?

        movies_with_crew_ratings = self.title_ratings.join(self.title_crew, ColumnNames.tconst)
        directors_avg_rating = movies_with_crew_ratings.groupBy(ColumnNames.directors).agg(
            f.avg(ColumnNames.averageRating).alias(ColumnNames.avgRating))
        directors_with_names = directors_avg_rating.join(self.name_basics,
                                                         directors_avg_rating[ColumnNames.directors] ==
                                                         self.name_basics[ColumnNames.nconst])
        top_10_directors = directors_with_names.select(ColumnNames.primaryName, ColumnNames.avgRating).orderBy(
            f.desc(ColumnNames.avgRating)).limit(
            10)
        top_10_directors.show()

    def most_frequent_co_actors(self):
        # Question #4 ------------------------------------------------------------------
        # Які актори знімалися разом у найбільшій кількості фільмів, і які фільми це були?

        actors_with_names = self.title_principals.join(self.name_basics,
                                                       self.title_principals[ColumnNames.nconst] == self.name_basics[
                                                           ColumnNames.nconst])
        actors_movies = actors_with_names.join(self.title_basics,
                                               actors_with_names[ColumnNames.tconst] == self.title_basics[
                                                   ColumnNames.tconst])
        actors_movies_count = actors_movies.groupBy(ColumnNames.primaryName, ColumnNames.primaryTitle).count()
        top_actors_movies = actors_movies_count.orderBy(f.col(ColumnNames.count).desc()).select(ColumnNames.primaryName,
                                                                                                ColumnNames.primaryTitle,
                                                                                                ColumnNames.count)
        top_actors_movies.show()

    def most_common_professions(self):
        # Question #5 ------------------------------------------------------------------
        # Які професії найчастіше зустрічаються серед акторів/режисерів?

        filtered_professions = self.name_basics.filter(
            (f.array_contains(f.col(ColumnNames.primaryProfession), "actor")) |
            (f.array_contains(f.col(ColumnNames.primaryProfession), "actress")) |
            (f.array_contains(f.col(ColumnNames.primaryProfession), "producer"))
        )
        profession_counts = filtered_professions.select(
            f.explode(ColumnNames.primaryProfession).alias(ColumnNames.profession)) \
            .groupBy(ColumnNames.profession).count()
        profession_counts.orderBy(ColumnNames.count, ascending=False).show()

    def top_5_movies_by_votes_per_year(self):
        # Question #6 ------------------------------------------------------------------
        # Топ-5 фільмів з найбільшою кількістю голосів у кожному році?

        title_basics_with_ratings = self.title_basics.join(self.title_ratings, ColumnNames.tconst)
        window_spec = Window.partitionBy(ColumnNames.startYear).orderBy(f.col(ColumnNames.numVotes).desc())

        ranked_movies = title_basics_with_ratings.withColumn(ColumnNames.rank, f.row_number().over(window_spec))

        top_5_movies_per_year = ranked_movies.filter(f.col(ColumnNames.rank) <= 5).orderBy(ColumnNames.startYear,
                                                                                           ascending=False)
        top_5_movies_per_year.show()

    def compare_movie_rating_to_genre_mean_by_year(self):
        # Question #7 ------------------------------------------------------------------
        # Як змінюється рейтинг фільмів в порівнянні з середнім рейтингом жанру в кожному році?

        title_basics_with_ratings = self.title_basics.join(self.title_ratings, ColumnNames.tconst)
        window_spec = Window.partitionBy(ColumnNames.genres, ColumnNames.startYear).orderBy(ColumnNames.startYear)
        films_with_genre_avg = title_basics_with_ratings.withColumn(ColumnNames.avg_genre_rating,
                                                                    f.avg(ColumnNames.averageRating).over(window_spec))
        rating_deviation = films_with_genre_avg.withColumn(ColumnNames.rating_deviation,
                                                           f.col(ColumnNames.averageRating) - f.col(
                                                               ColumnNames.avg_genre_rating))
        rating_deviation.select(ColumnNames.tconst, ColumnNames.primaryTitle, ColumnNames.startYear, ColumnNames.genres,
                                ColumnNames.averageRating, ColumnNames.avg_genre_rating,
                                ColumnNames.rating_deviation).show()

    def top_20_actors_most_episodes(self):
        # Question #8 ------------------------------------------------------------------
        # Які актори знімалися у найбільшій кількості епізодів одного серіалу за останні 10 років?
        current_year = 2023
        last_10_years = self.title_basics.filter(
            (f.col(ColumnNames.startYear) >= (current_year - 10)) & (f.col(ColumnNames.titleType) == "tvSeries")
        )
        episodes_count = self.title_principals.join(
            last_10_years, self.title_principals.tconst == last_10_years.tconst
        ).filter(
            f.col(ColumnNames.category).isin(["actor", "actress"])
        ).groupBy(ColumnNames.nconst).count()

        top_20_actors = episodes_count.join(
            self.name_basics, episodes_count.nconst == self.name_basics.nconst
        ).select(
            ColumnNames.primaryName, ColumnNames.count
        ).orderBy(f.desc(ColumnNames.count)).limit(20)

        top_20_actors.show()

    def actors_with_worst_ratings(self):
        # Question #9 ------------------------------------------------------------------
        # Які актори мають найгірші середні оцінки серед фільмів, у яких вони знімалися, при умові, що вони знялися у більш ніж 50 проектах?
        actors = self.title_principals.alias("actors")
        ratings = self.title_ratings.alias("ratings")

        actors_ratings = actors.join(
            ratings,
            actors[ColumnNames.tconst] == ratings[ColumnNames.tconst]
        ).filter(
            f.col(ColumnNames.category).isin(["actor", "actress"])
        )

        actor_avg_ratings = actors_ratings.groupBy("actors.nconst").agg(
            f.avg("ratings.averageRating").alias("avg_rating"),
            f.count("actors.tconst").alias("num_projects")
        )

        experienced_actors = actor_avg_ratings.filter(
            f.col("num_projects") > 50
        )

        worst_rated_actors = experienced_actors.join(
            self.name_basics,
            experienced_actors[ColumnNames.nconst] == self.name_basics[ColumnNames.nconst]
        ).select(
            ColumnNames.primaryName, "avg_rating", "num_projects"
        ).orderBy("avg_rating")

        worst_rated_actors.show()

    def actors_most_improved_rating(self):
        # Question #10 ------------------------------------------------------------------
        # Які актори мають найбільший приріст середньої оцінки в порівнянні з першим та останнім фільмом, у яких вони знімалися?
        actors_with_titles = self.title_principals.join(
            self.title_basics, self.title_principals.tconst == self.title_basics.tconst
        ).join(
            self.title_ratings, self.title_principals.tconst == self.title_ratings.tconst
        )

        window_spec = Window.partitionBy(ColumnNames.nconst).orderBy(ColumnNames.startYear)

        actors_rating_improvement = actors_with_titles.withColumn(
            "first_rating", f.first(ColumnNames.averageRating).over(window_spec)
        ).withColumn(
            "last_rating", f.last(ColumnNames.averageRating).over(window_spec)
        ).withColumn(
            "rating_improvement", f.col("last_rating") - f.col("first_rating")
        )

        top_actors_by_improvement = actors_rating_improvement.groupBy(ColumnNames.nconst).agg(
            f.max("rating_improvement").alias("max_improvement")
        )

        top_actors_by_improvement = top_actors_by_improvement.join(
            self.name_basics,
            top_actors_by_improvement[ColumnNames.nconst] == self.name_basics[ColumnNames.nconst]
        ).select(
            "primaryName", "max_improvement"
        ).orderBy(f.desc("max_improvement"))

        top_actors_by_improvement.show()

    def actors_most_genres(self):
        # Question #11 ------------------------------------------------------------------
        # Які актори знімалися у найбільшій кількості жанрів?
        exploded_genres = self.title_basics.withColumn(ColumnNames.genre, f.explode(ColumnNames.genres))
        actor_genres = self.title_principals.join(
            exploded_genres, self.title_principals.tconst == exploded_genres.tconst
        ).filter(f.col(ColumnNames.category).isin(["actor", "actress"]))
        genre_count = actor_genres.groupBy(ColumnNames.nconst).agg(
            f.countDistinct(ColumnNames.genre).alias("genre_count")
        )
        actors_names = genre_count.join(
            self.name_basics,
            genre_count.nconst == self.name_basics.nconst
        ).select(
            ColumnNames.primaryName, "genre_count"
        ).orderBy(f.desc("genre_count"))
        actors_names.show()

    def directors_most_actors(self):
        # Question #12 ------------------------------------------------------------------
        # Які режисери працювали з найбільшою кількістю різних акторів?
        actors_per_director = self.title_principals.join(
            self.title_crew, self.title_principals.tconst == self.title_crew.tconst
        ).filter(f.col(ColumnNames.category).isin(["actor", "actress"]))
        unique_actors = actors_per_director.groupBy(ColumnNames.directors).agg(
            f.countDistinct(ColumnNames.nconst).alias("unique_actors_count")
        )
        directors_names = unique_actors.join(
            self.name_basics,
            unique_actors.directors == self.name_basics.nconst
        ).select(
            ColumnNames.primaryName, "unique_actors_count"
        ).orderBy(f.desc("unique_actors_count"))
        directors_names.show()

    def actors_most_directors(self):
        # Question #13 ------------------------------------------------------------------
        # Які актори працювали з найбільшою кількістю різних режисерів?
        directors_per_actor = self.title_principals.join(
            self.title_crew, self.title_principals.tconst == self.title_crew.tconst
        ).filter(f.col(ColumnNames.category).isin(["actor", "actress"]))
        unique_directors = directors_per_actor.groupBy(ColumnNames.nconst).agg(
            f.countDistinct(ColumnNames.directors).alias("unique_directors_count")
        )
        unique_directors = unique_directors.join(
            self.name_basics,
            unique_directors.nconst == self.name_basics.nconst
        ).select(
            ColumnNames.primaryName, "unique_directors_count"
        ).orderBy(f.desc("unique_directors_count"))
        unique_directors.show()
