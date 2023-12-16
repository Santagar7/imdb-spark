import pyspark.sql.functions as f
from pyspark import SparkConf
from pyspark.sql import SparkSession, Window

from src.TsvData import TsvData
from src.schemas.name_basics_schema import name_basics_schema
from src.schemas.title_basics_schema import title_basics_schema
from src.schemas.title_crew_schema import title_crew_schema
from src.schemas.title_principals_schema import title_principals_schema
from src.schemas.title_ratings_schema import title_ratings_schema
from src.schemas.title_akas_schema import title_akas_schema
from src.schemas.title_episode_schema import title_episode_schema
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
        self.title_akas = TsvData(path=DatasetsDirectories.akas, spark_session=self.spark_session,
                                  schema=title_akas_schema).data
        self.title_basics = TsvData(path=DatasetsDirectories.basics, spark_session=self.spark_session,
                                    schema=title_basics_schema).data
        self.title_episodes = TsvData(path=DatasetsDirectories.episodes, spark_session=self.spark_session, schema=title_episode_schema).data
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

    def series_with_most_episodes(self):
        window_spec = Window.partitionBy(ColumnNames.parentTconst)
        episode_count = self.title_episodes.withColumn("episodeCount",
                                                       f.count(ColumnNames.episodeNumber).over(window_spec))
        top_series = episode_count.groupBy(ColumnNames.parentTconst).agg(f.max("episodeCount").alias("totalEpisodes"))
        top_series_with_names = top_series.join(self.title_basics,
                                                top_series[ColumnNames.parentTconst] == self.title_basics[
                                                    ColumnNames.tconst]).select(ColumnNames.primaryTitle,
                                                                                "totalEpisodes").orderBy(
            f.desc("totalEpisodes")).limit(10)
        top_series_with_names.show()

    def shortest_movies(self):
        # Вибираємо тільки ті записи, де тривалість вказана
        non_null_movies = self.title_basics.filter(self.title_basics[ColumnNames.runtimeMinutes].isNotNull())

        # Використовуємо Window function для сортування фільмів за тривалістю
        window_spec = Window.partitionBy(ColumnNames.titleType).orderBy(ColumnNames.runtimeMinutes)
        shortest_movies_ranked = non_null_movies.withColumn("rank", f.row_number().over(window_spec))

        # Відбираємо топ 10 найкоротших фільмів
        shortest_movies = shortest_movies_ranked.filter(shortest_movies_ranked.rank <= 10).select(
            ColumnNames.primaryTitle, ColumnNames.runtimeMinutes)

        shortest_movies.show()

    def actors_with_biggest_rating_difference(self):
        window_spec = Window.partitionBy(self.title_principals[ColumnNames.nconst])
        actors_movies_with_ratings = self.title_principals.join(self.title_ratings,
                                                                self.title_principals[ColumnNames.tconst] ==
                                                                self.title_ratings[ColumnNames.tconst])
        max_min_ratings = actors_movies_with_ratings.withColumn("maxRating", f.max(ColumnNames.averageRating).over(
            window_spec)).withColumn("minRating", f.min(ColumnNames.averageRating).over(window_spec))
        actors_rating_diff = max_min_ratings.withColumn("ratingDifference", f.col("maxRating") - f.col("minRating"))
        top_actors_by_rating_diff = actors_rating_diff.join(self.name_basics,
                                                            actors_rating_diff[ColumnNames.nconst] == self.name_basics[
                                                                ColumnNames.nconst]).select(ColumnNames.primaryName,
                                                                                            "ratingDifference").distinct().orderBy(
            f.desc("ratingDifference")).limit(10)
        top_actors_by_rating_diff.show()

    def movies_with_most_adaptations(self):
        window_spec = Window.partitionBy(ColumnNames.titleId)
        adaptations_count = self.title_akas.withColumn("adaptationsCount", f.count(ColumnNames.title).over(window_spec))
        top_movies_by_adaptations = adaptations_count.groupBy(ColumnNames.titleId).agg(
            f.max("adaptationsCount").alias("totalAdaptations"))
        top_movies_with_names = top_movies_by_adaptations.join(self.title_basics,
                                                               top_movies_by_adaptations[ColumnNames.titleId] ==
                                                               self.title_basics[ColumnNames.tconst]).select(
            ColumnNames.primaryTitle, "totalAdaptations").orderBy(f.desc("totalAdaptations")).limit(10)
        top_movies_with_names.show()

    def actors_with_longest_careers(self):
        actors_career_length = self.name_basics.withColumn("careerLength",
                                                           f.col(ColumnNames.deathYear) - f.col(ColumnNames.birthYear))
        window_spec = Window.orderBy(f.desc("careerLength"))
        top_actors_by_career_length = actors_career_length.withColumn("rank", f.row_number().over(window_spec)).filter(
            f.col("rank") <= 10)
        top_actors_by_career_length.select(ColumnNames.primaryName, "careerLength").show()

    def actors_in_most_animated_movies(self):
        animated_movies = self.title_basics.filter(f.array_contains(f.col(ColumnNames.genres), "Animation"))
        actors_in_animated = animated_movies.join(self.title_principals,
                                                  animated_movies[ColumnNames.tconst] == self.title_principals[
                                                      ColumnNames.tconst])

        # Вказуємо конкретну таблицю для tconst
        window_spec = Window.partitionBy(self.title_principals[ColumnNames.nconst])
        actors_count = actors_in_animated.withColumn("animatedCount",
                                                     f.count(self.title_principals[ColumnNames.tconst]).over(
                                                         window_spec))

        # Явно вказуємо таблицю для nconst
        top_actors_in_animated = actors_count.join(self.name_basics,
                                                   actors_count[ColumnNames.nconst] == self.name_basics[
                                                       ColumnNames.nconst]).groupBy(
            self.name_basics[ColumnNames.nconst], self.name_basics[ColumnNames.primaryName]).agg(
            f.max("animatedCount").alias("totalAnimated")).orderBy(f.desc("totalAnimated")).limit(10)
        top_actors_in_animated.show()



