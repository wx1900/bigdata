import os
from pyspark.mllib.recommendation import ALS
from pyspark.mllib.recommendation import MatrixFactorizationModel

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_counts_and_averages(ID_and_ratings_tuple):
    """Given a tuple (movieID, ratings_iterable) 
    returns (movieID, (ratings_count, ratings_avg))
    """
    nratings = len(ID_and_ratings_tuple[1])
    return ID_and_ratings_tuple[0], (nratings, float(sum(x for x in ID_and_ratings_tuple[1]))/nratings)


class RecommendationEngine:
    """A movie recommendation engine
    """

    def __count_and_average_ratings(self):
        """Updates the movies ratings counts from 
        the current data self.ratings_RDD
        """
        logger.info("Counting movie ratings...")
        movie_ID_with_ratings_RDD = self.ratings_RDD.map(lambda x: (x[1], x[2])).groupByKey()
        self.movie_ID_with_avg_ratings_RDD = movie_ID_with_ratings_RDD.map(get_counts_and_averages)
        self.movies_rating_counts_RDD = self.movie_ID_with_avg_ratings_RDD.map(lambda x: (x[0], x[1][0])) # id rating_counts
        # self.movies_avg_rating_counts_RDD = movie_ID_with_avg_ratings_RDD.map(lambda x: (x[0], x[1][0], x[1][1])) #id rating_counts rating 

    def __train_model(self):
        """Train the ALS model with the current dataset
        """
        logger.info("Training the ALS model...")
        self.model = ALS.train(self.ratings_RDD, self.rank, seed=self.seed,
                               iterations=self.iterations, lambda_=self.regularization_parameter)
        logger.info("ALS model built!")

    def __load_model(self):
        """Load the ALS model with the current dataset
        """
        logger.info("Loading the ALS model...")
        model_path = os.path.join("file:///usr/local/spark/mycode/recommender", 'models', 'movie_lens_als')
        # /home/201500130058/task5
        # file:///usr/local/spark/mycode/recommender
        self.model = MatrixFactorizationModel.load(self.sc, model_path)
        logger.info("ALS model loaded!")

    def __predict_ratings(self, user_and_movie_RDD):
        """Gets predictions for a given (userID, movieID) formatted RDD
        Returns: an RDD with format (movieTitle, movieRating, numRatings)
        """
        predicted_RDD = self.model.predictAll(user_and_movie_RDD)
        predicted_rating_RDD = predicted_RDD.map(lambda x: (x.product, x.rating))
        predicted_rating_title_and_count_RDD = \
            predicted_rating_RDD.join(self.movies_titles_RDD).join(self.movies_rating_counts_RDD)
        predicted_rating_title_and_count_RDD = \
            predicted_rating_title_and_count_RDD.map(lambda r: (r[1][0][1], r[1][0][0], r[1][1]))
        
        return predicted_rating_title_and_count_RDD
    
    def add_ratings(self, ratings):
        """Add additional movie ratings in the format (user_id, movie_id, rating)
        """
        # Convert ratings to an RDD
        new_ratings_RDD = self.sc.parallelize(ratings)
        # Add new ratings to the existing ones
        self.ratings_RDD = self.ratings_RDD.union(new_ratings_RDD)
        # Re-compute movie ratings count
        # self.__count_and_average_ratings()
        # Re-train the ALS model with the new ratings
        self.__train_model()
        
        return ratings

    def get_ratings_for_movie_ids(self, user_id, movie_ids):
        """Given a user_id and a list of movie_ids, predict ratings for them 
        """
        requested_movies_RDD = self.sc.parallelize(movie_ids).map(lambda x: (user_id, x))
        # Get predicted ratings
        ratings = self.__predict_ratings(requested_movies_RDD).collect()

        return ratings
    
    def get_top_ratings(self, user_id, movies_count):
        """Recommends up to movies_count top unrated movies to user_id
        """
        # Get pairs of (userID, movieID) for user_id unrated movies
        user_unrated_movies_RDD = self.ratings_RDD
                                        .filter(lambda rating: not rating[0] == user_id)
                                        .map(lambda x: (user_id, x[1])).distinct()
        # Get predicted ratings
        ratings = self.__predict_ratings(user_unrated_movies_RDD)
                    .filter(lambda r: r[2]>=25).takeOrdered(movies_count, key=lambda x: -x[1])
        print(ratings)
        return ratings

    def get_top_list(self, k):
        """Return the most top k popular movies with their average rating and counts 
        """
        # top_list = self.movie_ID_with_avg_ratings_RDD.filter(lambda r: r[1][1]>=4.7).takeOrdered(k)
        # movie_ID_avg_ratings_titles_RDD = self.movie_ID_with_avg_ratings_RDD.join(self.movies_titles_RDD).collect()
        # print(movie_ID_avg_ratings_titles_RDD)
        # top_list = movie_ID_avg_ratings_titles_RDD.filter(lambda r: r[1][0][0] > 500).takeOrdered(k, key=lambda x: -x[1][0][1])
        top_list = self.movie_ID_with_avg_ratings_RDD.join(self.movies_titles_RDD).
                    filter(lambda r: r[1][0][0] > 100).takeOrdered(k, key=lambda x: -x[1][0][1])
        print(top_list)
        return top_list

    def get_movies_ID_title(self):
        return self.movies_titles_RDD.collect()

    def __init__(self, sc, dataset_path):
        """Init the recommendation engine given a Spark context and a dataset path
        """

        logger.info("Starting up the Recommendation Engine: ")

        self.sc = sc

        # Load ratings data for later use
        logger.info("Loading Ratings data...")
        ratings_file_path = os.path.join(dataset_path, 'ratings.csv')
        ratings_raw_RDD = self.sc.textFile(ratings_file_path)
        ratings_raw_data_header = ratings_raw_RDD.take(1)[0]
        self.ratings_RDD = ratings_raw_RDD.filter(lambda line: line!=ratings_raw_data_header)\
            .map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),int(tokens[1]),float(tokens[2]))).cache()

        dataset_path_small = os.path.join('file:///home/thea/work/big_data/task5/datasets', 'ml-latest-small')
        ratings_file_path_small = os.path.join(dataset_path_small, 'ratings.csv')
        ratings_raw_RDD_small = self.sc.textFile(ratings_file_path_small)
        ratings_raw_data_header_small = ratings_raw_RDD_small.take(1)[0]
        self.ratings_RDD_small = ratings_raw_RDD_small.filter(lambda line: line!=ratings_raw_data_header_small)\
            .map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),int(tokens[1]),float(tokens[2]))).cache()

        # Load movies data for later use
        logger.info("Loading Movies data...")
        movies_file_path = os.path.join(dataset_path, 'movies.csv')
        movies_raw_RDD = self.sc.textFile(movies_file_path)
        movies_raw_data_header = movies_raw_RDD.take(1)[0]
        self.movies_RDD = movies_raw_RDD.filter(lambda line: line!=movies_raw_data_header)\
            .map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),tokens[1],tokens[2])).cache()
        self.movies_titles_RDD = self.movies_RDD.map(lambda x: (int(x[0]),x[1])).cache()
        # Pre-calculate movies ratings counts
        self.__count_and_average_ratings()

        # Train the model
        self.rank = 8
        self.seed = 5L
        self.iterations = 10
        self.regularization_parameter = 0.1
        # self.__train_model() 
        self.__load_model()
