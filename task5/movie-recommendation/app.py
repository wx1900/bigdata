# from __future__ import json
from flask import Blueprint
main = Blueprint('main', __name__)

import json
from engine import RecommendationEngine
 
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
 
from flask import Flask, request, render_template, flash
from wtforms import Form, TextField, TextAreaField, validators, StringField, SubmitField

import random 
import sys  
reload(sys)  
sys.setdefaultencoding('utf8')

@main.route("/<int:user_id>/ratings/top/<int:count>", methods=["GET"])
def top_ratings(user_id, count):
    logger.debug("User %s TOP ratings requested", user_id)
    top_ratings = recommendation_engine.get_top_ratings(user_id,count)
    return json.dumps(top_ratings)
 
@main.route("/<int:user_id>/ratings/<int:movie_id>", methods=["GET"])
def movie_ratings(user_id, movie_id):
    logger.debug("User %s rating requested for movie %s", user_id, movie_id)
    ratings = recommendation_engine.get_ratings_for_movie_ids(user_id, [movie_id])
    return json.dumps(ratings)
 
 
@main.route("/<int:user_id>/ratings", methods = ["POST"])
def add_ratings(user_id):
    # get the ratings from the Flask POST request object
    ratings_list = request.form.keys()[0].strip().split("\n")
    ratings_list = map(lambda x: x.split(","), ratings_list)
    # create a list with the format required by the negine (user_id, movie_id, rating)
    ratings = map(lambda x: (user_id, int(x[0]), float(x[1])), ratings_list)
    # add them to the model using then engine API
    recommendation_engine.add_ratings(ratings)
 
    return json.dumps(ratings)
 
@main.route("/", methods=["GET", "POST"])
def index():
    # top_list
    top_list = recommendation_engine.get_top_list(50)
    results = {'children':[]}
    for x in top_list:
        results['children'].append({
            'movie_id': x[0],
            'counts': x[1][0][0],
            'rating': round(x[1][0][1], 2),
            'title': x[1][1].encode("UTF-8")
            })
    # movies randomly choose
    movies = recommendation_engine.get_movies_ID_title()
    movies_encode = []
    for x in movies:
        movies_encode.append([x[0], x[1]])
    random_movies = random.sample(population=movies_encode, k=5)
    results_movies = []
    for x in random_movies:
        temp = {'m_id': x[0], 'm_title': x[1].encode("UTF-8"), 'm_rate': 3}
        results_movies.append(temp)
        # results_movies.update({x[0]: x[1].encode("UTF-8")})
    print("---------------------------------------------- random_movies ")
    print(results_movies)
    print("-------------------------------------------------------------")
    
    # get form content -- 1
    # form = ReusableForm(request.form)
    # if request.method == 'POST':
        # name = request.form["name"]
        # print name

    ## get multi form content -- 2
    new_ratings = []
    if request.method == 'POST':
        m_rates = request.form.getlist('m_rate')
        m_ids = request.form.getlist('m_id')
        for i in range(len(m_rates)):
            new_ratings.append((0, int(m_ids[i].encode("UTF-8")), int(m_rates[i].encode("UTF-8"))))
        recommendation_engine.add_ratings(new_ratings)
        predict_rating = recommendation_engine.get_top_ratings(0, 10)
        recommend = []
        for i in predict_rating:
            recommend.append(i[0].encode("UTF-8"))
        return render_template('main.html', data=results, movies=results_movies, recommend=recommend)
    else:
        return render_template('main.html', data=results, movies=results_movies)

class ReusableForm(Form):
    name = TextField('Name:', validators=[validators.required()])

# def index():

#     print(results_movies)
#     return render_template('main.html', movies=results_movies)

@main.route("/topk/<int:k>", methods = ["GET"])
def popular_movies(k):
    top_list = recommendation_engine.get_top_list(k)
    return json.dumps(top_list)

# @main.route("/", methods = ['POST'])
# def predict():
#     test = request.form["test"]
#     # r = requests.get(test)
#     print test
#     return render_template('predict.html', form=form)


def create_app(spark_context, dataset_path):
    global recommendation_engine 

    recommendation_engine = RecommendationEngine(spark_context, dataset_path)    
    
    app = Flask(__name__)
    app.register_blueprint(main)
    return app 
