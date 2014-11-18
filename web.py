
from flask import Flask
from flask import jsonify, request
from celery import Celery
from pymongo import MongoClient
from werkzeug.serving import run_simple
import arrow
import time

MONGO_URL = "mongodb://localhost:27019"
MONGO_DB = "adder"
BROKER_URL = "amqp://"

def connect_db(url):
    client = MongoClient(url)
    return client

def make_flask_app(name):
    global db

    app = Flask(name)
    app.config.from_object(__name__)
    
    app.config.update({
        "DATABASE_URL" : MONGO_URL,
        "DATABASE_NAME" : MONGO_DB,
    })

    mongoclient = connect_db(MONGO_URL)
    db = mongoclient[MONGO_DB]

    return app

def make_celery_app(name, flask_app):
    celery = Celery(flask_app.import_name, broker=BROKER_URL)
    celery.conf.update(flask_app.config)
    TaskBase = celery.Task
    return celery    


flask_app = make_flask_app(__name__)
celery_app = make_celery_app(__name__, flask_app)

@flask_app.route("/result", methods=["GET"])
def return_result():
    results = db.result.find({})
    results = [r for r in results]
    for r in results:
        del r["_id"]
    return jsonify({"results" : results})

@flask_app.route("/add", methods=["GET"])
def add_api():
    x = int(request.args.get('x'))
    y = int(request.args.get('y'))
    print("Adding to queue %d + %d" % (x, y))
    add.apply_async((x, y))
    return jsonify({"result" : "okay!"})

def insert_result(result):
    db.result.insert(result)

@celery_app.task(name="tasks.add")
def add(x, y):
    print("adding %d %d" % (x , y))
    time.sleep(10)
    insert_result({"time" : arrow.utcnow().to("+08:00").isoformat(), "arg1" : x , "arg2" : y, "result" : x+y })
    print("adding %d %d compelted" % (x , y))


if __name__ == "__main__":
    run_simple('0.0.0.0', 5001, flask_app, use_reloader=True)


