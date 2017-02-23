import os
import redis
import config
from flask import Flask, request, jsonify, render_template
from tasks import HippoTask

app = Flask(__name__)
app.redis = redis.StrictRedis(host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.REDIS_DB, password=config.REDIS_PW)


@app.route('/tasks/',methods=['GET','POST'])
def tasks():
    if request.method == 'POST':
        data = request.get_json()
        # create a new task
        t = HippoTask(definition=data,redis_client=app.redis)
        validation_error =  t.validate()
        if validation_error:
            return jsonify({"error":validation_error})
        t.save()
        t.queue()
        return jsonify({"mesos_id",t.mesos_id})
    else:
        tasks = HippoTask.all_tasks(redis_client=app.redis)

    return jsonify(tasks)


@app.route('/tasks/<task_id>/',methods=['GET','DELETE'])
def single_task(task_id):
    if request.method == 'DELETE':
        t = HippoTask(mesos_id=task_id,redis_client=app.redis)
        t.delete()
        return jsonify({"deleted": task_id})
    else:
        t = HippoTask(mesos_id=task_id,redis_client=app.redis)
        return jsonify(t.definition)


@app.route('/tasks/<task_id>/kill/',methods=['GET','POST'])
def kill_task(task_id):
    r = app.redis
    r.get('')
    return jsonify({"status": "ACTIVE"})



@app.route('/queues/')
def queues():
    if request.method == 'POST':
        pass
    else:
        r = app.redis
    return jsonify({"status": "ACTIVE"})


@app.route('/queues/<queue_id>/',methods=['GET','POST','PUT','DELETE'])
def single_queue(queue_id):
    if request.method == 'POST':
        pass
    else:
        r = app.redis
    return jsonify({"status": "ACTIVE"})


if __name__ == '__main__':
    app.run(debug=True, threaded=True)