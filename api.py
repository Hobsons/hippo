import os
import redis
import config
from flask import Flask, request, jsonify
from tasks import HippoTask
from queues import HippoQueue
from data_sources import *

app = Flask(__name__,static_url_path='')
app.config['TEMPLATES_AUTO_RELOAD'] = True
app.redis = redis.StrictRedis(host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.REDIS_DB, password=config.REDIS_PW)


@app.route('/',methods=['GET'])
def index():
    return app.send_static_file('index.html')


@app.route('/tasks/',methods=['GET','POST'])
def tasks():
    if request.method == 'POST':
        data = request.get_json()
        # create a new task
        t = HippoTask(definition=data,redis_client=app.redis)
        validation_error = t.validate()
        if validation_error:
            return jsonify({"error":validation_error}), 400
        t.queue()
        return jsonify({"mesos_id":t.mesos_id})
    else:
        tasks = HippoTask.all_tasks(redis_client=app.redis)

    return jsonify([t.definition for t in tasks])


@app.route('/tasks/<task_id>/',methods=['GET','DELETE'])
def single_task(task_id):
    t = HippoTask(mesos_id=task_id,redis_client=app.redis)
    if not t.definition:
        return jsonify({"error":task_id + " not found"}), 404

    if request.method == 'DELETE':
        t.delete()
        return jsonify({"deleted": task_id})
    else:
        t = HippoTask(mesos_id=task_id,redis_client=app.redis)
        return jsonify(t.definition)


@app.route('/tasks/<task_id>/kill/',methods=['GET','POST'])
def kill_task(task_id):
    t = HippoTask(mesos_id=task_id,redis_client=app.redis)
    if not t.definition:
        return jsonify({"error":task_id + " not found"}), 404
    t.kill()
    return jsonify({"killed": task_id})


@app.route('/queues/', methods=['GET','POST','PUT'])
def queues():
    if request.method in ['POST','PUT']:
        data = request.get_json()
        q = HippoQueue(definition=data,redis_client=app.redis)
        validation_error = q.validate()
        if validation_error:
            return jsonify({"error":validation_error}), 400
        return jsonify({"id":q.id})
    else:
        all_queues = HippoQueue.all_queues(app.redis)

    return jsonify([q.definition for q in all_queues])


@app.route('/queues/<queue_id>/',methods=['GET','POST','PUT','DELETE'])
def single_queue(queue_id):
    q = HippoQueue(id=queue_id,redis_client=app.redis)
    if not q.definition:
        return jsonify({"error":queue_id + " not found"}), 404

    if request.method == 'DELETE':
        q.delete()
        return jsonify({"deleted": queue_id})
    elif request.method in ['POST','PUT']:
        data = request.get_json()
        q.definition = data
        if 'id' not in q.definition:
            q.definition['id'] = q.id
        validation_error = q.validate()
        if validation_error:
            return jsonify({"error":validation_error}), 400
        q.save()

    return jsonify(q.definition)


@app.route('/queues/<queue_id>/<toggle>/',methods=['GET'])
def single_queue_enabgle_toggle(queue_id, toggle):
    q = HippoQueue(id=queue_id,redis_client=app.redis)
    if not q.definition:
        return jsonify({"error":queue_id + " not found"}), 404

    if toggle == 'enable':
        q.enable()
        return jsonify({"enabled": queue_id})
    else:
        q.disable()
        return jsonify({"disabled": queue_id})


@app.route('/queuetypes/',methods=['GET'])
def queue_types():
    qtlist = []
    processors = HippoDataSource.__subclasses__()
    for p in processors:
        qtlist.append({
            'namespace':p.namespace,
            'inputs':p.inputs
        })
    return jsonify(qtlist)

if __name__ == '__main__':
    app.run(debug=True, threaded=True)