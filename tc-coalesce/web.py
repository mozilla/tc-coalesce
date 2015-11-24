import flask
from flask_aiohttp import AioHTTP
from flask_aiohttp.helper import async, websocket
import asyncio
import aiohttp
import json

app = flask.Flask(__name__)
aio = AioHTTP(app)



@app.route('/v1')
def root():
    """
    GET: Return an index of available api
    """
    # TODO: return an index on available api
    return 'Index goes here'

@app.route('/v1/coalesce')
@async
def coalasce_lists():
    """
    GET: returns a list of all coalesced objects load into the listener
    """
    data = _get_db()
    ddata = json.loads(data)
    stats = { 'coalesce_list': ddata['coalesce_list'] }
    return flask.jsonify(**stats)

@app.route('/v1/stats')
@async
def stats():
    """
    GET: returns stats
    """
    data = _get_db()
    ddata = json.loads(data)
    stats = ddata['stats']
    return flask.jsonify(**stats)

@app.route('/v1/list/<provisionerid>/<workertype>')
@async
def list(provisionerid, workertype, buildtype):
    """
    GET: returns list
    """
    prefix = "%s.%s" % (provisionerid, workertype)
    data = _get_db()
    ddata = json.loads(data)
    try:
        taskId_list = { prefix: ddata['coalesce_list'][prefix] }
    except KeyError as e:
        flask.abort(404)
    return flask.jsonify(**taskId_list)

# DEBUG: remove before release
@app.route('/v1/pending_status')
@async
def pending_status():
    """
    GET: return list of pending tasks
    """
    data = json.loads(_get_db())
    pt = data['pendingTasks'].keys()
    return flask.jsonify(**{"pendingTasks": [x for x in pt]})

# DEBUG: remove before release
@app.route('/v1/db')
@async
def db():
    """
    GET: return entire datastore json (for debugging only)
    """
    data = _get_db()
    return flask.jsonify(**json.loads(data))

def _get_db():
    """
    Retrieve simple json datastore.  To be replaced with a real inter dyno
    data share method
    """
    with open('data.txt', 'r') as f:
        data = f.read()
    f.close()
    return data


if __name__ == '__main__':
    # TODO: remove debug arg
    aio.run(app, host='0.0.0.0', debug=True)
