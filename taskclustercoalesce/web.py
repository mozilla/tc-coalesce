import sys
import os
import flask
import time
import redis
import logging
from flask import jsonify, g
from urlparse import urlparse
from werkzeug.contrib.fixers import ProxyFix
from flask_sslify import SSLify

starttime = time.time()

app = flask.Flask(__name__)

if 'DYNO' in os.environ:
    app.wsgi_app = ProxyFix(app.wsgi_app)
    SSLify(app, age=300, permanent=True)

app.config.update(dict(
    REDIS_URL="redis://localhost:6379",
    PREFIX="coalesce.v1.",
    DEBUG=False
))
app.config.from_envvar('REDIS_URL', silent=True)
app.config.from_envvar('PREFIX', silent=True)
app.config.from_envvar('DEBUG', silent=True)

app.logger.addHandler(logging.StreamHandler(sys.stdout))
lvl = logging.DEBUG if app.config['DEBUG'] == 'True' else logging.INFO
app.logger.setLevel(lvl)


def connect_rds():
    redis_url = urlparse(app.config['REDIS_URL'])
    rds = redis.Redis(host=redis_url.hostname,
                      port=redis_url.port,
                      password=redis_url.password,
                      decode_responses=True)
    return rds


@app.before_request
def before_request():
    if not hasattr(g, 'rds'):
        g.rds = connect_rds()
    if not hasattr(g, 'pf'):
        g.pf = app.config['PREFIX']


@app.route('/')
def root():
    """
    GET: Return an index of available api
    """
    # TODO: return an index on available api
    return jsonify({'versions': ['v1']})


@app.route('/v1/ping')
def ping():
    """ GET: return web process uptime """
    ping = {'alive': True, 'uptime': time.time() - starttime}
    return jsonify(**ping)


@app.route('/v1/list')
def coalasce_lists():
    """
    GET: returns a list of all coalesced objects load into the listener
    """
    list_keys_set = g.rds.smembers(g.pf + "list_keys")
    if len(list_keys_set) == 0:
        return jsonify(**{g.pf: []})
    list_keys = [x for x in list_keys_set]
    return jsonify(**{g.pf: list_keys})


@app.route('/v1/stats')
def stats():
    """
    GET: returns stats
    """
    pf_key = g.pf + 'stats'
    stats = g.rds.hgetall(pf_key)
    return flask.jsonify(**stats)


@app.route('/v1/list/<key>')
def list(key):
    """
    GET: returns a list of ordered taskIds associated with the key provided
    """
    pf_key = g.pf + 'lists.' + key
    coalesced_list = g.rds.lrange(pf_key, start=0, end=-1)
    return jsonify(**{'supersedes': coalesced_list})


if __name__ == '__main__':
    # TODO: remove debug arg
    app.run(host='0.0.0.0', debug=False)
