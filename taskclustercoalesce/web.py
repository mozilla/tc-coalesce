import sys
import os
import flask
import time
import redis
import logging
from flask import jsonify
from urlparse import urlparse
from werkzeug.contrib.fixers import ProxyFix
from flask_sslify import SSLify

starttime = time.time()

app = flask.Flask(__name__)

if 'DYNO' in os.environ:
    app.wsgi_app = ProxyFix(app.wsgi_app)
    SSLify(app, age=300, permanent=True)


def load_config(app):
    # Set basic defaults
    app.config.update(dict(
        REDIS_URL="redis://localhost:6379",
        PREFIX="coalesce.v1.",
        DEBUG=False))

    # Valid environment types are Production, Testing, Development (Default)
    environment_type = os.getenv('ENVIRONMENT_TYPE', 'Development')
    app.logger.info('ENVIRONMENT_TYPE set to {0}'.format(environment_type))
    app.config.from_object('config.config.{0}'.format(environment_type))

    # Override with environment vars if they exist
    app.config.from_envvar('REDIS_URL', silent=True)
    app.config.from_envvar('PREFIX', silent=True)
    app.config.from_envvar('DEBUG', silent=True)
    return app


def connect_redis(app):
    redis_url = urlparse(app.config['REDIS_URL'])
    app.redis = redis.Redis(host=redis_url.hostname,
                            port=redis_url.port,
                            password=redis_url.password,
                            decode_responses=True)
    return app


def set_prefix(app):
    app.prefix = app.config['PREFIX']
    return app


# Setup application
app = load_config(app)
app = connect_redis(app)
app = set_prefix(app)

app.logger.addHandler(logging.StreamHandler(sys.stdout))
lvl = logging.DEBUG if app.config['DEBUG'] == 'True' else logging.INFO
app.logger.setLevel(lvl)


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
    return jsonify(ping)


@app.route('/v1/list')
def coalasce_lists():
    """
    GET: returns a list of all coalesced objects load into the listener
    """
    list_keys_set = app.redis.smembers(app.prefix + "list_keys")
    if len(list_keys_set) == 0:
        return jsonify({app.prefix: []})
    list_keys = [x for x in list_keys_set]
    return jsonify({app.prefix: list_keys})


@app.route('/v1/stats')
def stats():
    """
    GET: returns stats
    """
    prefix_key = app.prefix + 'stats'
    stats = app.redis.hgetall(prefix_key)
    return flask.jsonify(stats)


@app.route('/v1/list/<key>')
def list(key):
    """
    GET: returns a list of ordered taskIds associated with the key provided
    """

    prefix_key = app.prefix + 'lists.' + key
    empty_resp = jsonify({'supersedes': []})
    coalesced_list = app.redis.lrange(prefix_key, 0, -1)

    # Return empty resp if list is empty
    if len(coalesced_list) == 0:
        return empty_resp

    # Get threshold settings for key if key exists otherwise return empty resp
    if app.config['THRESHOLDS'].get(key):
        threshold_age = app.config['THRESHOLDS'][key].get('age')
        threshold_size = app.config['THRESHOLDS'][key].get('size')
    else:
        app.logger.warning(
            "Key '{0}' does not exist in threshold settings".format(key))
        return empty_resp

    # Return empty resp if either age or size threshold are not defined
    if threshold_age is None or threshold_size is None:
        app.logger.warning(
            "Key '{0}' is missing one or more threshold settings".format(key))
        return empty_resp

    # Return empty resp if taskid list is
    # less than or equal to the size threshold
    if len(coalesced_list) <= threshold_size:
        app.logger.debug("List does not meet size threshold")
        return empty_resp

    # Get age of oldest taskid in the list
    oldest_task_age = app.redis.get(app.prefix +
                                    coalesced_list[-1] + '.timestamp')

    # Return empty resp if age of the oldest taskid in list is
    # less than or equal to the age threshold
    if (time.time() - float(oldest_task_age)) <= threshold_age:
        app.logger.debug("Oldest task in list does not meet age threshold")
        return empty_resp

    # Thresholds have been exceeded. Return list for coalescing
    return jsonify({'supersedes': coalesced_list})


@app.route('/v1/threshold/<key>')
def threshold(key):
    """
    GET: Returns an object containing the age and size threshold setting for
    the key provided. Returns 200 on success, 404 if key does not exist
    """
    if app.config['THRESHOLDS'].get(key):
        # Age and/or size return None is unset but key exists
        age = app.config['THRESHOLDS'][key].get('age')
        size = app.config['THRESHOLDS'][key].get('size')
        return jsonify({key: {'age': age, 'size': size}})
    return action_response('get_threshold', False, 404)


@app.route('/v1/threshold')
def list_thresholds():
    """
    GET: Returns an object containing all keys and their associated thresholds.
    Returns 200 on success
    """
    return jsonify(app.config['THRESHOLDS'])


def action_response(action, success=True, status_code=200):
    """ Returns a stock json response """
    resp = jsonify({'action': action, 'success': success})
    resp.status_code = status_code
    return resp


if __name__ == '__main__':
    # TODO: remove debug arg
    app.run(host='0.0.0.0', debug=False)
