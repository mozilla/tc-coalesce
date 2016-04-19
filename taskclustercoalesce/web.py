import traceback
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
handler = logging.StreamHandler(sys.stdout)
app.logger.addHandler(handler)
lvl = logging.DEBUG if os.getenv('DEBUG') == 'True' else logging.INFO
app.logger.setLevel(lvl)

if 'DYNO' in os.environ:
    app.wsgi_app = ProxyFix(app.wsgi_app)
    SSLify(app, age=300, permanent=True)


def load_config(app):
    # Valid environment types are Production, Testing, Development (Default)
    environment_type = os.getenv('ENVIRONMENT_TYPE', 'Development')
    app.logger.info('ENVIRONMENT_TYPE set to {0}'.format(environment_type))
    app.config.from_object('config.config.{0}'.format(environment_type))
    return app

app = load_config(app)


pf = "coalesce.v1."

try:
    redis_url = urlparse(os.environ['REDIS_URL'])
except KeyError:
    traceback.print_exc()
    sys.exit(1)

rds = redis.Redis(host=redis_url.hostname,
                  port=redis_url.port,
                  password=redis_url.password,
                  decode_responses=True)


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
    list_keys_set = rds.smembers(pf + "list_keys")
    if len(list_keys_set) == 0:
        return jsonify(**{pf: []})
    list_keys = [x for x in list_keys_set]
    return jsonify(**{pf: list_keys})


@app.route('/v1/stats')
def stats():
    """
    GET: returns stats
    """
    pf_key = pf + 'stats'
    stats = rds.hgetall(pf_key)
    return flask.jsonify(**stats)


@app.route('/v1/list/<key>')
def list(key):
    """
    GET: returns a list of ordered taskIds associated with the key provided
    """
    pf_key = pf + 'lists.' + key
    empty_resp = jsonify(**{'supersedes': []})
    coalesced_list = rds.lrange(pf_key, start=0, end=-1)

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
    if not threshold_age or not threshold_size:
        app.logger.warning(
            "Key '{0}' is missing one or more threshold settings".format(key))
        return empty_resp

    # Return empty resp if taskid list is
    # less than or equal to the size threshold
    if len(coalasce_lists) <= threshold_size:
        return empty_resp

    # Get age of oldest taskid in the list
    oldest_task_age = rds.get(pf + coalesced_list[-1] + '.timestamp')

    # Return empty resp if age of the oldest taskid in list is
    # less than or equal to the age threshold
    if (time.time() - oldest_task_age) <= threshold_age:
        return empty_resp

    # Thresholds have been exceeded. Return list for coalescing
    return jsonify(**{'supersedes': coalesced_list})


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
        return jsonify(**{key: {'age': age, 'size': size}})
    return action_response('get_theshold', False, 404)


@app.route('/v1/threshold')
def list_thresholds():
    """
    GET: Returns an object containing all keys and their associated thresholds.
    Returns 200 on success
    """
    return jsonify(app.config['THRESHOLDS'])


def action_response(action, success=True, status_code=200):
    """ Returns a stock json response """
    resp = jsonify(**{'action': action, 'success': success})
    resp.status_code = status_code
    return resp


if __name__ == '__main__':
    # TODO: remove debug arg
    app.run(host='0.0.0.0', debug=False)
