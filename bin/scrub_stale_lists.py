#!/usr/bin/env python

import sys
import os
import time
import redis
import requests
import logging
from urlparse import urlparse
from datetime import timedelta


def main(rds):
    pf = "coalesce.v1."

    tasks_removed = 0
    lists_removed = 0

    list_keys = rds.smembers(pf + "list_keys")
    for key in list_keys:
        logging.debug("Inspecting list: " + pf + key)
        coalesce_list = rds.lrange(pf + "lists." + key, start=0, end=-1)
        for taskId in coalesce_list:
            logging.debug(" - inspecting task: " + taskId)
            if not is_pending(taskId):
                logging.debug("Removing stale task: " + taskId)
                rds.lrem(pf + 'lists.' + key, taskId, num=0)
                tasks_removed += 1
        if not rds.llen(pf + "lists." + key):
            logging.debug("Removing stale list key: " + key)
            rds.srem(pf + "list_keys", key)
            lists_removed += 1

    return tasks_removed, lists_removed

def is_pending(taskId):
    url = 'https://queue.taskcluster.net/v1/task/%s/status' % (taskId)
    try:
        r = requests.get(url, timeout=3)
        if r.status_code == 404:
            logging.debug("Queue service returned 404 for task: " + taskId)
            return False
        if not r.json()['status']['state'] == 'pending':
            return False
    except:
        logging.debug("Failed to get status")
    return True

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                        level=logging.DEBUG)

    try:
        redis_url = urlparse(os.environ['REDIS_URL'])
    except KeyError:
        logging.exception("Missing REDIS_URL env variable")
        sys.exit(1)

    rds = redis.Redis(host=redis_url.hostname,
                      port=redis_url.port,
                      password=redis_url.password)

    try:
        start = time.time()
        logging.info("Starting scrub task")

        tasks_removed, lists_removed = main(rds)
        elapsed = time.time() - start
        logging.info("Completed scrub task in %s" % (str(timedelta(seconds=elapsed))))
        logging.info("Removed %s lists and %s tasks" % (tasks_removed, lists_removed))
    except Exception:
        logging.exception("Fatal error in main loop")
