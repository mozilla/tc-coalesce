import traceback
import sys
import os
import logging
import redis
import signal
from urlparse import urlparse

from stats import Stats
from coalescer import CoalescingMachine

from mozillapulse.config import PulseConfiguration
from mozillapulse.consumers import GenericConsumer


class StateError(Exception):
    pass


log = None


class Options(object):

    options = {}

    def __init__(self):
        self._parse_env()

    def _parse_env(self):
        try:
            self.options['user'] = os.environ['PULSE_USER']
            self.options['passwd'] = os.environ['PULSE_PASSWD']
            self.options['redis'] = urlparse(os.environ['REDIS_URL'])
        except KeyError:
            traceback.print_exc()
            sys.exit(1)


class TcPulseConsumer(GenericConsumer):
    def __init__(self, exchanges, **kwargs):
        super(TcPulseConsumer, self).__init__(
            PulseConfiguration(**kwargs), exchanges, **kwargs)


class TaskEventApp(object):

    # ampq/pulse listener
    listener = None

    # State transitions
    # pending --> running --> (completed|exception|failed)
    #         \-> exception
    exchanges = ['exchange/taskcluster-queue/v1/task-pending',
                 'exchange/taskcluster-queue/v1/task-completed',
                 'exchange/taskcluster-queue/v1/task-exception',
                 'exchange/taskcluster-queue/v1/task-failed']

    # TODO: move these to args and env options
    # TODO: make perm coalescer service pulse creds
    consumer_args = {
        'applabel': 'releng-tc-coalesce',
        'topic': ['#', '#', '#', '#'],
        'durable': True,
        'user': 'public',
        'password': 'public'
    }

    options = None

    # Coalesing machine
    coalescer = None

    def __init__(self, prefix, options, stats, datastore):
        self.prefix = prefix
        self.options = options
        self.stats = stats
        self.redis = datastore
        self.coalescer = CoalescingMachine(prefix,
                                           datastore,
                                           stats=stats)
        route_key = "route." + prefix + "#"
        self.consumer_args['topic'] = [route_key] * len(self.exchanges)
        self.consumer_args['user'] = self.options['user']
        self.consumer_args['password'] = self.options['passwd']
        log.info("Binding to queue with route key: %s" % (route_key))
        self.listener = TcPulseConsumer(self.exchanges,
                                        callback=self._route_callback_handler,
                                        **self.consumer_args)

    def run(self):
        while True:
            try:
                self.listener.listen()
            except KeyboardInterrupt:
                # Handle both SIGTERM and SIGINT
                self._graceful_shutdown()
            except:
                traceback.print_exc()

    def _graceful_shutdown(self):
        log.info("Gracefully shutting down")
        log.info("Deleting Pulse queue")
        self.listener.delete_queue()
        sys.exit(1)

    def _route_callback_handler(self, body, message):
        """
        Route call body and msg to proper callback handler
        """
        # Ignore tasks with non-zero runId (for now)
        if not body['runId'] == 0:
            message.ack()
            return

        taskState = body['status']['state']
        taskId = body['status']['taskId']
        # Extract first coalesce key that matches
        for route in message.headers['CC']:
            route = route[6:]
            if self.prefix == route[:len(self.prefix)]:
                coalesce_key = route[len(self.prefix):]
                break
        if taskState == 'pending':
            self.coalescer.insert_task(taskId, coalesce_key)
        elif taskState == 'completed' or \
                taskState == 'exception' or \
                taskState == 'failed':
            self.coalescer.remove_task(taskId, coalesce_key)
        else:
            raise StateError
        message.ack()
        self.stats.notch('total_msgs_handled')
        log.debug("taskId: %s (%s)" % (taskId, taskState))


def setup_log():
    global log
    log = logging.getLogger(__name__)
    lvl = logging.DEBUG if os.getenv('DEBUG') == 'True' else logging.INFO
    log.setLevel(lvl)
    console_handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('[%(asctime)s] [%(process)d]' +
                                  '[%(levelname)s] %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S +0000')
    console_handler.setFormatter(formatter)
    log.addHandler(console_handler)
    return log


def main():
    setup_log()
    options = Options().options
    log.info("Starting Coalescing Service")

    # prefix for all redis keys and route key
    prefix = "coalesce.v1."

    # setup redis object
    rds = redis.Redis(host=options['redis'].hostname,
                      port=options['redis'].port,
                      password=options['redis'].password)
    stats = Stats(prefix, datastore=rds)
    app = TaskEventApp(prefix, options, stats, datastore=rds)
    signal.signal(signal.SIGTERM, signal_term_handler)
    app.run()
    # graceful shutdown via SIGTERM


def signal_term_handler(signal, frame):
    log.info("Handling signal: term")
    raise KeyboardInterrupt


if __name__ == '__main__':
    main()
