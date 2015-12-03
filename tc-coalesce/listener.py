import traceback
import sys
import os
import json
import socket
import logging
import redis
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
        self._parse_args()

    def _parse_env(self):
        try:
            self.options['user'] = os.environ['PULSE_USER']
            self.options['passwd'] = os.environ['PULSE_PASSWD']
        except KeyError:
            traceback.print_exc()
            sys.exit(1)
        try:
            self.options['redis'] = urlparse(os.environ['REDIS_URL'])
        except KeyError:
            traceback.print_exc()
            sys.exit(1)

    def _parse_args(self):
        # TODO: parse args and return them as options
        pass


class TcPulseConsumer(GenericConsumer):
    def __init__(self, exchanges, **kwargs):
        super(TcPulseConsumer, self).__init__(
            PulseConfiguration(**kwargs), exchanges, **kwargs)

    def listen(self, callback=None, on_connect_callback=None):
        while True:
            consumer = self._build_consumer(
                callback=callback,
                on_connect_callback=on_connect_callback
            )
            with consumer:
                self._drain_events_loop()

    def _drain_events_loop(self):
        while True:
            try:
                self.connection.drain_events(timeout=self.timeout)
            except socket.timeout:
                logging.warning("Timeout! Restarting pulse consumer.")
                try:
                    self.disconnect()
                except Exception:
                    logging.warning("Problem with disconnect().")
                break


class TaskEventApp(object):

    # ampq/pulse listener
    listener = None

    # State transitions
    # pending --> running
    #         \-> exception
    exchanges = ['exchange/taskcluster-queue/v1/task-pending',
                 'exchange/taskcluster-queue/v1/task-running',
                 'exchange/taskcluster-queue/v1/task-exception']

    # TODO: move these to args and env options
    # TODO: make perm coalescer service pulse creds
    consumer_args = {
        'applabel': 'releng-tc-coalesce',
        'topic': ['#', '#', '#'],
        'durable': True,
        'user': 'public',
        'password': 'public'
    }

    options = None

    # Coalesing machine
    coalescer = None

    def __init__(self, options, stats, coalescer, datastore):
        self.options = options
        self.stats = stats
        self.coalescer = coalescer
        route_key = coalescer.get_route_key()
        self.consumer_args['topic'] = [route_key] * len(self.exchanges)
        self.rds = datastore
        self.consumer_args['user'] = self.options['user']
        self.consumer_args['password'] = self.options['passwd']
        self.listener = TcPulseConsumer(self.exchanges,
                                callback=self._route_callback_handler,
                                **self.consumer_args)

    def run(self):
        # TODO: bind with better topic to limit queue

        while True:
            try:
                self.listener.listen()
            except KeyboardInterrupt:
                # TODO: delete_queue doesn't work. fix me
                # self.listener.delete_queue()
                self.rds.flushdb()
                sys.exit(1)
            except:
                traceback.print_exc()


    def delete_queue(self):
        self._check_params()
        if not self.connection:
            self.connect()

        queue = self._create_queue()
        try:
            queue(self.connection).delete()
        except ChannelError as e:
            if e.message != 404:
                raise
        except:
            raise


    def _route_callback_handler(self, body, message):
        """
        Route call body and msg to proper callback handler
        """
        taskState = body['status']['state']
        taskId = body['status']['taskId']
        if taskState == 'pending':
            self._add_task_callback(taskId, body)
        elif taskState == 'running' or taskState == 'exception':
            self._remove_task_callback(taskId, body)
        else:
            raise StateError
        message.ack()
        self.stats.notch('total_msgs_handled')
        # DEBUG statement: please remove before release
        log.debug("taskId: %s (%s) - PendingTasks: %s" % (taskId, taskState,self.stats.get('pending_count')))


    def _add_task_callback(self, taskId, body):
        self.coalescer.insert_task(taskId, body)

    def _remove_task_callback(self, taskId, body):
        self.coalescer.remove_task(taskId, body)


def setup_log():
    # TODO: pass options and check for log level aka debug or not
    global log
    log = logging.getLogger(__name__)
    log.setLevel(logging.DEBUG)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    log.addHandler(console_handler)
    return log


def main():
    setup_log()
    options = Options().options
    log.info("Starting Coalescing Service")
    # TODO: parse args
    # TODO: pass args and options
    # setup redis object
    rds = redis.Redis(host=options['redis'].hostname,
                      port=options['redis'].port,
                      password=options['redis'].password)
    stats = Stats(datastore=rds)
    coalescer_machine = CoalescingMachine(datastore=rds, stats=stats)
    app = TaskEventApp(options, stats, coalescer_machine, datastore=rds)
    app.run()
    # graceful shutdown via SIGTERM

if __name__ == '__main__':
    main()
