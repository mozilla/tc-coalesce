import traceback
import sys
import os
import json
import socket
import logging

from stats import Stats
from coalescer import CoalescingMachine

from mozillapulse.config import PulseConfiguration
from mozillapulse.consumers import GenericConsumer


class AuthenticationError(Exception):
    pass

class StateError(Exception):
    pass

log = None

def debug_print(msg):
    log.debug(msg)
    # running in heroku causes stdio to be buffered therefore we flush
    sys.stdout.flush()


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
            raise AuthenticationError
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

    routing_keys = []

    # TODO: move these to args and env options
    # TODO: make perm coalescer service pulse creds
    consumer_args = {
        'applabel': 'jwatkins@mozilla.com|pulse-test2',
        'topic': ['#', '#', '#'],
        'durable': True,
        'user': 'public',
        'password': 'public'
    }

    options = None

    # Coalesing machine
    coalescer = None

    def __init__(self, options, stats, coalescer):
        self.options = options
        self.stats = stats
        self.coalescer = coalescer
        self.routing_keys = self.coalescer.get_routing_keys()
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
        debug_print("taskId: %s (%s) - PendingTasks: %s" % (taskId, taskState, self.stats.get('pending_count')))


    def _add_task_callback(self, taskId, body):
        self.coalescer.insert_task(taskId, body)

    def _remove_task_callback(self, taskId, body):
        self.coalescer.remove_task(taskId, body)

    def _spawn_taskdef_worker(self, taskId):
        """
        Spawn an async worker thread to fetch taskdef and fill in pendingTasks
        value. Once worker has taskdef, Coalescer object is applied to possible
        add taskId to a current 'defined commonality list' already indexed or
        to build a new list
        """
        pass

    def _spawn_listener_worker(self, taskId):
        """
        Spawn an async worker thread to listen to pulse exchanges and add keys
        to dict
        """
        pass

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
    options = Options()
    setup_log()
    # DEBUG statement: please remove before release
    debug_print("Starting")
    # TODO: parse args
    # TODO: parse and load evn options
    # TODO: pass args and options
    stats = Stats()
    coalescer_machine = CoalescingMachine(stats)
    app = TaskEventApp(options.options, stats, coalescer_machine)
    app.run()
    # graceful shutdown via SIGTERM

if __name__ == '__main__':
    main()
