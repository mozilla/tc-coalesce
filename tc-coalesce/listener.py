import traceback
import sys
import os
import json
import socket
import asyncio
import aiohttp

from mozillapulse.config import PulseConfiguration
from mozillapulse.consumers import GenericConsumer


class AuthenticationError(Exception):
    pass


def debug_print(msg):
    print(msg)
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
            # DEBUG statement: please remove before release
            debug_print("Auth error")
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

    # pendingTasks is a dict where all pending tasks are kept along with the
    # received amqp msg and taskdef. Tasks are added or removed based on msgs
    # from task-pending, task-running, and task-exception
    # {'taskId': {'task_def': 'def', 'task_msg': 'boby'}}
    pendingTasks = {}

    # ampq/pulse listener
    listener = None

    # stats is a dict where running statistical data is stored to be available
    # via the api
    stats = {'pendingTasks': 0,    # number of pending tasks
             'coalesced_lists': 0, # number of coalesced lists
             'unknown_tasks': 0,   # number of tasks seen missing from pending
             'tasks_reran': 0      # number of tasks sent back to pending
    }

    # DEBUG: these are for debugging only.  Remove before release
    task_unknown_state = {}
    task_missing = []

    # State transitions
    # pending --> running
    #         \-> exception
    exchanges = ['exchange/taskcluster-queue/v1/task-pending',
                 'exchange/taskcluster-queue/v1/task-running',
                 'exchange/taskcluster-queue/v1/task-exception']

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

    coalesce_list = []
    # coalesing machine
    coalescer = None

    def __init__(self, options, coalease_list):
        self.options = options
        self.coalease_list = coalease_list
        self.consumer_args['user'] = self.options['user']
        self.consumer_args['password'] = self.options['passwd']
        self.listener = TcPulseConsumer(self.exchanges,
                                callback=self._route_callback_handler,
                                **self.consumer_args)
        self.coalescer = self._build_coalescers(self.coalease_list)

    def run(self):
        # TODO: bind with better topic to limit queue
        # DEBUG statement: please remove before release
        debug_print(self.consumer_args)

        while True:
            try:
                self.listener.listen()
            except KeyboardInterrupt:
                # TODO: delete_queue doesn't work. fix me
                # self.listener.delete_queue()
                sys.exit(1)
            except:
                traceback.print_exc()

    def _route_callback_handler(self, body, message):
        """
        Route call body and msg to proper callback handler
        """
        taskState = body['status']['state']
        taskId = body['status']['taskId']
        # DEBUG statement: please remove before release
        # print json.dumps(body, sort_keys=True, indent=4)
        if taskState == 'pending':
            self._add_task_callback(body, message, taskId)
        elif taskState == 'running' or taskState == 'exception':
            self._remove_task_callback(body, message, taskId)
        else:
            # TODO: Handle unknown states; for now just ack the msg
            # DEBUG: these are for debugging only.  Remove before release
            self.task_unknown_state[taskId] = taskState
        # DEBUG statement: please remove before release
        # debug_print("PendingTasks: %s" % (len(self.pendingTasks)))
        message.ack()
        self.stats['pendingTasks'] = len(self.pendingTasks)
        # DEBUG statement: please remove before release
        debug_print("taskId: %s (%s) - PendingTasks: %s" % (taskId, taskState, len(self.pendingTasks)))
        self._write_db()

    def _add_task_callback(self, body, message, taskId):
        # Insert taskId into pendingTask with None as place holder
        # TODO: handle if task already exists
        taskDef = self._retrieve_taskdef(taskId)
        # TODO: store msg timestamp in pendingTasks
        if taskId in self.pendingTasks:
            self.stats['tasks_reran'] += 1
        self.pendingTasks[taskId] = { 'task_msg': body, 'task_def': taskDef }
        # TODO: apply task to coalesce filters for insertion
        # self.coalescer(taskId)

    def _remove_task_callback(self, body, message, taskId):
        # TODO: make idempotent; handle KeyError
        try:
            del self.pendingTasks[taskId]
            # TODO: apply task to coalesce filters for extraction
        except:
            self.stats['unknown_tasks'] += 1
            # DEBUG: these are for debugging only.  Remove before release
            self.task_missing.append(taskId)

    def _retrieve_taskdef(self, taskId):
        # TODO: retry api call
        # DEBUG: api call disabled
        taskDef = "TaskDef goes here"
        # TODO: validate response
        # DEBUG statement: please remove before release
        return taskDef

    def _build_coalescers(self, coalease_list=None):
        """
        Import multiple coalescing objects
        """
        pass


    def _write_db(self):
        db = {'stats': self.stats,
              'pendingTasks': self.pendingTasks,
              'task_missing': self.task_missing,
              'task_unknown_state': self.task_unknown_state,
              'coalesce_list': self.coalesce_list
        }
        with open('data.txt', 'w') as outfile:
            json.dump(db, outfile)


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


class CoalescerGeneric(object):
    """
    Generic Coalescer object contains logic to build lists of tasks based on
    defined commonality eg. ProvisionerId, WorkerType, TaskDef env
    These 'defined commonalities' will be the index key used to quickly
    retrieve lists via the wsgi REST api multiple objects may be defined
    accommodate mulitple 'defined commonalities'
    """

    def __init__(self):
        pass


class TestCoalescer(CoalescerGeneric):
    """
    A simple test Coalescer object
    """

    # The coalesce_def contains the attributes for tasks to be matched up
    # primary
    #     .taskId
    #         .runId
    #             .workerGroup
    #                 .workerId
    #                     .provisionerId
    #                         .workerType
    #                             .schedulerId
    #                                 .taskGroupId
    #                                     .reserved
    coalesce_def = {'topic_route': 'primary.'}

    def __init__(self):
        pass


def logging():
    # TODO: Setup logging facility to be compatible with heroku
    pass


def main():
    options = Options()
    # DEBUG statement: please remove before release
    debug_print("Starting")
    # TODO: parse args
    # TODO: parse and load evn options
    # TODO: pass args and options
    coalesce_obj_list = [TestCoalescer]
    app = TaskEventApp(options.options, coalesce_obj_list)
    app.run()
    # graceful shutdown via SIGTERM

if __name__ == '__main__':
    main()
