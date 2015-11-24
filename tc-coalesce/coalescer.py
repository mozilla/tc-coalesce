import json
import stat


class CoalescingMachine(object):
    """
    Generic Coalescer object contains logic to build lists of tasks based on
    defined commonality eg. ProvisionerId, WorkerType, TaskDef env
    These 'defined commonalities' will be the index key used to quickly
    retrieve lists via the wsgi REST api multiple objects may be defined
    accommodate multiple 'defined commonalities'
    """

    # pendingTasks is a dict where all pending tasks are kept along with the
    # received amqp msg and taskdef. Tasks are added or removed based on msgs
    # from task-pending, task-running, and task-exception
    # {'taskId': {'task_def': 'def', 'task_msg': 'boby'}}
    pendingTasks = {}

    coalesced_lists = {}

    routing_keys = set()

    msg_aggregator = None

    def __init__(self, stats, **kwargs):
        self.stats = stats
        self.msg_aggregator = self._make_msg_aggregator()


    def insert_task(self, taskId, body, *args, **kwargs):
        # TODO: handle if task already exists
        # TODO: store msg timestamp in pendingTasks
        if taskId in self.pendingTasks:
            self.stats.notch('tasks_reran')
        taskDef = self._retrieve_taskdef(taskId)
        self.pendingTasks[taskId] = { 'task_msg': body, 'task_def': taskDef }

        # TODO: iterate over msg aggregator
        self.msg_aggregator.entry(taskId, self.pendingTasks[taskId])

        self.stats.set('pending_count', len(self.pendingTasks))
        self._write_db()

    def remove_task(self, taskId, *args, **kwargs):
        try:
            task_data = self.pendingTasks[taskId]
            # TODO: apply task to coalesce filters for extraction
        except KeyError:
            self.stats.notch('unknown_tasks')
            return
            # DEBUG: these are for debugging only.  Remove before release
        self.msg_aggregator.exit(taskId, self.pendingTasks[taskId])
        del self.pendingTasks[taskId]
        self.stats.set('pending_count', len(self.pendingTasks))
        self._write_db()


    def _retrieve_taskdef(self, taskId):
        # TODO: retry api call
        # DEBUG: api call disabled
        taskDef = "TaskDef goes here"
        # TODO: validate response
        # DEBUG statement: please remove before release
        return taskDef

    def _write_db(self):
        db = {'stats': self.stats.dump(),
              'pendingTasks': [taskId for taskId in self.pendingTasks.keys()],
              'coalesce_list': self.coalesced_lists
        }
        with open('data.txt', 'w') as outfile:
            json.dump(db, outfile)

    def _make_msg_aggregator(self):
        # TODO: load multiple coalesce objects and return an iterator
        msg_aggregator = RelengInboundBuild(self.coalesced_lists)
        self.routing_keys = self.routing_keys | msg_aggregator.routing_keys
        return msg_aggregator

    def get_routing_keys(self):
        return [keys for keys in self.routing_keys]

class CoalesceObj(object):

    # This should always be overridden
    routing_keys = {'#'}

    filter_list = []

    def entry(self, taskId, task_data):
        for filter_func in self.filter_list:
            list_key = filter_func(taskId, task_data)
            if list_key:
                try:
                    self.coalesced_lists[list_key].append(taskId)
                except KeyError:
                    self.coalesced_lists[list_key] = []
                    self.coalesced_lists[list_key].append(taskId)
                break

    def exit(self, taskId, task_data):
        for filter_func in self.filter_list:
            list_key = filter_func(taskId, task_data)
            if list_key:
                try:
                    self.coalesced_lists[list_key].remove(taskId)
                except KeyError:
                    pass
                break

    def get_routing_keys(self):
        return self.routing_keys


class RelengInboundBuild(CoalesceObj):

    keys = ["aws-provisioner-v1.opt_linux32", "aws-provisioner-v1.opt_linux64",
            "aws-provisioner-v1.dbg-linux32", "aws-provisioner-v1.dbg-linux64"]

    routing_keys = {"index.gecko.v2.mozilla-inbound.revision.*.firefox.#"}

    def __init__(self, coalesced_lists):
        self.coalesced_lists = coalesced_lists
        self.filter_list = [self.opt_linux32, self.opt_linux64, self.dbg_linux32, self.dbg_linux64]
        for key in self.keys:
            self.coalesced_lists[key] = []

    def opt_linux32(self, taskId, task_data):
        key = "aws-provisioner-v1.opt-linux32"
        if self.provisionerId(taskId, task_data):
            return None
        if not task_data['task_msg']['status']['workerType'] == "opt_linux32":
            return None
        return key

    def opt_linux64(self, taskId, task_data):
        key = "aws-provisioner-v1.opt-linux64"
        if self.provisionerId(taskId, task_data):
            return None
        if not task_data['task_msg']['status']['workerType'] == "opt_linux64":
            return None
        return key

    def dbg_linux32(self, taskId, task_data):
        key = "aws-provisioner-v1.dbg-linux32"
        if self.provisionerId(taskId, task_data):
            return None
        if not task_data['task_msg']['status']['workerType'] == "dbg_linux32":
            return None
        return key

    def dbg_linux64(self, taskId, task_data):
        key = "aws-provisioner-v1.dbg-linux64"
        if self.provisionerId(taskId, task_data):
            return None
        if not task_data['task_msg']['status']['workerType'] == "dbg_linux64":
            return None
        return key

    def provisionerId(self, taskId, task_data):
        return not task_data['task_msg']['status']['provisionerId'] == "aws-provisioner-v1"



