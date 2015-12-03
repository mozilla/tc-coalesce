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

    rds_prefix = "coalescer.v1."

    route_key = "gecko.v2.mozilla-inbound.latest.firefox.#"

    def __init__(self, datastore, stats, **kwargs):
        self.rds = datastore
        self.stats = stats

    def insert_task(self, taskId, body, *args, **kwargs):
        # TODO: handle if task already exists
        # TODO: store msg timestamp in pendingTasks
        if taskId in self.pendingTasks:
            self.stats.notch('tasks_reran')
        taskDef = self._retrieve_taskdef(taskId)

        # Insert task in to a master list for pending tasks
        self.pendingTasks[taskId] = { 'task_msg': body, 'task_def': taskDef }
        self.rds.sadd(self.rds_prefix + 'pendingTasks', taskId)

        coalesce_key = self._get_coalesce_key(taskId)
        self.rds.sadd(self.rds_prefix + "list_keys", coalesce_key)
        self.rds.rpush(self.rds_prefix + 'lists.' + coalesce_key, taskId)
        self.stats.set('pending_count', len(self.pendingTasks))

    def remove_task(self, taskId, *args, **kwargs):
        try:
            taskDef = self.pendingTasks[taskId]['task_def']
        except KeyError:
            self.stats.notch('unknown_tasks')
            return

        coalesce_key = self._get_coalesce_key(taskId)
        self.rds.lrem(self.rds_prefix + 'lists.' + coalesce_key, taskId, num=0)
        if self.rds.llen(self.rds_prefix + 'lists.' + coalesce_key) == 0:
            self.rds.srem(self.rds_prefix + "list_keys", coalesce_key)

        del self.pendingTasks[taskId]
        self.rds.srem(self.rds_prefix + 'pendingTasks', taskId)
        self.stats.set('pending_count', len(self.pendingTasks))

    def _get_coalesce_key(self, taskId):
        provisionerId = self.pendingTasks[taskId]['task_msg']['status']['provisionerId']
        workerType = self.pendingTasks[taskId]['task_msg']['status']['workerType']
        coalesce_key = "%s.%s" % (provisionerId, workerType)
        return coalesce_key

    def _parse_routes(self, taskId):
        # TODO: uses this to get coalescing key when coalesce key in routes
        taskDef = self.pendingTasks[taskId]['task_def']
        for route in taskDef['routes']:
            if self.rds_prefix == route[:len(self.rds_prefix)]:
                coalesce_key = route[len(self.rds_prefix):]
                break
        return coalesce_key

    def _retrieve_taskdef(self, taskId):
        # TODO: retry api call
        # DEBUG: api call disabled
        taskDef = { 'routes': [self.rds_prefix +'somefakekey'] }
        # TODO: validate response
        # DEBUG statement: please remove before release
        return taskDef

    def get_route_key(self):
        # TODO: handle providing multiple route keys
        return self.route_key
