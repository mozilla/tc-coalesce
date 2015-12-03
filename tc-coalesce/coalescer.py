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

    pf = "default."

    pending_tasks = None

    def __init__(self, pending_tasks, redis_prefix, datastore, stats):
        self.pending_tasks = pending_tasks
        self.pf = redis_prefix
        self.rds = datastore
        self.stats = stats

    def insert_task(self, taskId):
        coalesce_key = self._get_coalesce_key(taskId)
        self.rds.sadd(self.pf + "list_keys", coalesce_key)
        self.rds.rpush(self.pf + "lists." + coalesce_key, taskId)

    def remove_task(self, taskId):
        coalesce_key = self._get_coalesce_key(taskId)
        self.rds.lrem(self.pf + 'lists.' + coalesce_key, taskId, num=0)
        if self.rds.llen(self.pf + 'lists.' + coalesce_key) == 0:
            self.rds.srem(self.pf + "list_keys", coalesce_key)

    def _get_coalesce_key(self, taskId):
        return "fakekey"

    def _parse_routes(self, taskId):
        # TODO: uses this to get coalescing key when coalesce key in routes
        taskDef = self.pendingTasks[taskId]['task_def']
        for route in taskDef['routes']:
            if self.rds_prefix == route[:len(self.rds_prefix)]:
                coalesce_key = route[len(self.rds_prefix):]
                break
        return coalesce_key


