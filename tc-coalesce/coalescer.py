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
        coalesce_key = self.pending_tasks[taskId]['coalesce_key']
        self.rds.sadd(self.pf + "list_keys", coalesce_key)
        self.rds.lpush(self.pf + "lists." + coalesce_key, taskId)
        self.stats.set('coalesced_lists',
                       len(self.rds.llen(self.pf + "list_keys")))

    def remove_task(self, taskId):
        coalesce_key = self.pending_tasks[taskId]['coalesce_key']
        self.rds.lrem(self.pf + 'lists.' + coalesce_key, taskId, num=0)
        if self.rds.llen(self.pf + 'lists.' + coalesce_key) == 0:
            self.rds.srem(self.pf + "list_keys", coalesce_key)
            self.stats.set('coalesced_lists',
                           len(self.rds.llen(self.pf + "list_keys")))
