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

    def __init__(self, datastore, stats, **kwargs):
        self.rds = datastore
        self.stats = stats

    def insert_task(self, taskId, body, *args, **kwargs):
        # TODO: handle if task already exists
        # TODO: store msg timestamp in pendingTasks
        if taskId in self.pendingTasks:
            self.stats.notch('tasks_reran')
        taskDef = self._retrieve_taskdef(taskId)
        self.pendingTasks[taskId] = { 'task_msg': body, 'task_def': taskDef }
        self.rds.sadd(self.rds_prefix + 'pendingTasks', taskId)
        for route in taskDef['routes']:
            if self.rds_prefix == route[:len(self.rds_prefix)]:
                coalescekey = route[len(self.rds_prefix):]
                self.rds.sadd(self.rds_prefix + "list_keys", coalescekey)
                self.rds.rpush(self.rds_prefix + 'lists.' + coalescekey, taskId)
        self.stats.set('pending_count', len(self.pendingTasks))

    def remove_task(self, taskId, *args, **kwargs):
        try:
            taskDef = self.pendingTasks[taskId]['task_def']
        except KeyError:
            self.stats.notch('unknown_tasks')
            return
        for route in taskDef['routes']:
            if self.rds_prefix == route[:len(self.rds_prefix)]:
                coalescekey = route[len(self.rds_prefix):]
                self.rds.lrem(self.rds_prefix + 'lists.' + coalescekey,
                              taskId, num=0)
                if self.rds.llen(self.rds_prefix + 'lists.' + coalescekey) == 0:
                    self.rds.srem(self.rds_prefix + "list_keys", coalescekey)


        del self.pendingTasks[taskId]
        self.rds.srem(self.rds_prefix + 'pendingTasks', taskId)
        self.stats.set('pending_count', len(self.pendingTasks))

    def _retrieve_taskdef(self, taskId):
        # TODO: retry api call
        # DEBUG: api call disabled
        taskDef = { 'routes': [self.rds_prefix +'somefakekey'] }
        # TODO: validate response
        # DEBUG statement: please remove before release
        return taskDef
