class CoalescingMachine(object):
    """
    Generic Coalescer object contains logic to build lists of tasks based on
    defined commonality eg. ProvisionerId, WorkerType, TaskDef env
    These 'defined commonalities' will be the index key used to quickly
    retrieve lists via the wsgi REST api multiple objects may be defined
    accommodate multiple 'defined commonalities'
    """

    prefix = "default."

    def __init__(self, prefix, datastore, stats):
        self.prefix = prefix
        self.redis = datastore
        self.stats = stats

    def insert_task(self, taskId, coalesce_key):
        self.redis.sadd(self.prefix + "list_keys", coalesce_key)
        self.redis.lpush(self.prefix + "lists." + coalesce_key, taskId)
        self.stats.set('coalesced_lists',
                       self.redis.scard(self.prefix + "list_keys"))

    def remove_task(self, taskId, coalesce_key):
        self.redis.lrem(self.prefix + 'lists.' + coalesce_key, taskId, 0)
        if self.redis.llen(self.prefix + 'lists.' + coalesce_key) == 0:
            self.redis.srem(self.prefix + "list_keys", coalesce_key)
            self.stats.set('coalesced_lists',
                           self.redis.scard(self.prefix + "list_keys"))
