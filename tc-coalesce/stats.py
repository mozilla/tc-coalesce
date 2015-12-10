
class Stats(object):

    pf = "default.stats"

    # stats is a dict where running statistical data is stored to be available
    # via the api
    stats = {'pending_count': 0,   # number of pending tasks
             'coalesced_lists': 0, # number of coalesced lists
             'unknown_tasks': 0,   # number of tasks seen missing from pending
             'premature': 0,       # number of premature msgs
             'total_msgs_handled': 0
    }

    def __init__(self, redis_prefix, datastore):
        self.pf = redis_prefix + "stats"
        self.rds = datastore
        h_keys = self.rds.hkeys(self.pf)
        for key in self.stats.keys():
            if key in h_keys:
                self.stats[key] = self.rds.hget(self.pf, key)
            else:
                self.rds.hset(self.pf, key, self.stats[key])

    def notch(self, counter):
        self.stats[counter] += 1
        self.rds.hset(self.pf, counter, self.stats[counter])

    def get(self, stat_name):
        return self.stats[stat_name]

    def set(self, stat_name, stat):
        self.stats[stat_name] = stat
        self.rds.hset(self.pf, stat_name, stat)

    def dump(self):
        return self.stats
