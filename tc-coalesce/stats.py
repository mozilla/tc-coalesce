
class Stats(object):

    rds_pf = "coalescer.v1.stats"

    # stats is a dict where running statistical data is stored to be available
    # via the api
    stats = {'pending_count': 0,   # number of pending tasks
             'coalesced_lists': 0, # number of coalesced lists
             'unknown_tasks': 0,   # number of tasks seen missing from pending
             'tasks_reran': 0,     # number of tasks sent back to pending
             'premature': 0,       # number of premature msgs
             'total_msgs_handled': 0
    }

    def __init__(self, datastore):
        self.rds = datastore
        for key in self.stats.keys():
            self.rds.hset(self.rds_pf, key, self.stats[key])

    def notch(self, counter):
        self.stats[counter] += 1
        self.rds.hset(self.rds_pf, counter, self.stats[counter])

    def get(self, stat_name):
        return self.stats[stat_name]

    def set(self, stat_name, stat):
        self.stats[stat_name] = stat
        self.rds.hset(self.rds_pf, stat_name, stat)

    def dump(self):
        return self.stats
