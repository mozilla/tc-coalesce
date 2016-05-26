class Config(object):
    # All variables must be UPPERCASE
    DEBUG = False
    TESTING = False
    REDIS_URL = "redis://localhost:6379"
    PREFIX = "coalesce.v1."
    THRESHOLDS = {}


class Production(Config):
    THRESHOLDS = {
        'builds.mozilla-inbound.linux64-pgo': {
            'size': 5,
            'age': 900
        }
    }


class Development(Config):
    DEBUG = True


class Testing(Config):
    TESTING = True
