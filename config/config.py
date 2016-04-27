class Config(object):
    # All variables must be UPPERCASE
    DEBUG = False
    TESTING = False
    REDIS_URL = "redis://localhost:6379"
    PREFIX = "coalesce.v1."
    THRESHOLDS = {}


class Production(Config):
    # TODO: Set actual threshold settings for production keys
    pass


class Development(Config):
    DEBUG = True


class Testing(Config):
    TESTING = True
