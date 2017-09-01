class Config(object):
    # All variables must be UPPERCASE
    DEBUG = False
    TESTING = False
    REDIS_URL = "redis://localhost:6379"
    PREFIX = "coalesce.v1."


class Production(Config):
    pass


class Development(Config):
    DEBUG = True


class Testing(Config):
    TESTING = True
