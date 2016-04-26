class Config(object):
    DEBUG = False
    TESTING = False
    THRESHOLDS = {}


class Production(Config):
    # TODO: Set actual threshold settings for production keys
    pass


class Development(Config):
    DEBUG = True


class Testing(Config):
    TESTING = True
