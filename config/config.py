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
    THRESHOLDS = {
        'test_key': {'size': 10, 'age': 120},
        'test_key2': {'size': 10},
        'test_key3': {'age': 120}
        }
