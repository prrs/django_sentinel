__author__ = 'ravis'

from django_redis.redis_ha_client import RedisHA

class RedisReconnecting:
    '''
    This the the wrapper class to convert the RedisCache (django redis implementation) calls to RediHA
    (an implementation of redis sentinel for knowlus)
    '''

    def __init__(self, server, params):
        # Keeping server details for retaining the similar interface
        self.server = server
        self.params = params
        self.options = params.get('OPTIONS', {})
        self.redis = RedisHA(db=self.db())

    def db(self):
        db = self.params.get('db', self.options.get('DB', 1))
        try:
            db = int(db)
        except (ValueError, TypeError):
            raise ImproperlyConfigured("db value must be an integer")
        return db

    def __getattr__(self, name):
        return getattr(self.redis, name)
