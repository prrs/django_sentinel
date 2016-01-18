__author__ = 'ravis'
import logging
import traceback
import pickle
import settings

from functools import wraps
from redis.sentinel import Sentinel
from redis.sentinel import MasterNotFoundError
from redis.sentinel import SlaveNotFoundError


class BaseCache(object):
    '''
        This class uses redis sentinel to establish a connection with redis.
        It ensures that all write operations goes to master instance and all read operations
        served by slave instances.

    '''
    # To support the *args, passed from ReConnectingCache
    def __init__(self, host='localhost', port=6379, db=0, password=None, socket_timeout=None):
        self.sentinel = None
        self.master = None
        self.selected_db = db
        self.slave = None
        self.socket_timeout = socket_timeout
        self.logger = logging.getLogger("django_sentinel")
        self.initialize()

    def initialize(self):
        try:
            self.sentinel = Sentinel(settings.LIST_OF_SENTINEL, self.socket_timeout)
            self.discover_master()
            self.discover_slave()
        except:
            self.logger.exception("Exception !! %s", traceback.format_exc())
            raise
        self.logger.info("RedisHA initialized, db is %d", self.selected_db)

    def get_master(self):
        return self.master

    def get_slave(self):
        return self.slave

    def discover_instance(func):
        '''
        It discover the new master/slave, if the current master or slave is down.
        '''
        @wraps(func)
        def inner(self, *args, **kwargs):
            try:
                res = func(self, *args, **kwargs)
            except SlaveNotFoundError:
                self.logger.exception("Exception : SlaveNotFoundError")
                self.discover_slave()
                res = func(self, *args, **kwargs)
            except MasterNotFoundError:
                self.logger.exception("Exception : MasterNotFoundError")
                self.discover_master()
                res = func(self, *args, **kwargs)
            except:
                self.logger.exception("Exception !! %s", traceback.format_exc())
                raise
            return res

        return inner

    @discover_instance
    def get(self, key):
        return self.slave.get(key)

    @discover_instance
    def set(self, key, value, expiry=7776000):
        return self.master.set(key, value, expiry)

    @discover_instance
    def delete(self, key):
        return self.master.delete(key)

    @discover_instance
    def keys(self):
        return self.slave.keys()

    @discover_instance
    def lpop(self, name):
        return self.master.lpop(name)

    @discover_instance
    def rpop(self, name):
        return self.master.rpop(name)

    @discover_instance
    def lpush(self, name, *values):
        return self.master.lpush(name, *values)

    @discover_instance
    def rpush(self, name, *values):
        return self.master.rpush(name, *values)

    @discover_instance
    def expire(self, name, time):
        return self.master.expire(name, time)

    @discover_instance
    def incr(self, name, amount=1):
        return self.master.incr(name, amount)

    @discover_instance
    def sadd(self, name, *values):
        return self.master.sadd(name, *values)

    @discover_instance
    def srem(self, name, *values):
        return self.master.srem(name, *values)

    @discover_instance
    def scard(self, name):
        return self.slave.scard(name)

    @discover_instance
    def smembers(self, name):
        return self.slave.smembers(name)

    @discover_instance
    def zadd(self, name, *args, **kwargs):
        return self.master.zadd(name, *args, **kwargs)

    @discover_instance
    def zrem(self, name, *values):
        return self.master.zrem(name, *values)

    @discover_instance
    def hset(self, name, key, value):
        return self.master.hset(name, key, value)

    @discover_instance
    def hget(self, name, key):
        return self.slave.hget(name, key)

    @discover_instance
    def hdel(self, name, *keys):
        return self.master.hdel(name, *keys)

    @discover_instance
    def hgetall(self, name):
        return self.slave.hgetall(name)

    @discover_instance
    def hmget(self, name, keys, *args):
        return self.slave.hmget(name, keys, *args)

    @discover_instance
    def flushdb(self):
        return self.master.flushdb()

    @discover_instance
    def dbsize(self):
        return self.slave.dbsize()

    @discover_instance
    def zrangebyscore(self, name, min, max, start=None, num=None, withscores=False, score_cast_func=float):
        return self.slave.zrangebyscore(name, min, max, start, num, withscores, score_cast_func)

    def discover_master(self):
        try:
            self.master = self.sentinel.master_for(settings.SENTINEL_MASTER, db=self.selected_db, socket_timeout=None)
        except:
            self.logger.exception("Exception !! %s", traceback.format_exc())
            raise

    def discover_slave(self):
        try:
            self.slave = self.sentinel.slave_for(settings.SENTINEL_MASTER, db=self.selected_db, socket_timeout=None)
        except:
            self.logger.exception("Exception !! %s", traceback.format_exc())
            raise

    def __getattr__(self, name):
        return getattr(self.get_slave(), name)


class RedisHA(BaseCache):
    '''
    This class overrides the get and set operations to pickle and unpickle while performing the operations.
    '''
    def __init__(self, host='localhost', port=6379, db=0, password=None, socket_timeout=None):
        super(RedisHA, self).__init__(host, port, db, password, socket_timeout)

    def get(self, key):
        # supporting the older key format in existing cache
        key = ":1:" + key
        ret = super(RedisHA, self).get(key)
        if ret is not None:
            try:
                ret = pickle.loads(ret)
            except:
                raise
        return ret

    def set(self, key, value, expiry=7776000):
        # supporting the older key format in existing cache
        key = ":1:" + key
        try:
            value = pickle.dumps(value)
        except:
            raise
        super(RedisHA, self).set(key, value, expiry)
