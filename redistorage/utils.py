from urlparse import urlparse

import redis


def connection_args(url):
    """
    Parse a URL of the form:

        redis://host:port/db

    and return the values as a dictionary suitable for creating a redis
    connection pool.
    """
    args = {}
    parsed = urlparse(url)
    args['host'] = parsed.hostname
    args['port'] = parsed.port
    args['db'] = parsed.path.split('/', 1)[1]
    return args


def connect(url, max_connections=None):
    """
    Returns a connection pool to a redis database from a given URI.
    """
    args = connection_args(url)
    pool = redis.ConnectionPool(max_connections=max_connections, **args)
    return pool
