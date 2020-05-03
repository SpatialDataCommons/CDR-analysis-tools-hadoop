from impala.dbapi import connect


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class HiveConnection(metaclass=Singleton):
    def __init__(self, host='', port='', user=''):
        self.conn = connect(host, port, user=user, auth_mechanism='PLAIN')
        self.cursor = self.conn.cursor()
        self.cursor.set_arraysize(1)
