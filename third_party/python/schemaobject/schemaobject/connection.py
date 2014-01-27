import MySQLdb
import re

REGEX_RFC1738 = re.compile(r'''
            (?P<protocol>\w+)://
            (?:
                (?P<username>[^:/]*)
                (?::(?P<password>[^/]*))?
            @)?
            (?:
                (?P<host>[^/:]*)
                (?::(?P<port>[^/]*))?
            )?
            (?:/(?P<database>.*))?
            ''', re.X)


def parse_database_url(url):
    matches = REGEX_RFC1738.match(url)
    result = {}

    if matches:
        if matches.group('protocol'):
            result['protocol'] = matches.group('protocol')

        if matches.group('username'):
            result['user'] = matches.group('username')

        if matches.group('password'):
            result['passwd'] = matches.group('password')

        if matches.group('database'):
            result['db'] = matches.group('database')

        if matches.group('host'):
            result['host'] = matches.group('host')

        try:
            result['port'] = int(matches.group('port'))
        except (TypeError, ValueError):
            pass

    return result


class DatabaseConnection(object):
    """A lightweight wrapper around MySQLdb DB-API"""

    def __init__(self):
        self._db = None
        self.db = None
        self.host = None
        self.port = None
        self.user = None

    @property
    def version(self):
        result = self.execute("SELECT VERSION() as version")
        return result[0]['version']

    def execute(self, sql, values=None):
        cursor = self._db.cursor()
        cursor.execute(sql, values)

        if not cursor.rowcount:
            return None

        fields = [field[0] for field in cursor.description]
        rows = cursor.fetchall()

        cursor.close()
        return  [dict(zip(fields, row)) for row in rows]

    def connect(self, connection_url):
        """Connect to the database"""

        kwargs = parse_database_url(connection_url)
        if not (kwargs and kwargs['protocol'] == 'mysql'):
            raise TypeError("Connection protocol must be MySQL!")

        self.db = kwargs.get('db', None)
        self.host = kwargs.get('host', 'localhost')
        self.port = kwargs.get('port', 3306)
        self.user = kwargs.get('user', None)

        # can't pass protocol to MySQLdb
        del kwargs['protocol']
        self._db = MySQLdb.connect(**kwargs)

    def close(self):
        """Close the database connection."""
        if self._db is not None:
            self._db.close()

    def __del__(self):
        self.close()

# Alias MySQL exception
DatabaseError = MySQLdb.Error
