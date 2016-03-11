import MySQLdb as _mysql
import collections

class MySQLDatabase:
    def __init__(self, database_name, username, password, host='localhost'):
        try:
            self.db = _mysql.connect(db=database_name,
                                     host=host,
                                     user=username,
                                     passwd=password)
            self.database_name = database_name

            print "Connected to mySQL!"
        except _mysql.Error, e:
            print e

    def __del__(self):
        if hasattr(self, 'db'):
            self.db.close()
            print "MySQL Connection closed"

    def get_available_tables(self):
        cursor = self.db.cursor()
        cursor.execute("SHOW TABLES;")

        tables = cursor.fetchall()

        cursor.close()

        return tables

    def get_columns_for_tables(self, table_name):
        cursor = self.db.cursor()
        cursor.execute("SHOW COLUMNS FROM '%s'" % table_name)

        columns = cursor.fetchall()

        cursor.close()

        return columns

    def convert_to_named_tuples(self, cursor):
        results = None

        names = " ".join(d[0] for d in cursor.description)
        klass = collections.namedtuple('Results', names)

        try:
            results = map(klass._make, cursor.fetchall())
        except _mysql.ProgrammingError:
            pass

        return results

    def select(self, table, columns=None, named_tuples=False, **kwargs):
        """
        select(table_name, [list of column names])
        """
        sql_str = "select"

        # add columns or justthe wildcard

        if not columns:
            sql_str += " * "
        else:
            for column in columns:
                sql_str += "%s, " % column

            sql_str = sql_str[:-2]  # remove the last comma

        # add the table to select from
        sql_str += " FROM  '%s ,'%s'" % (self.database_name, table)

        # ther a join clause attached
        if kwargs.has_key('join'):
            sql_str += " join %s" % kwargs.get('join')

        # ther a where clause attached
        if kwargs.has_key('were'):
            sql_str += " WHERE %s" % kwargs.get('where')

        sql_str += ";"  # finalise our sql string

        cursor = self.db.cursor()
        cursor.execute(sql_str)


        if named_tuples:
            results = self.convert_to_named_tuples(cursor)
        else:
            results = cursor.fetchall()

        cursor.close()

        return results
