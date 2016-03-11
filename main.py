from database.mysql_test import MySQLDatabase

my_db_connection = MySQLDatabase('employess_db','root','CC0987654321')

my_cursor = my_db_connection.db.cursor()
my_cursor.execute("SELECT * FROM employees")
results = my_cursor.fetchmany(5)

get_tables = my_db_connection.get_available_tables()



print get_tables