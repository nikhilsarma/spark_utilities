"""
a ready reckoner r made from 
1. https://datathirst.net/blog/2019/3/7/databricks-connect-finally
2. https://datathirst.net/blog/2018/10/12/executing-sql-server-stored-procedures-on-databricks-pyspark
3. https://kb.informatica.com/solution/23/Pages/72/590441.aspx
4. https://stackoverflow.com/questions/53704187/connecting-to-an-azure-database-using-sqlalchemy-in-python
5. https://docs.sqlalchemy.org/en/13/dialects/mssql.html#module-sqlalchemy.dialects.mssql.pyodbc
6. https://stackoverflow.com/questions/44527452/cant-open-lib-odbc-driver-13-for-sql-server-sym-linking-issue?rq=1
7. https://stackoverflow.com/questions/56053724/microsoftodbc-driver-17-for-sql-serverlogin-timeout-expired-0-sqldriverco
8. Connect via windows DNS - TODO
"""
"""
pyodbc - a different execute_many handling for native Microsoft ODBC drivers, to speed up bulk inserts.
fast_executemany = True on cursor instance
"""

import pyodbc
from sqlalchemy import create_engine
from urllib.parse import quote_plus

#dummy code to create tables and load data into them
#expects a cursor object from pyodbc OR connection object from sqlalchemy
def create_tables(cur):
    heros_sql = """
    CREATE TABLE heros (id integer PRIMARY KEY, first_name text NOT NULL,last_name text NOT NULL)"""
    cur.execute(heros_sql)
    powers_sql = """CREATE TABLE powers (id integer PRIMARY KEY, name text NOT NULL, intensity real NOT NULL)"""
    cur.execute(powers_sql)

def put_dummy_data(cur):
    powers_sql = "INSERT INTO powers (id, name, intensity) VALUES (?,?,?)"
    cur.execute(powers_sql, (1,'super speed', 10))
    cur.execute(powers_sql, (2,'shapeshifting', 5))
    cur.execute(powers_sql, (3,'super sense', 15))
    cur.execute(powers_sql, (4,'invisibility', 30))
    heros_sql = "INSERT INTO heros (id, first_name, last_name) VALUES (?, ?, ?)"
    cur.execute(heros_sql, (1,'spider', 'man'))
    cur.execute(heros_sql, (2,'super', 'man'))
    cur.execute(heros_sql, (3,'hit', 'man'))
    cur.execute(heros_sql, (4,'he', 'man'))
    
"""
regular pyodbc way of doing things (connecting to SQL Server)
make sure Azure allows IP to be connected. (make changes in firewall accordingly)
"""
server = 'nikil.database.windows.net'
database = 'actors'
username = 'india'
password = 'Country123'
driver= '{ODBC Driver 17 for SQL Server}'
cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()
create_tables(cursor)
put_dummy_data(cursor)
cursor.execute("select * from heros")
cursor.fetchall()

"""
SQLAlchemy way of doing things (connecting to SQL Server)
"""
params = quote_plus(r'Driver={ODBC Driver 17 for SQL Server};Server=tcp:nikil.database.windows.net,1433;Database=actors;Uid=india;Pwd=Country123;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
engine_azure = create_engine(conn_str, echo=True, fast_executemany=True)

print('connection is ok')
#print(engine_azure.table_names())
connection = engine_azure.connect()
#create_tables(connection)
#put_dummy_data(connection)
result = connection.execute("select * from heros")
result.fetchall()


"""
Reading and writing back from a spark Dataframe. (using JDBC)
https://docs.databricks.com/data/data-sources/sql-databases.html
"""
TO-DO
