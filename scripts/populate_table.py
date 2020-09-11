import MySQLdb

INSERT_COMEBACK_EQUATION_SQL = """
    INSERT INTO comeback_equation VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

mysql_conn = MySQLdb.connect(host="localhost",
                            user="root",
                            db="wikipedia")
cur = mysql_conn.cursor()
total_eqs = 1227581

for eq in range(1, total_eqs):
    t = (eq, 0, 0, 0, 0, 0, 0, 0, 0)
    cur.execute(INSERT_COMEBACK_EQUATION_SQL, t)
    if(eq % 100000 == 0):
        print(eq)
        mysql_conn.commit()