import pymongo
import MySQLdb
import traceback
import time
from operator import itemgetter

start_time = time.time()
print("inicio " + str(time.localtime()))

mongo_client = pymongo.MongoClient('localhost', 27017)
mongo_db = mongo_client.wikipedia
mongo_db_revs = mongo_db.revs

rev_list = []

for rev in mongo_db_revs.find():
    rev_list.append(rev)

rev_list_sorted = sorted(rev_list, key=itemgetter('date', 'rev_id'))


REV_INSERT = """INSERT INTO revision VALUES (%s,%s,%s)"""
RL_XINSERT = """INSERT INTO revision_equation(rev_id, eq_id, count) VALUES """
EQ_XINSERT = """INSERT INTO equation(eq_id, equation) VALUES """
EQ_SELECT = """SELECT eq_id FROM equation WHERE equation = {}"""

mysql_conn = MySQLdb.connect(host="localhost",
                            user="root",
                            passwd="root", 
                            db="wikipedia")

cur = mysql_conn.cursor()

eq_id_controller = 0

qtd_revs = 0

try:
    for rev in rev_list_sorted:
        qtd_revs += 1
        if qtd_revs % 10000 == 0:
            mid_time = time.time()
            print(str(qtd_revs/10000)+"/13 "+str(time.localtime()))
        qtd_insert_eq = 0
        eq_insert_list = []
        rl_insert_list = []
        revision = (rev['page_id'], rev['rev_id'], rev['date'])
        equation_set = set(rev['formulas'])
        qtd_equation = len(equation_set)
        cur.execute(REV_INSERT, revision)
        for formula in equation_set:
            eq = str(formula.encode('utf-8').hex())
            if eq:
                cur.execute(EQ_SELECT.format("0x"+eq))
                equation_id = cur.fetchone()
                qtd = rev['formulas'].count(formula)
                if equation_id:
                    rl_insert_list.extend([rev['rev_id'], equation_id[0], qtd])
                else:
                    eq_id_controller += 1
                    qtd_insert_eq += 1
                    eq_insert_list.extend([eq_id_controller, eq])
                    rl_insert_list.extend([rev['rev_id'], eq_id_controller, qtd])

        if qtd_insert_eq:
            eq_query = EQ_XINSERT + ','.join("(%s, unhex(%s))" for _ in range(qtd_insert_eq))
            cur.execute(eq_query, eq_insert_list)

        if qtd_equation:
            rl_query = RL_XINSERT + ','.join("(%s, %s, %s)" for _ in range(qtd_equation))
            cur.execute(rl_query, rl_insert_list)

        mysql_conn.commit()

except: traceback.print_exc()

mysql_conn.close()

end_time = time.time()
print("fim " + str(time.localtime()))

print("tempo gasto")
print(end_time - start_time)