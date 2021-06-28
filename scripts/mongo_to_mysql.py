import pymongo
import MySQLdb
import traceback
import time
import logging
from functools import lru_cache
from operator import itemgetter

logging.basicConfig(level = logging.INFO, format = '%(asctime)s  %(levelname)-10s %(processName)s  %(name)s %(message)s', datefmt =  "%Y-%m-%d-%H-%M-%S")

start_time = time.time()
logging.info("inicio " + str(time.asctime()))

REV_INSERT = """INSERT INTO revision VALUES (%s,%s,%s)"""
RL_XINSERT = """INSERT INTO revision_equation(rev_id, eq_id, count) VALUES """
EQ_XINSERT = """INSERT INTO equation(eq_id, equation) VALUES """
EQ_SELECT = """SELECT eq_id FROM equation WHERE equation = {}"""

eq_id_controller = 0
last_eq_id = 0
mysql_conn = MySQLdb.connect(host="localhost",
                            user="root", 
                            db="wikipedia")
cur = mysql_conn.cursor()

def get_mongo_sorted_revs():
    mongo_client = pymongo.MongoClient('localhost', 27017)
    mongo_db = mongo_client.wikipedia
    mongo_db_revs = mongo_db.revs 
    seen = {}
    rev_list = []
    for rev in mongo_db_revs.find():
        h = hash((rev['page_id'], rev['rev_id']))
        if h not in seen.keys():
            rev_list.append(rev)
            seen[h] = 1

    return sorted(rev_list, key=itemgetter('date', 'rev_id'))

@lru_cache(maxsize=307200)
def get_formula_id(eq):
    global eq_id_controller
    cur.execute(EQ_SELECT.format("0x"+eq))
    equation_id = cur.fetchone()
    if equation_id:
        return equation_id
    else:
        eq_id_controller = eq_id_controller + 1
        return eq_id_controller

def process(sorted_revs = []):
    global last_eq_id
    qtd_revs = 0
    for rev in sorted_revs:
        try:
            qtd_revs += 1
            if qtd_revs % 10000 == 0:
                logging.info(str(qtd_revs)+"/"+str(len(sorted_revs))+" "+ str(time.asctime()))
                logging.info("actual rev date " + str(rev['date']))

            qtd_insert_eq = 0
            eq_insert_list = []
            rl_insert_list = []
            revision = (rev['page_id'], rev['rev_id'], rev['date'])
            equation_set = set(filter(None, rev['formulas']))
            qtd_equation = len(equation_set)
            cur.execute(REV_INSERT, revision)
            for formula in equation_set:
                eq = str(formula.encode('utf-8').hex())
                qtd = rev['formulas'].count(formula)
                eq_id = get_formula_id(eq)
                rl_insert_list.extend([rev['rev_id'], eq_id, qtd])
                if eq_id == eq_id_controller and eq_id != last_eq_id:
                    qtd_insert_eq += 1
                    eq_insert_list.extend([eq_id, eq])
                    last_eq_id = eq_id

            if qtd_insert_eq:
                eq_query = EQ_XINSERT + ','.join("(%s, unhex(%s))" for _ in range(qtd_insert_eq))
                cur.execute(eq_query, eq_insert_list)

            if qtd_equation:
                rl_query = RL_XINSERT + ','.join("(%s, %s, %s)" for _ in range(qtd_equation))
                cur.execute(rl_query, rl_insert_list)

            mysql_conn.commit()
        except:
            logging.error("error in page_id="+str(rev['page_id'])+ " rev_id=" +str(rev['rev_id']))

    mysql_conn.close()

    end_time = time.time()
    logging.info("fim " + str(time.asctime()))
    logging.info("tempo gasto " + str(end_time - start_time))

def main():
    sorted_revs_list = get_mongo_sorted_revs()
    process(sorted_revs_list)

if __name__ == "__main__":
    main()
