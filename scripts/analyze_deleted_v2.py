import MySQLdb
from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
import pandas as pd

SQL_EQS_IN_DAY = """
    select distinct(re.eq_id)
    from revision r
    inner join
    (
        select r1.page_id, max(rev_id) max_rev_id
        from revision r1
        inner join
        (
            select page_id, max(date) max_date
            from revision
            where date <= '{}'
            group by page_id
        ) q1
        on r1.page_id = q1.page_id
        and r1.date = q1.max_date
        group by page_id
    ) q2
    on r.page_id = q2.page_id
    and rev_id = q2.max_rev_id
    inner join
    revision_equation re
    on q2.max_rev_id = re.rev_id;
"""

SQL_UPDATE_DELETED_EQ_COUNT = """
    UPDATE comeback_equation
    SET {} = {} + 1
    WHERE eq_id = {}
"""

print("start -> {}".format(datetime.now()))

start_date = date(2003, 1, 20)
end_date = date(2020, 6, 20)
dates = []
d = start_date
while d <= end_date:    
    dates.append(d)
    d = d + relativedelta(months=+1)

mysql_conn = MySQLdb.connect(host="localhost",
                            user="root",
                            db="wikipedia")
cur = mysql_conn.cursor()

deleted_eqs = {}
not_comeback_eqs = {}
last_month_eqs = {}

for date in dates:
    if(date.month == 1):
        print("{} -> {}".format(date, datetime.now()))
    cur.execute(SQL_EQS_IN_DAY.format(date))
    month_eqs = {item[0] : 1 for item in set(cur.fetchall())}
    for eq in list(deleted_eqs):
        ttl = deleted_eqs[eq]
        col = None
        if eq in month_eqs:
            if ttl == 6:
                col = "one_month"
            elif ttl == 5:
                col = "two_months"
            elif ttl == 4:
                col = "three_months"
            elif ttl == 3:
                col = "four_months"
            elif ttl == 2:
                col = "five_months"
            elif ttl == 1:
                col = "six_months"
            elif ttl == 0:
                col = "more_months"

            deleted_eqs.pop(eq)
            cur.execute(SQL_UPDATE_DELETED_EQ_COUNT.format(col, col, eq))

        else:
            if ttl > 0:
                deleted_eqs[eq] = ttl - 1

    mysql_conn.commit()

    month_deleted_eqs = [eq for eq in last_month_eqs if eq not in month_eqs]
    for eq in month_deleted_eqs:
        deleted_eqs[eq] = 6

    last_month_eqs = month_eqs

col = "not_comeback"
for eq, ttl in deleted_eqs.items():
        cur.execute(SQL_UPDATE_DELETED_EQ_COUNT.format(col, col, eq))

mysql_conn.commit()
mysql_conn.close()

print("end -> {}".format(datetime.now()))