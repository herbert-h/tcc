import MySQLdb
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta

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

SQL_UPDATE_INSERTED_EQ_COUNT = """
    UPDATE temporary_equation
    SET {} = {} + 1
    WHERE eq_id = {}
"""

def decrease_ttl(del_eqs):
    new_del_eqs = []
    for eq, ttl in del_eqs:
        new_del_eqs.append((eq, ttl-1))
        
    return new_del_eqs

sdate = date(2003, 1, 20)
edate = date(2020, 6, 20)
dates = []
d = sdate
while d <= edate:
    dates.append(d)
    d = d + relativedelta(months=+1)

mysql_conn = MySQLdb.connect(host="localhost",
                            user="root",
                            db="wikipedia")
cur = mysql_conn.cursor()

inserted_eqs = []
not_temporary_eqs = []
last_month_eqs = []

for date in dates:
    if(date.month == 1):
        print(date)
    inserted_eqs = decrease_ttl(inserted_eqs)

    cur.execute(SQL_EQS_IN_DAY.format(date))
    month_eqs = [item[0] for item in cur.fetchall()]
    month_eqs = set(month_eqs)
    for eq, ttl in inserted_eqs:
        if eq not in month_eqs:
            col = None
            if ttl == 5:
                col = "one_month"
            elif ttl == 4:
                col = "two_months"
            elif ttl == 3:
                col = "three_months"
            elif ttl == 2:
                col = "four_months"
            elif ttl == 1:
                col = "five_months"
            elif ttl == 0:
                col = "six_months"
            elif eq in not_temporary_eqs:
                not_temporary_eqs.remove(eq)
                col = "more_months"

            if col != None:
                inserted_eqs.remove((eq, ttl))
                cur.execute(SQL_UPDATE_INSERTED_EQ_COUNT.format(col, col, eq))
        else:
            if ttl == 0:
                inserted_eqs.remove((eq, ttl))
                not_temporary_eqs.append(eq)

    mysql_conn.commit()

    month_inserted_eqs = [eq for eq in month_eqs if eq not in last_month_eqs]
    for eq in month_inserted_eqs:
        inserted_eqs.append((eq, 6))
    
    last_month_eqs = month_eqs

col = 'never_comeback'
for eq in not_temporary_eqs:
    cur.execute(SQL_UPDATE_INSERTED_EQ_COUNT.format(col, col, eq))

mysql_conn.commit()
