import MySQLdb
import csv
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

SQL_TOTAL_EQS_IN_DAY = """
    select max(eq_id) from revision
    join revision_equation re on revision.rev_id = re.rev_id
    where date <= '{}';
"""

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

last_month = set()
last_max = 0

f = open('output.csv', 'w')
with f:
    writer = csv.writer(f)
    writer.writerow(['date', 'total_days', 'total_eq', 'active_eqs', 'deleted_eqs', 'inserted_eqs', 'comeback_eqs'])
    cur = mysql_conn.cursor()
    for date in dates:
        cur.execute(SQL_TOTAL_EQS_IN_DAY.format(date))
        total_eq = cur.fetchone()[0]
        cur.execute(SQL_EQS_IN_DAY.format(date))
        month_equations = [item[0] for item in cur.fetchall()]
        month_equations = set(month_equations)
        active_eqs = len(month_equations)
        deleted_eqs = sum([1 for eq in last_month if eq not in month_equations])
        new_eqs = [eq for eq in month_equations if eq not in last_month]
        inserted_eqs = len(new_eqs)
        comeback_eqs = sum([1 for eq in new_eqs if eq <= last_max])
        last_month = month_equations
        last_max = total_eq
        total_days = date - sdate
        line = [str(date), str(total_days.days), str(total_eq), str(active_eqs), str(deleted_eqs), str(inserted_eqs), str(comeback_eqs)]
        writer.writerow(line)

f.close()
