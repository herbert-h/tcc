class DBHelper:

    @staticmethod
    def execute(db, query):
        cur = db.cursor()
        cur.execute(query)
        result = list(cur.fetchall())
        cur.close()
        return result

    @staticmethod
    def get_ana_stations(db):
        import pandas as pd
        query = """
        SELECT custom_id_str station_id,is_telemetric,is_telemetric_res3,is_water_quality
        FROM ana_station a
        JOIN gaivota_odp g
        ON a.gaivota_odp_id=g.id;
        """

        return pd.read_sql(query, db)

    @staticmethod
    def get_accu_cities(db):
        query = """
        SELECT custom_id_str, name
        FROM gaivota_odp
        WHERE crawler_id = (
            SELECT id
            FROM gaivota_crawler
            WHERE name = 'accuweather');
        """

        return DBHelper.execute(db, query)

    @staticmethod
    def get_sidra_cities(db):
        query = """
        SELECT custom_id_str
        FROM gaivota_odp
        WHERE crawler_id = (
            SELECT id
            FROM gaivota_crawler
            WHERE name = 'sidra'); 
        """

        return DBHelper.execute(db, query)

    @staticmethod
    def get_sidra_kg_products(db):
        query = """
        SELECT id, kg_product
        FROM sidra_products
        WHERE kg_product IS NOT NULL;
        """

        return DBHelper.execute(db, query)

    @staticmethod
    def get_climatempo_cities(db):
        query = """
        SELECT custom_id_str, name
        FROM gaivota_odp
        WHERE crawler_id = (
            SELECT id
            FROM gaivota_crawler
            WHERE name = 'climatempo');
        """

        return DBHelper.execute(db, query)

    @staticmethod
    def get_sisser_values(db, state_abbr, year):
        query = """
        SELECT
        city, category, cycle, insurer,
        field, subfield, classification, sa.name
        FROM sisser_psr sp
        JOIN gaivota_odp o
        ON sp.gaivota_odp_id = o.id
        JOIN gaivota_location l
        ON o.location_id = l.id
        JOIN sisser_activities sa
        ON sa.gaivota_agriculture_id = sp.gaivota_agriculture_id
        WHERE l.state_abbr = '{}' AND sp.year = '{}'
        AND sp.created_at > DATE_SUB('{}', INTERVAL 32 DAY)
        AND sp.created_at <= '{}';
        """
        import datetime
        today = datetime.date.today().strftime('%Y-%m-%d')
        query = query.format(state_abbr, year, today, today)

        return DBHelper.execute(db, query)

    @staticmethod
    def get_sinda_pcds(db):
        query = """
        SELECT odp.custom_id_str, odp.name, max(s.date)
        FROM gaivota_odp AS odp 
        LEFT JOIN sinda_hourly s 
        ON odp.id = s.gaivota_odp_id
        WHERE crawler_id = (
				SELECT id
				FROM gaivota_crawler
				WHERE name = 'sinda'
        )
        GROUP BY odp.id;
        """

        return DBHelper.execute(db, query)

    @staticmethod
    def get_sinda_column_id(db, column_names):
        column_names = ','.join(["'{}'".format(name) for name in column_names])
        query = 'SELECT id, name FROM sinda_columns WHERE name IN ({});'
        query = query.format(column_names)

        result = DBHelper.execute(db, query)

        column_name_map = {}
        for column_id, column_name in result:
            column_name_map[column_name] = int(column_id)

        return column_name_map

    @staticmethod
    def get_odp_id(db, crawler_id, column, ids):
        ids = ','.join(["'{}'".format(_id.replace("'", "\'")) for _id in ids])
        query = 'SELECT id, {} FROM gaivota_odp WHERE {} IN ({}) AND crawler_id = {};'
        query = query.format(column, column, ids, crawler_id)

        result = DBHelper.execute(db, query)

        id_odp_map = {}
        for gaivota_odp_id, _id in result:
            id_odp_map[_id] = int(gaivota_odp_id)

        return id_odp_map

    @staticmethod
    def get_agriculture_id(db, tablename, column='name'):
        query = 'SELECT gaivota_agriculture_id, {} FROM {};'
        query = query.format(column, tablename)

        result = DBHelper.execute(db, query)

        col_agri_map = {}
        for gaivota_agriculture_id, col in result:
            col_agri_map[col] = int(gaivota_agriculture_id)

        return col_agri_map

    @staticmethod
    def get_crawler_id(db, crawler_name):
        query = "SELECT id FROM gaivota_crawler WHERE name = '{}';"
        query = query.format(crawler_name)

        result = DBHelper.execute(db, query)

        if not result:
            msg = "Crawler '{}' is not registred in database"
            raise Exception(msg.format(crawler_name))

        return int(result[0][0])
