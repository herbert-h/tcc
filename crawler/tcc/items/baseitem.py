# -*- coding: utf-8 -*-
import logging
import warnings
from abc import abstractstaticmethod

from scrapy import Item


class BaseItem(Item):
    __tablename__ = ''
    __sqlkeys__ = []

    def get_keys_as_csv(self):
        return '{}\n'.format(','.join(self.fields.keys()))

    def get_values_as_csv(self):
        values = [self[f] if f in self.keys() else '' for f in self.fields.keys()]
        values = ['"{}"'.format(v) if ',' in str(v) else str(v) for v in values]
        return '{}\n'.format(','.join(values))

    @staticmethod
    def get_inserts(items):
        # find all tables
        tablenames = list(set([item.__tablename__ for item in items]))

        table_columns = {}
        for tablename in tablenames:

            if tablename not in table_columns:
                table_columns[tablename] = {}

            # filter items by table
            table_items = [item for item in items if item.__tablename__ == tablename]

            for item in table_items:

                # find non empty item columns
                item_columns = set([c for c in item.keys() if item[c] != None and str(item[c])])

                # find item sql columns
                sql_columns = set([c for c in item.__sqlcolumns__])

                # find common columns
                columns = tuple(item_columns & sql_columns)
                if columns not in table_columns[tablename]:
                    table_columns[tablename][columns] = []

                table_columns[tablename][columns].append(item)

        queries = []
        insert_template = 'INSERT INTO {} ({}) VALUES {} ON DUPLICATE KEY UPDATE {};'
        for tablename in table_columns:
            for columns in table_columns[tablename].keys():

                all_values = []
                for item in list(set(table_columns[tablename][columns])):
                    item_values = ["'{}'".format(item[c]) for c in columns]
                    all_values.append(','.join(item_values))

                all_values = ['({})'.format(v) for v in all_values]
                update = ['{}=VALUES({})'.format(c, c) for c in columns]
                query = insert_template.format(tablename, ','.join(columns),\
                                       ','.join(all_values), ','.join(update))
                queries.append(query)

        return queries

    @classmethod
    def save(cls, db, crawler_id, items):
        warnings.filterwarnings('error', category=MySQLdb.Warning)
        logger = logging.getLogger('save_method')

        items = cls.presave(db, crawler_id, items)
        items = cls.cast(items)

        if not items:
            # Returns True assuming that no items can be stored in database
            return True

        cur = db.cursor()
        success = False

        try:
            for insert in BaseItem.get_inserts(items):
                cur.execute(insert)
            db.commit()
            success = True

            msg = '{} new records added to the database.'
            logger.info(msg.format(len(items)))
        except Exception as e:
            db.rollback()

            msg = 'Failed to persist data: {}'.format(e)
            logger.error(msg)
        finally:
            cur.close()

        return success

    @staticmethod
    def presave(db, crawler_id, items):
        return items

    @staticmethod
    def cast(items):
        return items
