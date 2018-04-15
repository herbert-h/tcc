# -*- coding: utf-8 -*-
import codecs
import os
import pymongo
from scrapy.exceptions import DropItem

class PipelineCSV:
    item_file_map = {}

    def process_item(self, item, spider):
        item_class = type(item).__name__
        if item_class not in self.item_file_map:
            self.item_file_map[item_class] = item_class + '.csv'
            with codecs.open(self.item_file_map[item_class], 'w', 'utf-8') as f:
                f.write(item.get_keys_as_csv())

        with codecs.open(self.item_file_map[item_class], 'a', 'utf-8') as f:
            f.write(item.get_values_as_csv())

        return item

class PipelineMongoDB:

    def __init__(self):
        MONGODB_SERVER = "localhost"
        MONGODB_PORT = 27017
        MONGODB_DB = "wikipedia"
        MONGODB_COLLECTION = "revs"

        connection = pymongo.MongoClient(
            MONGODB_SERVER,
            MONGODB_PORT
        )
        db = connection[MONGODB_DB]
        self.collection = db[MONGODB_COLLECTION]

    def process_item(self, item, crawler):
        valid = True
        for data in item:
            if not data:
                valid = False
                raise DropItem("Missing {0}!".format(data))
        if valid:
            self.collection.insert(dict(item))

        return item