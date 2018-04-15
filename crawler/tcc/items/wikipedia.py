# -*- coding: utf-8 -*-
import logging

from scrapy import Field

from tcc.dbhelper import DBHelper
from tcc.items.baseitem import BaseItem


class WikipediaBaseItem(BaseItem):
    
    page_id = Field()
    rev_id = Field()
    date = Field()
    formulas = Field()

    __tablename__ = 'wikipedia'
    __sqlcolumns__ = []