# -*- coding: utf-8 -*-
import os
import json
import pandas as pd
from scrapy import Request, Spider
from tcc.items.wikipedia import WikipediaBaseItem

class WikipediaSpider(Spider):

    PATH = '/home/herbert/tcc/tcc'
    INPUT_FILE = 'input_pages.csv'
    filePath = os.path.join(PATH, INPUT_FILE)
    ENCODING = 'utf-8'
    name = 'wikipedia'
    allowed_domains = ['en.wikipedia.org']

    def __init__(self):
        super(WikipediaSpider, self).__init__()

        self.host = 'https://en.wikipedia.org/w/api.php'
        #my cool project useragent
        user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
        user_agent += '(KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36'

        # self.pages = self.load_pages()

        self.default_headers = {
            'User-Agent' : user_agent
        }

    def start_requests(self):
        df = pd.read_csv(self.filePath, header=0)
        # df = {'page_ids': [41641]}
        for page_id in df['page_ids']:
            print(page_id)
            endpoint = '&'.join([
                'action=query',
                'formatversion=2',
                'pageids={}'.format(page_id),
                'prop=revisions',
                'rvprop=ids|timestamp|content',
                'format=json',
                'rvlimit=max'
            ])
            rev_list = []
            url = '{}?{}'.format(self.host, endpoint)
            yield Request(
                url,
                meta={'rev_list': rev_list},
                headers=self.default_headers,
                callback=self.parse
            )

    def parse(self, response):
        rev_list = response.meta['rev_list']
        j = json.loads(response.text)

        if 'query' in j:
            j = j['query']
            if 'pages' in j:
                j = j['pages']
                for page in j:
                    page_id = page['pageid']
                    if 'revisions' in page:
                        for rev in page['revisions']:
                            if 'content' in rev:
                                item = WikipediaBaseItem()
                                item['page_id'] = page_id
                                item['rev_id'] = rev['revid']
                                item['date'] = rev['timestamp'].split('T')[0]
                                item['formulas'] = []
                                formulas = [f.strip() for f in rev['content'].split('<math>')[1:]]
                                for f in formulas:
                                    item['formulas'].append(
                                        f.split('</math>')[0].strip()
                                    )
                                rev_list.append(item)

        if 'continue' in j:
            rvcontinue = j['continue']['rvcontinue']
            url = response.url + '&rvcontinue={}'.format(rvcontinue)
            yield Request(
                url,
                meta={'rev_list':rev_list},
                headers=self.default_headers,
                callback=self.parse
            )

        elif rev_list:
            ready_items = []
            ready_items.append(rev_list.pop())
            while(rev_list):
                rev = rev_list.pop()
                last = ready_items[-1]
                if set(last['formulas']) == set(rev['formulas']):
                    if len(last['formulas']) != len(rev['formulas']):
                        ready_items.append(rev)
                else:
                    ready_items.append(rev)

            for item in ready_items:
                yield item

# to json
# scrapy crawl wikipedia -o items.json -t json