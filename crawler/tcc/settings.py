# -*- coding: utf-8 -*-

# Scrapy settings for gaivota project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#     http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html
#     http://scrapy.readthedocs.org/en/latest/topics/spider-middleware.html

BOT_NAME = 'tcc'

SPIDER_MODULES = ['tcc.spiders']
NEWSPIDER_MODULE = 'tcc.spiders'

LOG_LEVEL = 'INFO'
LOG_FORMAT = '%(asctime)s [%(name)s] %(levelname)s: %(message)s'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
# USER_AGENT = 'weather (+http://www.yourdomain.com)'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 12

# The download delay setting will honor only one of:
CONCURRENT_REQUESTS_PER_DOMAIN = 12
CONCURRENT_REQUESTS_PER_IP = 0

# Disable Telnet Console (enabled by default)
TELNETCONSOLE_ENABLED = False

FEED_EXPORT_ENCODING = 'utf-8'

# Configure a delay for requests for the same website (default: 0)
# See http://scrapy.readthedocs.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
DOWNLOAD_DELAY = 0

# https://stackoverflow.com/a/31233576
DOWNLOAD_HANDLERS = {
    's3': None
}

# Configure item pipelines
# See http://scrapy.readthedocs.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    # 'tcc.pipelines.PipelineCSV': 300,
    'tcc.pipelines.PipelineMongoDB': 200,
    # 'tcc.pipelines.PipelineSQL': 200
}

# Max items stored in buffer (batch size)
SQL_PIPELINE_BUFFER_MAX_ITEMS = 200
