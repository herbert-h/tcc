# Simple example of streaming a Wikipedia 
# Copyright 2017 by Jeff Heaton, released under the The GNU Lesser General Public License (LGPL).
# http://www.heatonresearch.com
# -----------------------------
import xml.etree.ElementTree as etree
import codecs
import csv
import time
import os

# http://www.ibm.com/developerworks/xml/library/x-hiperfparse/

FILES_PATH = '/home/herbert/tcc'
FILENAME_WIKI = 'example.xml'
PAGES_WITH_EQ = 'pages_equation.csv'
LOGFILE = 'output.log'
ENCODING = "utf-8"

# Nicely formatted time string
def hms_string(sec_elapsed):
    h = int(sec_elapsed / (60 * 60))
    m = int((sec_elapsed % (60 * 60)) / 60)
    s = sec_elapsed % 60
    return "{}:{:>02}:{:>05.2f}".format(h, m, s)


def strip_tag_name(t):
    t = elem.tag
    idx = k = t.rfind("}")
    if idx != -1:
        t = t[idx + 1:]
    return t


def has_formula_in_text(text):
    if "<math>" in text:
        return True
    else:
        return False


pathWikiXML = os.path.join(FILES_PATH, FILENAME_WIKI)
pathLogs = os.path.join(FILES_PATH, LOGFILE)
pathPages = os.path.join(FILES_PATH, PAGES_WITH_EQ)

total_count = 0
formula_count = 0
start_time = time.time()
with codecs.open(pathPages, "w", ENCODING) as pagesCSV:
    pageWriter = csv.writer(pagesCSV, quoting=csv.QUOTE_MINIMAL)
    pageWriter.writerow(['page_id', 'title'])
    for event, elem in etree.iterparse(pathWikiXML, events=('start', 'end')):
        tname = strip_tag_name(elem.tag)
        if event == 'start':
            if tname == 'page':
                title = ''
                page_id = ''
                text = ''
                in_revision = False
                has_formula = False
            elif tname == 'revision':
                in_revision = True
            elif tname == 'title':
                title = elem.text
            elif tname == 'id' and not in_revision:
                page_id = elem.text
                print(page_id)
            elif tname == 'text':
                text = elem.text
                if text:
                    has_formula = has_formula_in_text(text)
        else:
            if tname == 'page':
                total_count += 1
                if has_formula:
                    formula_count += 1
                    pageWriter.writerow([page_id, title])

        elem.clear()

        if total_count > 1 and (total_count % 250000) == 0:
            with open(pathLogs, 'a') as l:
                spend_time = time.time() - start_time
                l.write("{}\n".format(hms_string(spend_time)))
                l.write("{:,} total pages\n".format(total_count))
                l.write("{:,} formula pages\n".format(formula_count))

elapsed_time = time.time() - start_time

print("Total pages: {:,}".format(total_count))
print("Formula pages: {:,}".format(formula_count))
print("Elapsed time: {}".format(hms_string(elapsed_time)))
