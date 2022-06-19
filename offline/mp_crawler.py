import threading
import math
import time
from lxml import etree
import os

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from conf import conf
from file_utils import writeJson, readJson, writeXlsx, readTxt

chrome_options = Options()
chrome_options.add_argument('--headless')

# Ubuntu releases: http://releases.ubuntu.com/ & http://manpages.ubuntu.com/manpages/
releases = ['precise', 'trusty', 'xenial', 'artful', 'bionic', 'cosmic', 'disco', 'eoan', 'focal']
release_vers = ['12.04', '14.04', '16.04', '17.10', '18.04', '18.10', '19.04', '19.10', '20.04']


def crawlMans(manual_dir):
    """
    Crawl the command lists contained in Nine mans' of Nine ubuntu releases.
    A man url: http://manpages.ubuntu.com/manpages/artful/en/man1/
    """
    driver = webdriver.Chrome(options=chrome_options)

    for release in releases:
        _dir = manual_dir + '/' + release
        if not os.path.exists(_dir):
            os.makedirs(_dir)

        for i in range(1, 10):
            html_f = _dir + '/man' + str(i) + '.html'
            if os.path.exists(html_f):
                continue
            release_man_url = conf.ubuntu_mp_url + '/' + release + '/en/man' + str(i) + '/'
            print('Crawling:', release_man_url)

            try:
                driver.get(release_man_url)
                with open(html_f, 'w', encoding='utf-8') as f:
                    f.write(driver.page_source)
                e = etree.HTML(driver.page_source)
                man_links = e.xpath("//pre/a/@href")
                if man_links is not None and len(man_links) > 3:
                    man_links = [ release_man_url + link for link in man_links[2:] ]
                    _dict = {'# Command links': len(man_links), 'Command links': man_links}
                    writeJson(_dict, _dir + '/man' + str(i) + '.json')
            except Exception as e:
                print('**** Error:', e)

    driver.close()


def outputCmdNumInMans4DiffReleases_xlsx(manual_dir, res_xlsx):
    """
    Output the number of commands in Nine mans for different releases.
    """
    mans, lines, all_cmdnum = [ 'man' + str(i) for i in range(1, 10) ], [], 0
    for release in releases:
        cols = [release]
        for man in mans:
            _dict = readJson(manual_dir + '/' + release + '/' + man + '.json')
            cmdnum = _dict['# Command links']
            cols.append(cmdnum)
            all_cmdnum += cmdnum
        cols.append(sum(cols[1:]))
        lines.append(cols)
    print('# all commands:', all_cmdnum)  # 1,071,056
    writeXlsx(['Release'] + mans + ['Total'], lines, res_xlsx)


class my_Thread(threading.Thread):
    """
    My Thread for handling a list of items using func.
    """
    def __init__(self, thread_id, func, items, *args):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.func = func
        self.items = items
        self.args = args

    def run(self):
        print('Start Thread:', self.thread_id, '*****')
        self.func(self.thread_id, self.items, *self.args)
        print('Exit Thread:', self.thread_id, '*****')


def handleItems_threading(item_path_dict, func, thread_num, failed_urls_f):
    """
    Handle a list of items using func with threading.
    """
    items = list(item_path_dict.keys())
    thread_itemsize = math.ceil(len(items) / thread_num)

    for i in range(thread_num):
        thread_id = str(i + 1)
        parti_items = items[i * thread_itemsize:(i + 1) * thread_itemsize]
        if len(parti_items) > 0:
            if failed_urls_f is not None:
                thread = my_Thread(thread_id, func, parti_items, item_path_dict, failed_urls_f)
            else:
                thread = my_Thread(thread_id, func, parti_items, item_path_dict)
            thread.start()


def crawlCmds(manual_dir):
    """
    Crawl commands in all mans of Nine releases.
    A command url: http://manpages.ubuntu.com/manpages/precise/en/man1/0alias.1.html
    """
    failed_url_c_dict = {}
    failed_urls_path = manual_dir + '/failed_cmdurls.txt'
    if os.path.exists(failed_urls_path):
        lines = readTxt(failed_urls_path)
        for line in lines:
            url = line.split('\t')[0]
            if url not in failed_url_c_dict:
                failed_url_c_dict[url] = 0
            failed_url_c_dict[url] += 1

    # a failed url will be tried for 5 times at most
    noretry_urls = set([ url for url in failed_url_c_dict if failed_url_c_dict[url] >= 5 ])
    failed_urls_f = open(failed_urls_path, 'a+', encoding='utf-8')
    url_path_dict, exist_paths = {}, set()

    for release in releases:
        for i in range(1, 10):
            s = manual_dir + '/' + release + '/man' + str(i)
            _dict, man_dir = readJson(s + '.json'), s + '-html'
            if not os.path.exists(man_dir):
                os.makedirs(man_dir)
            else:
                for name in os.listdir(man_dir):
                    path = man_dir + '/' + name
                    if os.path.getsize(path) > 39:  # <html><head></head><body></body></html>
                        exist_paths.add(path)
                    else:
                        print('empty:', path)
                        os.remove(path)
            urls = _dict['Command links']
            for j in range(len(urls)):
                # 'pmdaMain.3.html' and 'pmdamain.3.html' cannot be exist in one folder
                # Thus, the command html files must be indexed
                url, path = urls[j], man_dir + '/' + str(j) + '.html'
                if url not in noretry_urls and path not in exist_paths:
                    url_path_dict[url] = path

    urlnum = len(url_path_dict)
    print('# Handled links:', len(exist_paths))
    print('# Remaining links to be crawled:', urlnum)
    print('# No retry failed links:', len(noretry_urls))
    print('SUM:', len(exist_paths) + urlnum + len(noretry_urls))

    if urlnum == 0:
        return
    if urlnum > 50:
        handleItems_threading(url_path_dict, getHtml4Urls, 10, failed_urls_f)
    else:
        getHtml4Urls('Without-Thread', url_path_dict.keys(), url_path_dict, failed_urls_f)


def getHtml4Urls(thread_id, urls, url_path_dict, failed_urls_f):
    """
    Get html content for a set of urls.
    """
    driver = webdriver.Chrome(options=chrome_options)

    for url in urls:
        path = url_path_dict[url]
        try:
            print(thread_id, ':', url, '->', path)
            driver.get(url)
            # driver.find_element_by_id('tableWrapper')
            # with open(path, 'w', encoding='utf-8') as f:
            #     f.write(driver.page_source)
            page_source = driver.page_source
            if len(page_source) > 39:  # empty: <html><head></head><body></body></html>
                print(url, '-->', 'OK!')
                with open(path, 'w', encoding='utf-8') as f:
                    f.write(page_source)
        except Exception as e:
            failed_urls_f.write(url + '\t' + path + '\n')
            failed_urls_f.flush()
            print('**** Error:', e)

    driver.close()


if __name__ == '__main__':

    if not os.path.exists(conf.exp_manual_dir):
        os.makedirs(conf.exp_manual_dir)

    _relman_statis_xlsx = conf.exp_manual_dir + '/release_mans_statis.xlsx'

    start = time.time()
    # crawlMans(conf.exp_manual_dir)  # 3087s
    # outputCmdNumInMans4DiffReleases_xlsx(conf.exp_manual_dir, _relman_statis_xlsx)  # 1,071,056 MPs totally

    # NOTE: we found that the MPs has no change during '2020/04/21 - 2020/07/20'
    # crawlCmds(conf.exp_manual_dir)  # 1,071,056 commands obtained
    print(time.time() - start, 's')
