from collections import Counter, defaultdict
from datetime import datetime
from collections import OrderedDict
import re
# -*- encoding: utf-8 -*-

class Log(object):
    def __init__(self, request_date, request_type, request, protocol, response_code, response_time):
        self.request_date = request_date
        self.request_type = request_type
        self.request = request
        self.protocol = protocol
        self.response_code = response_code
        self.response_time = response_time

def parse_log(line):
    date = line[0].replace('[', '')
    time = line[1].replace(']', '')
    req_date = datetime.strptime('{} {}'.format(date, time), "%d/%b/%Y %H:%M:%S")

    req_type = line[2].replace('"', '')
    req = line[3]
    prot = line[4].replace('"','')
    res_code = line[5]
    res_time = int(line[6])

    return Log(req_date, req_type, req, prot, res_code, res_time)

def parse(
    ignore_files=False,
    ignore_urls=[],
    start_at=None,
    stop_at=None,
    request_type=None,
    ignore_www=False,
    slow_queries=False
):

    #start_at = datetime(2018, 3, 17, 11, 20, 10)
    #stop_at = datetime(2018, 3, 23, 11, 20, 10)
    #ignore_files = True
    #ignore_www = True
    #ignore_urls = ['https://sys.mail.ru/calendar/config/254/40267/', 'https://www.sys.mail.ru/calendar/config/254/40261/']
    #request_type = "GET"
    #slow_queries = True

    rd_file = open("log.log", "r")
    data = rd_file.readlines()
    def_dict = defaultdict(int)
    top_request = []
    cnt = []
    for idx,line in enumerate(data):
        if re.search('\[\d{2}/\w{3}/\d{4} \d{2}:\d{2}:\d{2}\] "\w{3} .*://.+\d{1,}\.\d{1,}" \d{3} \d{1,}', line):
            line = line.split()
            if not request_type or line[2].replace('"', '') == request_type:
                lg = parse_log(line)
                if ignore_www:
                    lg.request = re.sub('www.', '', lg.request)

                lg.request = re.sub('\?.*', '', re.sub('\w+://', '', lg.request))

                if ignore_files and re.search('\w+://.*/(([a-zA-Z0-9\-_])+\.)+[a-zA-Z]+', lg.request):
                    continue

                if not ignore_urls or not lg.request in ignore_urls:
                    if not (start_at and stop_at) or (stop_at and stop_at > lg.request_date)\
                        or (start_at and start_at < lg.request_date):
                        cnt.append(lg.request)
                        def_dict[lg.request] += lg.response_time

    cnt = Counter(cnt)
    if slow_queries:
        for key in def_dict:
            def_dict[key] = def_dict[key] // cnt[key]
        def_dict = Counter(def_dict).most_common(5)
        for i in range(5):
            top_request.append(def_dict[i][1])
    else:
        cnt = cnt.most_common(5)
        for i in range(5):
            top_request.append(cnt[i][1])

    return top_request

if __name__ == '__main__':
    parse()
