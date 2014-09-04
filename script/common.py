

import time
import json
import urllib
import urllib2
from setting import POST_URL
import httplib

httplib.HTTPConnection._http_vsn = 10
httplib.HTTPConnection._http_vsn_str = 'HTTP/1.0'
TIME_FORMAT = '%Y-%m-%d %H:%M:%S'


def timestamp_datetime(value):
    value = time.localtime(value)
    dt = time.strftime(TIME_FORMAT, value)
    return dt


def datetime_timestamp(dt):
     time.strptime(dt, TIME_FORMAT)
     s = time.mktime(time.strptime(dt, TIME_FORMAT))
     return int(s)


def get_serverdata(querydata):
    postdata = urllib.urlencode({'report_param': querydata})
    try:
        rsp = urllib2.build_opener().open(urllib2.Request(POST_URL, postdata)).read()
    except Exception as e:
        return None

    try:
        data = json.loads(rsp).get('data', '').get('data', '')
    except AttributeError as e:
        return None


def hround(num, prc=3):
    strnum = repr(num)
    strnum_len = len(strnum)
    p_dot = strnum.find('.')

    if strnum_len - 1 - p_dot < prc+1:
        return strnum

    strnum_list = list(strnum[:p_dot+prc+2])

    keynum = int(strnum_list[p_dot+prc+1])
    if keynum >= 5:
        tmp = int(strnum_list[p_dot+prc])
        if tmp + 1 == 10:
            strnum_list[p_dot+prc] = '0'
            if int(strnum_list[p_dot+prc-1]) + 1 == 10:
                strnum_list[p_dot+prc-1] = '0'
                if int(strnum_list[p_dot+prc-2]) + 1 == 10:
                    strnum_list[p_dot+prc-2] = '0'
                    strnum_list[p_dot+prc-4] = str(int(strnum_list[p_dot+prc-4]) + 1)
                else:
                    strnum_list[p_dot+prc-2] = str(int(strnum_list[p_dot+prc-2]) + 1)
            else:
                strnum_list[p_dot+prc-1] = str(int(strnum_list[p_dot+prc-1]) + 1)
        else:
            strnum_list[p_dot+prc] = str(tmp + 1)


    tmpstrnum = str(float(''.join(strnum_list[:p_dot+prc+1])))
    return tmpstrnum

def get_currency():
    get_url = 'http://172.20.0.51:9099/xchangeRates?params%3D%7B%22query_type%22%3A%22all%22%2C%22return_format%22%3A%22json%22%2C%22query_id%22%3A%2288e771bc-a11-44dd-afa2-9fcc056e9eeb%22%2C%22colums%22%3A%5B%22currency_from%22%2C%22currency_to%22%2C%22rate_from_to%22%2C%22rate_usd_to%22%5D%7D'
    return json.loads(urllib2.urlopen(urllib2.Request(get_url)).read()).get('rates')


if __name__ == '__main__':
    d = datetime_timestamp('2012-03-28 06:53:40')
    print d
    s = timestamp_datetime(1332888820)
    print s



