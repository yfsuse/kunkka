#! /usr/bin/env python

from dboperation import DbOperation
from setting import *
from common import get_currency, hround
from mongoproducer import MongoProducer
from common import get_serverdata
import json
from decimal import Decimal



def expect(setting):
    expecter = DbOperation(setting)
    return expecter.query_mongodb()

def check_none(setting):
    setting = json.dumps(setting).replace(', "', ',"')
    return get_serverdata(setting)

def reality(setting):
    sort_item = MongoProducer(setting).get_sorted()
    topn = MongoProducer(setting).get_topn()
    # topn
    #
    # only support "topn":{"metricvalue":"click","threshold":1}
    # not support "topn":{"threshold":1, "metricvalue":"click"}
    #
    if 'topn' in setting:
        del setting['topn']
        setting = json.dumps(setting).replace('", ', '",')
        topn_str = ',"topn":{"metricvalue":"%s", "threshold":%d}' % (topn[0], topn[1])
        setting = setting.rstrip('}') + topn_str + '}'
    else:
        setting = json.dumps(setting).replace('", ', '",')
    dataset = []

    data = get_serverdata(setting)

    key = data[0]
    for value in data[1:]:
        dataset.append(dict(zip(key, value)))

    if topn :
        topnlist = []
        for topitem in dataset:
            topnlist.append(topitem.get(topn[0]))
        return topnlist

    if sort_item:
        sortlist = []
        sortlen = len(sort_item)
        if sortlen == 1:
            for sortitem in dataset:
                sortlist.append(sortitem.get(sort_item[0][0]))
        elif sortlen == 2:
            for sortitem in dataset:
                sortlist.append((sortitem.get(sort_item[0][0]), sortitem.get(sort_item[1][0])))
    else:
        return dataset
    return sortlist


def currency_convert(start, end, groupitem, topnitem=None): # group by adv_id and offer_id
    success = 0
    failed = 0
    for from_currency in DIFF_CURRENCY_TYPE:
        for to_currency in DIFF_CURRENCY_TYPE:
            if from_currency == to_currency:
                continue
            rate_from_to, rate_usd_to  = get_rate(from_currency, to_currency)
            real_result = get_data(start, end, from_currency, groupitem, None, to_currency)
            expect_result = get_data(start, end, from_currency, groupitem)

            for result in expect_result:
                result['cost'] = hround(result['cost'] * rate_usd_to, 3)
                result['revenue'] = hround(result['revenue'] * rate_from_to, 3)
                result['profit'] = str(round(Decimal(result['revenue']) - Decimal(result['cost']), 3))

            for result in real_result:
                result['cost'] = str(format(result['cost']))
                result['revenue'] = str(format(result['revenue']))
                result['profit'] = str(format(result['profit']))
            if real_result == expect_result:
                success += 1
            else:
                failed += 1
    if failed == 0:
        return 'pass'
    return None

def get_data(start, end, from_currency, groupitem, topnitem=None, to_currency=None):
    if not to_currency:
        postdata = NO_CURRENCY_TYPE_POST % (start, end, from_currency, groupitem)
    else:
        postdata = CURRENCY_TYPE_POST % (start, end, from_currency, groupitem, to_currency)
    if topnitem:
        postdata = postdata.rstrip('}') + ',"topn":{0}'.format(topnitem) + '}'
    dataset = get_serverdata(postdata)
    key, values = dataset[0], dataset[1:]
    datalst = []
    for value in values:
        datalst.append(dict(zip(key, value)))
    return datalst

def get_rate(from_currency, to_currency):
    CURRENCY_TABLE = get_currency()
    for rate in CURRENCY_TABLE:
        if rate.get('currency_from') == from_currency and rate.get('currency_to') == to_currency:
            return (rate.get('rate_from_to'), rate.get('rate_usd_to'))


if __name__ == '__main__':
    print expect({"settings": {"report_id": "1402919015", "return_format": "json", "time": {"start": 1405987200, "end": 1406073600, "timezone": 0},"data_source": "ymds_druid_datasource", "pagination": {"size": 4, "page": 0}}, "filters": {"$and": {}}, "data": ["click", "conversion", "cost", "revenue", "profit"],  "group":  ["day", "hour"],"sort":[{"orderBy": "click", "order": -1}]})
    print reality({"settings": {"report_id": "1402919015", "return_format": "json", "time": {"start": 1405987200, "end": 1406073600, "timezone": 0},"data_source": "ymds_druid_datasource", "pagination": {"size": 4, "page": 0}}, "filters": {"$and": {}}, "data": ["click", "conversion", "cost", "revenue", "profit"],  "group":  ["day", "hour"],"sort":[{"orderBy": "click", "order": -1}]})