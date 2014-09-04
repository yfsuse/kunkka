#!  /usr/bin/env python


from setting import *
from druidparser import DruidParser
from mongoproducer import MongoProducer
from common import get_serverdata
import pymongo


class DbOperation(object):
    def __init__(self, setting):
        self.setting = setting
        self.dataset = []
        self.parser = DruidParser(self.setting)
        self.producer = MongoProducer(self.setting)
        self.dbhost = DATABASES['default']['HOST']
        self.dbport = int(DATABASES['default']['PORT'])
        self.raw_pagenum = self.parser.get_pagenum()
        self.filter_item = self.producer.get_filtered()
        self.raw_sort_item = self.parser.get_sort()
        self.sort_item = self.producer.get_sorted()
        self.pagesize, self.pagenum = self.producer.get_paged()
        self.topn = self.producer.get_topn()
        self.set_setting()
        self.get_dataset()
        self.init_mongodb()
        self.save_mongodb()

    def set_setting(self):
        """
        convert

        {"settings": {"report_id": "1402919015", "return_format": "json", "time": {"start": 1405382400, "end": 1405641600, "timezone": 0}, "data_source": "ymds_druid_datasource", "pagination": {"size": 1000, "page": 1}}, "filters": {"$and": {"offer_id": {"$eq": 14044}, "click": {"$gt": 1000}}}, "sort": [{"orderBy": "click", "order": 1}, {"orderBy": "profit", "order": 1}], "data": ["click", "conversion", "cost", "revenue", "profit"],  "group":  ["offer_id", "aff_id"], "topn": {"metricvalue": "click", "threshold": 5}}

        to

        {"settings": {"report_id": "1402919015", "return_format": "json", "time": {"start": 1405382400, "end": 1405641600, "timezone": 0}, "data_source": "ymds_druid_datasource", "pagination": {"size": 1000, "page": 1}}, "filters": {"$and": {}}, "sort": [], "data": ["click", "conversion", "cost", "revenue", "profit"],  "group":  ["offer_id", "aff_id"]}

        """
        template = '{"settings": {"report_id": "1402919015", "return_format": "json", "time": {"start": %d, "end": %d, "timezone": 0},"data_source": "ymds_druid_datasource", "pagination": {"size": %d, "page": 0}}, "filters": {"$and": {}}, "data": %s,  "group":  %s}'
        unix_start, unix_end = self.parser.get_unix_time()

        if self.raw_pagenum == 0:
            self.raw_sort_item = []
        else:
            pass

        self.query = template % (
            unix_start,
            unix_end,
            self.parser.get_pagesize() * (self.parser.get_pagenum() + 1),
            self.parser.get_data(),
            self.parser.get_group()
        )

        self.query = self.query.rstrip('}') + ',"sort":{0}'.format(str(self.raw_sort_item).replace("'", '"')) + '}'

    def get_dataset(self):
        data = get_serverdata(self.query)
        if not data:
            self.dataset = []
        else:
            key = data[0]
            for value in data[1:]:
                self.dataset.append(dict(zip(key, value)))

    def init_mongodb(self):
        try:
            conn = pymongo.Connection(self.dbhost, self.dbport)
        except Exception as e:
            raise e
        else:
            dbhandler = conn.mercurial
            self.collectionhandler = dbhandler.mercurial_tmp
            self.collectionhandler.remove({})

    def save_mongodb(self):
        for data in self.dataset:
            self.collectionhandler.insert(data)

    def query_mongodb(self):

        if self.topn:
            # when topn exists, sort will not appear
            self.sort_item = [(self.topn[0], -1)]
            self.pagesize = self.topn[1]
        if self.sort_item:
            dataset = self.collectionhandler.find(self.filter_item).sort(self.sort_item).limit(self.pagesize).skip(self.pagenum)
        else:
            dataset = self.collectionhandler.find(self.filter_item).limit(self.pagesize)
        datalist = []
        for data in dataset:
            del data['_id']
            datalist.append(data)

        if self.sort_item:
            sortlist = []
            sortlen = len(self.sort_item)
            if sortlen == 1:
                # one sort key
                for sortitem in datalist:
                    sortlist.append(sortitem.get(self.sort_item[0][0]))
            elif sortlen == 2:
                # only support two key
                for sortitem in datalist:
                    sortlist.append((sortitem.get(self.sort_item[0][0]), sortitem.get(self.sort_item[1][0])))
            return sortlist
        else:
            return datalist

if __name__ == '__main__':
    p = DbOperation(TOPN_CASE_1)
    print p.query_mongodb()


