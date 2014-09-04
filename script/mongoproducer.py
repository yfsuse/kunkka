#! /usr/bin/env python


from druidparser import DruidParser
import json


class MongoProducer(object):
    def __init__(self,  setting):
        self.__setting = setting
        self.get_parser()

    def get_parser(self):
        self.parser = DruidParser(self.__setting)
        self.time = self.parser.get_time()
        self.tablename = self.parser.get_datasource()
        self.pagesize = self.parser.get_pagesize()
        self.pagenum = self.parser.get_pagenum()
        self.filters = self.parser.get_filters()
        self.sort = self.parser.get_sort()
        self.data = self.parser.get_data()
        self.group = self.parser.get_group()
        self.topn = self.parser.get_topn()

    def get_filtered(self):
        if not self.filters:
            return None
        filtered = {}
        for key in self.filters:
            value = self.filters.get(key)
            if '$eq' in value:
                value = str(value.get('$eq'))
            filtered[key] = value
        return filtered

    def get_sorted(self):
        if not self.sort:
            return None
        print self.sort
        sorted = []
        for order_rule in self.sort:
            try:
                order_rule = json.loads(order_rule)
            except TypeError as e:
                pass
            sorted.append((order_rule.get('orderBy'), order_rule.get('order')))
        return sorted

    def get_topn(self):
        return self.topn

    def get_paged(self):
        if self.pagenum == 0:
            return self.pagesize, self.pagenum
        pagesizes = (self.pagenum + 1) * self.pagesize
        return pagesizes, pagesizes - self.pagesize

    def set_sql(self):
        """
        normal:
            "data":["name",  "age"] -> db.userInfo.find({},  {"name": 1,  "age": 1}) -> select name,  age from userInfo;

        filters:
            "filters":{"$and":{"age":{"$eq":22}}} -> db.userinfo.find({"age":22}) -> select * from userinfo where age = 22
            "filters":{"$and":{"age":{"$gt":22}}} -> db.userinfo.find({"age":{"$gt":22}}) -> select * from userinfo where age > 22
            "filters":{"$and":{"age":{"$lt":22}}} -> db.userinfo.find({"age":{"$lt":22}}) -> select * from userinfo where age < 22
            "filters":{"$and":{"age":{"$gte":22}}} -> db.userinfo.find({"age":{"$gte":22}}) -> select * from userinfo where age >= 22
            "filters":{"$and":{"age":{"$lte":22}}} -> db.userinfo.find({"age":{"$lte":22}}) -> select * from userinfo where age <= 22
            "filters":{"$and":{"age":{"$gte":23}, "age":{"$lte":26}}} -> db.userInfo.find({"age":{"$gte":23, "$lte":26}}) -> select * from userinfo where age >=23 and age <= 26
            "filters":{"$and":{"age":{"$gte":23}, "age":{"$lte":26}}} -> db.userInfo.find({"name":/mongo/}) -> select * from userInfo where name like '%mongo%'
            "filters":{"$and":{"name":{"$js":"function(x){return(x.indexOf('mongo') == 0)}"}}} -> db.userInfo.find({"name":/^mongo/}) -> select * from userInfo where name like 'mongo%'
            "data":["name",  "age"], "filters":{"$and":{"age":{"$eq":22}, "name":{"$eq":zhangsan}}} -> db.userInfo.find({"name":'zhangsan', "age": 22}) -> select * from userInfo where name = 'zhangsan' and age = 22;
            "data":["name",  "age"], "filters":{"$and":{"age":{"$gt":22}}} -> db.userInfo.find({"age":{"$gt": 25}},  {"name": 1,  "age": 1}) -> select name,  age from userInfo where age >25

        sort:
            "sort":[{"orderBy":"age", "order":1}] -> db.userInfo.find().sort({"age": 1}) -> select * from  userInfo order by age asc
            "sort":[{"orderBy":"age", "order":-1}] -> db.userInfo.find().sort({"age": -1}) -> select * from  userInfo order by age desc

        topn:
            "topn":{"metricvalue":"click", "threshold":5} -> db.userInfo.find().sort({"click": 1}).limit(5) -> select * from  userInfo limit 5

        page:
            "pagination":{"size":5, "page":1} -> db.userInfo.find().limit(10).skip(5) -> select * from  userInfo limit 5, 10

        currency:
            new cost = old cost * rate_usd_to
            new_revenue = old_revenue * rate_from_to
            new_profit = new_revenue - new_cost
        """


if __name__ == '__main__':
    p = MongoProducer({"settings":{"report_id":"139995","return_format":"json","time":{"start":1404172800,"end":1406505600,"timezone":0},"data_source":"ymds_druid_datasource","pagination":{"size":2,"page":2}},"filters":{"$and":{}},"sort":['{"orderBy": "click", "order": 1}'],"data":["click","conversion","cost","revenue","profit"],"group":["adv_id"]})
    print p.get_filtered()
    print p.get_sorted()
    print p.get_paged()

