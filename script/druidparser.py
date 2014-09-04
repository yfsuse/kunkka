#! /usr/bin/env python


import json
from common import timestamp_datetime


class DruidParser(object):

    def __init__(self,  druid_settings):
        self.druid_setting = druid_settings
        self.setting = self.set_setting()

    def set_setting(self):
        try:
            json_setting = json.loads(self.druid_setting)
        except TypeError:
            return self.druid_setting
        return json_setting

    def get_time(self):
        unix_start, unix_end = self.get_unix_time()
        return timestamp_datetime(unix_start),  timestamp_datetime(unix_end)

    def get_unix_time(self):
        unix_start = self.setting.get('settings', '').get('time', '').get('start', '')
        unix_end = self.setting.get('settings', '').get('time', '').get('end', '')
        return unix_start, unix_end

    def get_datasource(self):
        try:
            return self.setting.get('settings', '').get('data_source',  '')
        except AttributeError as e:
            return None

    def get_pagesize(self):
        try:
            return self.setting.get('settings', '').get('pagination', '').get('size', '')
        except AttributeError as e:
            return None

    def get_pagenum(self):
        try:
            return self.setting.get('settings', '').get('pagination', '').get('page', '')
        except AttributeError as e:
            return None


    def get_filters(self):
        try:
            return self.setting.get('filters', '').get('$and', '')
        except AttributeError as e:
            raise AttributeError(e)
        return None

    def get_sort(self):
        try:
            return self.setting.get('sort',  '')
        except AttributeError as e:
            return None

    def get_data(self):
        return repr(self.setting.get('data',  '')).replace("'", '"')

    def get_group(self):
        return repr(self.setting.get('group',  '')).replace("'", '"')

    def get_topn(self):
        try:
            return self.setting.get('topn', '').get('metricvalue'),  self.setting.get('topn',  '').get('threshold',  '')
        except AttributeError as e:
            return None


if __name__ == '__main__':
    p = DruidParser({"settings":{"report_id":"139995","return_format":"json","time":{"start":1405987200,"end":1406073600,"timezone":0},"data_source":"ymds_druid_datasource","pagination":{"size":2,"page":2}},"filters":{"$and":{}},"sort":[{"orderBy": "click", "order": -1}],"data":["click","conversion","cost","revenue","profit"],"group":["aff_id"]})
    print p.get_time()
    print p.get_datasource()
    print p.get_pagesize()
    print p.get_pagenum()
    print p.get_filters()
    print p.get_sort()
    print p.get_data()
    print p.get_group()
    print p.get_topn()