#! /usr/bin/env python

import random
from setting import *
from itertools import *


class CaseProducer(object):
    def __init__(self, start=None, end=None, casetype=None):
        self.unixstart = start
        self.unixend = end
        self.casetype = casetype
        self.ordermap = {'1': 'asc',
                         '-1': 'desc'}

    def get_sort_case(self):
        '''
        {"orderBy":"click", "order":-1}, {"orderBy":"conversion", "order":1}
        :return:
        '''
        datalst = []
        dimensionlst = []
        for data_index in range(2):
            for data in combinations(DATA, data_index+1):
                datalst.append(data)
        for dimension_index in range(len(DIMENSION)):
            for dimension in combinations(DIMENSION, dimension_index+1):
                dimensionlst.append(dimension)

        casenamelst = []

        case_obj = open('sort_case.py', 'w')
        mehtod_obj = open('tmp','w')

        for datas in datalst:
            orderlst = []
            casename = ''
            for d in datas:
                ordermap = {}
                ordermap[r"orderBy"] = d
                order = (1, -1)[random.randint(0, 1)]
                ordermap["order"] = order
                orderlst.append(ordermap)
                casename += d + '_' + self.ordermap[str(order)]
            casename += '_groupby'
            for dimension in dimensionlst:
                copy_casename = casename
                tmp = []
                for d in dimension:
                    copy_casename += d
                    tmp.append(d)
                casenamelst.append(copy_casename)
                setting = SORTCASE_TEMPLATE % (self.unixstart, self.unixend, random.randint(1, 2), random.randint(1, 2), orderlst, str(tmp).replace("'", '"'))
                case_obj.writelines(copy_casename.upper() + '=' + setting + '\n')
                mehtod_obj.writelines(METHOD_STR % (copy_casename, copy_casename.upper()))
        case_obj.writelines('SORTLST = ' + str(casenamelst))
        case_obj.close()
        mehtod_obj.close()

    def get_group_case(self):
        method_obj = open('tmp', 'w')
        case_obj = open('group_case.py', 'w')
        casestrlst = []
        casenamelst = []
        for count in range(len(DIMENSION)):
            for dimension in combinations(DIMENSION, count+1):
                tmp = []
                casename = 'group_'
                for c in dimension:
                    tmp.append(c)
                    casename += c
                casestrlst.append(GROUPCASE_TEMPLATE % (str(tmp).replace("'", '"')))
                casenamelst.append(casename)

        for methodname, setting in zip(casenamelst, casestrlst):
            method = GROUP_METHOD_STR % (methodname, methodname.upper())
            method_obj.writelines(method)
            case_obj.writelines(methodname.upper() + '=' + setting + '\n')
        case_obj.writelines('GROUPLST =' + str(casenamelst))


if __name__ == '__main__':
    cp = CaseProducer(1405987200, 1406073600)
    print cp.get_sort_case()