
import elasticsearch
import datetime, json
import pandas as pd
from elasticsearch.helpers import scan
# from func_cal_doc_id import cal_doc_id
# from write_data_into_es.func_get_releaser_id import *
# from write_data_into_es.cal_similarity import *
from concurrent.futures import ProcessPoolExecutor

hosts = '192.168.17.11'
port = 80
user = 'zhouyujiang'
passwd = '**********'
http_auth = (user, passwd)
es = elasticsearch.Elasticsearch(hosts=hosts, port=port, http_auth=http_auth)

def func_turn_time(one_dict):
    try:
        one_dict["title"] = one_dict["title"].replace("\r", "").replace("\n", "")
    except:
        one_dict["title"] = ""
    fetch_time_int = int(one_dict['fetch_time'] / 1000)
    fetch_time_H = datetime.datetime.fromtimestamp(fetch_time_int).isoformat(sep=' ')
    release_time_int = int(one_dict['release_time'] / 1000)
    release_time_H = datetime.datetime.fromtimestamp(release_time_int).isoformat(sep=' ')
    duration = int(one_dict['duration'])
    duration_H = '%02d:%02d:%02d' % (duration // 3600, duration % 3600 // 60, duration % 60)
    one_dict.update({'fetch_time_H': fetch_time_H,
                     "release_time_H": release_time_H,
                     "duration_H": duration_H})
    return one_dict

def get_compare_list(key_fn):
    data_list = []
    with open(key_fn, 'r', encoding='gb18030')as f:
        head = f.readline()
        head_list = head.strip().split(',')
        for i in f:
            line_list = i.strip().split(',')
            test_dict = dict(zip(head_list, line_list))
            data_list.append(test_dict)
    return data_list

def get_key_releaser():
    search_short_body = {
        "query": {
            "bool": {
                "filter": [

                ], "should": [
                    {"match": {
                        "project_tags.keyword": "ronghe"
                    }},
                    {"match": {
                        "project_tags.keyword": "New_Customers"
                    }},
                    {"match": {
                        "project_tags.keyword": "SMG"
                    }}
                ], "minimum_should_match": 1,"must": [
        {"exists":{"field":"releaser_id_str"}}
      ]
            }
        }
    }
    releaser_id_str_list = []
    seach_re = scan(client=es, index="target_releasers",
                          query=search_short_body, scroll='3m')
    for res in seach_re:
        releaser_id_str_list.append(res["_source"]["releaser_id_str"])
    return releaser_id_str_list

def get_short_video_title(all_month_timerange, index, doc, compare_list, similer_value,releaser_id_str_list,title):
    all_list = []
    count = 0
    bulk_all_body = ""

    if True:
        error_info = ""
        count_true = 0
        all_list = []
        for releaser_id_str in releaser_id_str_list:
            search_short_body = {
            "query": {
                "bool": {
                    "filter": [
                        {"range": {"release_time": all_month_timerange}},
                        {"range": {"duration": {"lte": 600}}},
                        {"term": {"releaser_id_str":releaser_id_str}}
                    ]
                }
            }
        }
            seach_re_count = scan(client=es, index=index, doc_type=doc,
                                  query=search_short_body, scroll='3m')
            for count, res in enumerate(seach_re_count):
                print(count)
            print(res["_source"]["title"])


def split_timestap(start_timestmp,end_timestmp,split_times):
    time_range = end_timestmp - start_timestmp
    single_range = time_range / split_times
    all_month_timerange_list = []
    temp_stmap = start_timestmp
    count = 0
    while temp_stmap < end_timestmp:
        dic = {"gte": int(temp_stmap), "lt": int(temp_stmap+single_range)}
        all_month_timerange_list.append(dic)
        temp_stmap += single_range
        count += 1
    return all_month_timerange_list

if __name__ == "__main__":
    fn = r'F:\PycharmProjects\TC\top6_platform.csv'
    process = 2
    compare_list = get_compare_list(fn)
    all_month_timerange = split_timestap(1580745600000,1580832000000,process)
    print(all_month_timerange)
    releaser_id_str_list = get_key_releaser()
    print(releaser_id_str_list)
    # index_st = 'short-video-production-2019'
    # st_type = 'daily-url-2019-Q2'
    # index = 'short-video-production-2020'
    # doc = 'daily-url-2020-01-31'
    index = 'short-video-all-time-url'
    doc = 'all-time-url'
    similer_value = 0.1
    futures = []
    executor = ProcessPoolExecutor(max_workers=process)
    for count, time_range in enumerate(all_month_timerange):
        # get_short_video_title(time_range, index, doc, compare_list, similer_value,releaser_id_str_list,count)
        future = executor.submit(get_short_video_title, time_range, index, doc,compare_list, similer_value, releaser_id_str_list,count)
        # futures.append(future)
    executor.shutdown(True)