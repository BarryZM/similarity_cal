import csv
import datetime
import elasticsearch
import json
import numpy as np
import os
import pandas as pd
import pkuseg
# from func_cal_doc_id import cal_doc_id
# from write_data_into_es.func_get_releaser_id import *
# from write_data_into_es.cal_similarity import *
from concurrent.futures import ProcessPoolExecutor
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan


# 以下为分词步骤
def file_to_list(file_name):
    if file_name.endswith('csv'):
        try:
            with open(file_name, "r", encoding='utf-8') as csv_file:
                list_out = []
                csv_r = csv.reader((line.replace('\0', '') for line in csv_file))
                for row in csv_r:
                    list_out.append(row)
                return list_out
        except:
            # print('gb_csv')
            with open(file_name, "r", encoding='gb18030') as csv_file:
                list_out = []
                csv_r = csv.reader((line.replace('\0', '') for line in csv_file))
                for row in csv_r:
                    list_out.append(row)
                return list_out
    else:
        try:
            list_out = []
            with open(file_name, "r", encoding='utf-8') as file1:
                for row in file1.readlines():
                    list_out.append(row)
            return list_out
        except:
            try:
                list_out = []
                with open(file_name, "r", encoding='utf-8') as file1:
                    for row in file1.readlines():
                        list_out.append(row)
                return list_out
            except:
                print('Can not open', file_name)
                return []


def ld_to_csv(input_dic, csv_directory, csv_name):
    with open(r'{dic_rectory}\{name}.csv'.format(dic_rectory=csv_directory, name=csv_name), 'w', newline='',
              encoding='gb18030') as csv_w:
        file = csv.writer(csv_w)
        if type(input_dic).__name__ == 'dict':
            for key in input_dic.keys():
                list_write = []
                if type(input_dic[key]).__name__ == 'list':
                    for write_value in input_dic[key]:
                        list_write.append(write_value)
                else:
                    list_write = [key, input_dic[key]]
                file.writerow(list_write)
        elif type(input_dic).__name__ == 'list':
            for key in input_dic:
                list_write = []
                for write_value in key:
                    list_write.append(write_value)
                file.writerow(list_write)


def del_lastN(input_list):
    out_list = []
    for i in range(len(input_list)):
        out_list.append(input_list[i].strip('\n'))
    return out_list


def stop_word_build():
    stop_words = del_lastN(file_to_list('stop_words'))
    stop_words_add = del_lastN(file_to_list('stop_word_add'))
    stop_words_recover = del_lastN(file_to_list('stop_word_recover'))
    stop_word = set(stop_words + stop_words_add)
    out_stop_word = [word for word in stop_word if word not in stop_words_recover]
    return out_stop_word


def list_seg(list_input):
    out_list = []
    for str_line in list_input:
        uni_str = []
        row_list = seg.cut(str_line)
        for word_cp in row_list:
            if word_cp[0] not in stop_words:
                uni_str.append(word_cp)
        out_list.append(uni_str)
    return out_list


def file_to_sum_dic(input_list):
    dic_vector = {}
    for word in input_list:
        if word not in dic_vector.keys():
            dic_vector[word] = 1
        else:
            dic_vector[word] += 1
    return dic_vector


def title_parse(input_word):
    list_word = seg.cut(input_word)
    list_new = [word for word in list_word if word not in stop_words]
    list_sum_word = file_to_sum_dic(list_new)
    return list_sum_word


class Similarity_calculator():
    def __init__(self, basic_dic):  # test_dir,
        self.train_data = basic_dic

    def bit_product_sum(self, x, y):
        return sum([item[0] * item[1] for item in zip(x, y)])

    def cosine_similarity(self, x, y, norm=False):  # """ 计算两个向量x和y的余弦相似度 """
        assert len(x) == len(y), "len(x) != len(y)"
        zero_list = [0] * len(x)
        if x == zero_list or y == zero_list:
            return float(1) if x == y else float(0)
        cos = self.bit_product_sum(x, y) / (np.sqrt(self.bit_product_sum(x, x)) * np.sqrt(self.bit_product_sum(y, y)))
        return 0.5 * cos + 0.5 if norm else cos  # 归一化到[0, 1]区间内

    def pre_vector_build(self, dic_train, dic_test):  # 输入两个字典产生相同长度向量tetc正
        list_k_topic = list(dic_train.keys())  # [:int(len(dic_train.keys())*0.8)]
        list_c_topic = [float(int_i) for int_i in list(dic_train.values())]
        list_c_test = [0] * len(list_c_topic)
        for word_key in list(dic_test.keys()):
            if word_key in list_k_topic:
                word_index = list_k_topic.index(word_key)
                word_count = dic_test[word_key]
                list_c_test[word_index] = float(word_count)
        return list_c_test, list_c_topic

    def calculate_all_basic_dic(self, input_list):
        test_w_dic = input_list
        list_similarity = []
        for basic in self.train_data:
            topic_w_dic = basic
            list_trp = self.pre_vector_build(test_w_dic, topic_w_dic)
            distance_out = self.cosine_similarity(list_trp[0], list_trp[1])
            list_similarity.append(distance_out)
        return list_similarity


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
                ], "minimum_should_match": 1, "must": [
                    {"exists": {"field": "releaser_id_str"}}
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


class Es_operator():
    def __init__(self):
        es_option = {
            'host_reader': '192.168.17.11',
            'host_writer': '192.168.6.34',
            'port_reader': 80,
            'port_writer': 9200,
            'user': 'liukang',
            'passwd_reader': 'xSEHhTRGE6AX',
            'passwd_writer': 'xSEHhTRGE6AX'
        }
        # self.http_auth = (es_option['user'], es_option['passwd'])
        self.es_reader = Elasticsearch(hosts=es_option['host_reader'], port=es_option['port_reader'],
                                       http_auth=(es_option['user'], es_option['passwd_reader']))
        self.reader_option = {
            "query": {
                "bool": {
                    "filter": [
                        {"range": {"release_time": all_month_timerange}},
                        {"range": {"duration": {"lte": 600}}},
                        {"term": {"releaser_id_str": releaser_id_str}}
                    ]
                }
            }
        }

        self.similarity_calculator = Similarity_calculator(compare_list)

    # self.parse_data = Title_parse()

    def scan_build_result(self, index):
        es_result = scan(
            client=self.es_reader,
            query=self.reader_option,
            scroll='50m',
            index=index,
            timeout="3m",
            raise_on_error=False)
        return es_result

    def title_classify(self, data_input):
        print(data_input)
        if data_input:
            # data为 input_title, inpurt_releaser='', input_channel=''
            title = data_input[0]
            try:
                releaser = data_input[1]
            except:
                releaser = ''
            try:
                channel = data_input[2]
            except:
                channel = ''
            title_cal_topic = self.title_cal_topic
            parse_data = self.parse_data
            data_parsed = parse_data.parse_title_releaser_channel(title, releaser, channel)
            print(data_parsed)
            if not data_parsed:
                return None
            if isinstance(data_parsed, str):
                return [title, data_parsed]
            if isinstance(data_parsed, dict):
                out_tags = title_cal_topic.calculate_topic(data_parsed)
                tgs_out = ','.join(out_tags)
                return [title, tgs_out]
        else:
            print('3')
            return None

    def es_title_fetch(self, num='all'):  # 获取一定数目的ES数据
        es_scan = self.scan_build_result('short-video-all-time-url-v2')
        final_result_list = []
        count_cead = 0
        write_num = 0
        for item in es_scan:
            print('mark_here')
            count_cead += 1
            try:
                es_channel = item['_source']["channel"]
            except Exception as e:
                print('Error:', e)
                es_channel = 'None'
            finally:
                final_result_list.append([item['_source']["title"], item['_source']["releaser"], es_channel])
                print(item['_source']["title"])
            if len(final_result_list) >= 10:
                write_num += 1
                print(write_num)
                title_classfied = [self.title_classify(uni_data) for uni_data in final_result_list]
                for data in title_classfied:
                    parse_data = self.title_classify(data)

                    if parse_data:
                        if parse_data[1]:
                            self.write_data_es(parse_data)
                final_result_list = []
                count_cead = 0
                if num == 'all':
                    pass
                elif write_num * 10 >= int(num):
                    break

    # title_classfied = [self.title_classify(uni_data) for uni_data in final_result_list]
    # for data in title_classfied:
    # 	parse_data = self.title_classify(data)
    # 	self.write_data_es(parse_data)


if __name__ == "__main__":
    stop_words = stop_word_build()
    seg = pkuseg.pkuseg(postag=False)
    fn = r'F:\PycharmProjects\TC\top6p.csv'
    compare_list_true = file_to_list(fn)
    compare_list = [title_parse(element[0]) for element in compare_list_true]
    # sc = Similarity_calculator(compare_list)
    # sc.calculate_all_basic_dic(title_parse('湖北省红会3领导被问责：1人被免职'))#输入字符串得到一个308个值得list
    # print(len(compare_list))
    ld_to_csv(compare_list, r'F:\PycharmProjects\Tc_application', 'seg_out')
