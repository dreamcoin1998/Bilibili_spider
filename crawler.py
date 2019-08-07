#!/usr/bin/python
# -*- coding: UTF-8 -*-

import multiprocessing
import time
import csv
import requests
import os
import random


#总体思路为，用requests模块抓取https://api.bilibili.com/x/web-interface/view?aid={}里的视频数据，再送进csv文件

user_agent  = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.81 Safari/537.36"

def run(url,num,writer):
    global user_agent
    status = 0

    try:
        information = requests.get(url,headers ={'User-Agent': user_agent},timeout = 1.5)
        status = information.status_code
        infor = information.json()
        
        #提取视频信息
        aid         = infor["data"]["aid"]
        tid         = infor["data"]["tid"]
        tname       = infor["data"]["tname"]
        title       = infor["data"]["title"]
        pubdate     = infor["data"]["pubdate"]
        duration    = infor["data"]["duration"]
        owner_id    = infor["data"]["owner"]["mid"]
        owner_name  = infor["data"]["owner"]["name"]
        view        = infor["data"]["stat"]["view"]
        danmaku     = infor["data"]["stat"]["danmaku"]
        reply       = infor["data"]["stat"]["reply"]
        favorite    = infor["data"]["stat"]["favorite"]
        coin        = infor["data"]["stat"]["coin"]
        share       = infor["data"]["stat"]["share"]
        like        = infor["data"]["stat"]["like"]
        dislike     = infor["data"]["stat"]["dislike"]

        infor_row = [aid,tid,tname,title,pubdate,duration,owner_id,owner_name,view,danmaku,reply,favorite,coin,share,like,dislike]
        #写入csv
        writer.writerow(infor_row)
        time.sleep(random.randint(3,4)/10.1)  # 延迟时间，避免太快 ip 被封
        print(status)
        
    except:
            print(status)
            time.sleep(random.randint(3,4)/10.1)  # 延迟时间，避免太快 ip 被封

def start(num,start_index,interval,file_size,file_amount,file_family_name):

    si      = start_index[num]
    inte    = interval[num]
    fs      = file_size[num]
    fa      = file_amount[num]

    print("启动爬虫，开始爬取数据")

    #循环次数
    length_j = fa
    length_i = fs

    #大循环，将信息送入不同的csv文件
    for j in range(length_j):
        file_name = "b_site_"+file_family_name+"_pro"+str(num+1)+"_no_={}.csv".format(j+1)
        csvFile = open(file_name, "a",newline = "")
        writer = csv.writer(csvFile)
        print("打开csv文件   文件名："+file_name)
        
        #记录开始时间
        start_time = time.time()

        #小循环，将递增av号的视频信息送进某一文件
        for i in range(length_i):
            url = "https://api.bilibili.com/x/web-interface/view?aid={}".format(si+j*inte*length_i+i*inte+3)
            run(url,num,writer)
            if (i % 100 == 1):
                time.sleep(random.randint(10,20)/10.1)
        
        #记录结束时间
        end_time = time.time()

        #输出平均抓取频率
        print('\npro{}'.format(num+1)+'_Frequency is : %.2f'%(length_i/(end_time - start_time))+'/s\n')
        csvFile.close()
        
        #长时间延迟，避免封IP
        print("Sleeping")
        time.sleep(random.randint(100,200)/15.1)

        if (j%10 ==1):
            print("Sleeping")
            time.sleep(random.randint(10,20))
        if (j%30 == 1):
            print("Sleeping")
            time.sleep(random.randint(10,20))
def init():

    start_index =[]
    interval    = []
    file_size   = []
    file_amount = []
    file_family_name    = ""

    print("\n\n版权所有：“不知道我在哪儿我是谁但要杀光敌人”小组\n")
    print("########################################################################\n\
注意事项:\n\
1.爬虫速度过快会被封IP，估计速度上限高于 8条数据/秒 ，请勿超过太多。\
速度可由各同时运行的爬虫给出的抓取频率相加而得,抓取频率将在每次文件保存完毕后出现。\n")
    print("2.采集的数据依次为：av号、分区号、二级分区号、标题、发布时间、视频时长、up主ID、up主昵称、\
播放量、弹幕量、回复量、收藏量、投币量、分享量、喜欢量、不喜欢量。\n")
    print("3.本程序将同时运行多个爬虫(我同时运行4个),并在某一个爬虫进程内将数据分别存入不同的csv文件中。\n")
    print("4.本程序无正常退出方式，只能等待其完成任务自行退出。强行关闭程序将导致当前正在写入的单个文件数据全部丢失。\n\
但是不会影响已写完保存的文件。这也是将数据放入许多不同文件的原因。\n")
    print("5.运行中若出现“200”或“0”字样，则代表程序运转正常，若出现“403”字样，则代表由于抓取速度过快，你的IP已被网站封掉。\n\
请减小同时运行的爬虫个数，并等待一段时间直至IP解封(一个多小时左右)，或切换wifi以更改新的IP。\
查看解封与否可通过在浏览器输入   https://api.bilibili.com/x/web-interface/view?aid=7  来判断。\
若看见   {code:0,message:0,ttl:1,data:{aid:7,videos:1    等字样，代表已解封，其余页面均为未解封。\n")
    print("6.程序中安排了周期性的休眠时间，这是反-反爬虫策略，不是程序卡了(当然如果几十秒都不动，那就是真的卡了……)\
如果你想修改休眠时长，请修改源代码中的time.sleep()参数")
    print("7.文件打开方式为a,即添加，所以注意不要让新一次的程序运行生成的数据接着写入旧文件中\n########################################################################")
    file_family_name = str(input("\n请输入你希望该次运行所生成的文件均包含的名称(最好为英文、数字、下划线)：    "))

    print("\n你可以同时运行  {}  个爬虫程序".format(multiprocessing.cpu_count()))
    cpu_core_num    = int(input("\n你想同时运行几个爬虫？    ：  "))

    for j in range(cpu_core_num):
        print("\n############\n这是第{}个爬虫程序的设置，以下参数仅对该爬虫有效:".format(j+1))
        a = int(eval(input("\n   请输入抓取的视频av号起点   ：  ")))
        #start_index.append(int(eval(input("   请输入抓取的视频av号起点   ：  "))))
        start_index.append(a)
        interval.append(int(input("   请输入抓取的相邻视频av号之差  ：  ")))
        file_size.append(int(input("   请输入单个文件储存的数据数量 ；")))
        file_amount.append(int(input("   请输入想要保存的文件总数   ：  ")))

    for i in range(cpu_core_num):
        p = multiprocessing.Process(target=start, args=(i,start_index,interval,file_size,file_amount,file_family_name,))
        print('process start')
        p.start()

if __name__ == "__main__":
    init()
