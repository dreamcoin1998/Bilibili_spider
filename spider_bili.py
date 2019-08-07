# -*- coding: UTF-8 -*-

import multiprocessing
import time
import multiprocessing
import requests
import os
import random
import json
import xml.etree.ElementTree as ET
from app import app

from mysql import connector

user_agent  = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.81 Safari/537.36"

'''UP主信息 所需API集合'''
# 用户信息 arg: mid    res: nickname, face_url, sex
user_info = 'https://api.bilibili.com/x/space/acc/info?mid='
# 用户粉丝数 arg: vmid(mid) res: star
user_star = 'https://api.bilibili.com/x/relation/stat?vmid='
# 用户视频播放数 arg: mi   res: upstat
user_upstat = 'https://api.bilibili.com/x/space/upstat?mid='
# 视频列表 获取UP主分区
video = 'https://space.bilibili.com/ajax/member/getSubmitVideos?mid=&page=1&pagesize=100'
'''视频信息 所需API集合'''
# 视频信息
video_info = 'https://api.bilibili.com/x/web-interface/view?aid='
# 视频标签
video_tags = 'http://api.bilibili.com/x/tag/archive/tags?aid='
# 弹幕 arg: cid
danmu = 'https://api.bilibili.com/x/v1/dm/list.so?oid='
# 获取cid arg: aid(av号)
get_cid = 'https://www.bilibili.com/widget/getPageList?aid='


# 获取UP主信息
@app.task
def getUPinfo(mid: str):
    try:
        global user_agent
        res_info = requests.get(user_info + mid, headers={'User-Agent': user_agent}, timeout=1.5)
        res_star = requests.get(user_star + mid, headers={'User-Agent': user_agent}, timeout=1.5)
        res_upstat = requests.get(user_upstat + mid, headers={'User-Agent': user_agent}, timeout=1.5)
        res_video_list = requests.get('https://space.bilibili.com/ajax/member/getSubmitVideos?mid=%s&page=1&pagesize=100' % mid, headers={'User-Agent': user_agent}, timeout=1.5)
        up_info = json.loads(res_info.content)
        up_star = json.loads(res_star.content)
        up_upstat = json.loads(res_upstat.content)
        up_video_list = json.loads(res_video_list.content)
        # mid
        '''mid'''
        # 昵称
        nickname = up_info['data']['name']
        # 性别
        sex = up_info['data']['sex']
        # 头像地址
        face_url = up_info['data']['face']
        # 粉丝数
        follower = up_star['data']['follower']
        # 播放量
        upstat = up_upstat['data']['archive']['view']
        # UP主总视频数量
        count = up_video_list['data']['count']
        # 筛选出UP主所发布视频的哪个分区数量最大UP主就是属于哪个分区
        # 如果视频数量不为0
        print(mid, type(up_video_list['data']['tlist']))
        if len(up_video_list['data']['tlist']) != 0:
            type_list = [v['count'] for k, v in up_video_list['data']['tlist'].items()]
            type_lb = [v['name'] for k, v in up_video_list['data']['tlist'].items() if v['count'] == max(type_list)]
            # UP主所属分区
            partition = type_lb[0]
        else:
            partition = '无'
        # UP主所有视频的av号列表 所得为int类型的列表
        aid_list = []
        # 把第一页的aid添加到列表
        for d in up_video_list['data']['vlist']:
            aid_list.append(d['aid'])
        # 如果UP主总视频数量大于100， 把其余页aid添加到列表
        if count > 100:
            aid_list = aid_list + getVideoList(mid, count)
        print('user_info:' + mid, res_info.status_code)
        print('user_star:' + mid, res_star.status_code)
        print('user_upstat:' + mid, res_upstat.status_code)
        print('up_video_list:' + mid, res_video_list.status_code)
        return aid_list, [nickname, sex, face_url, partition, follower, upstat, mid]
    except Exception as e:
        print(mid, e)
        return None


# 将获取到的UP主信息写进数据库中
@app.task
def writeUP(nickname, sex, face_url, partition, follower, upstat, mid):
    # 连接数据库
    db = connector.connect(host='localhost', user='gaojunbin', passwd='18759799353', database='bilibili')
    # 获取游标
    yb = db.cursor()
    try:
        # SQL语句, 插入记录
        yb.execute("INSERT INTO up(nickname, sex, face_url, partitions, follower, upstat, mid) VALUES('%s', '%s', '%s', '%s', %d, %d, %d);" % (nickname, sex, face_url, partition, int(follower), int(upstat), int(mid)))
        # 提交执行
        db.commit()
        print('写UP主信息成功')
    except Exception as e:
        # 如果执行sql语句出现问题，则执行回滚操作
        db.rollback()
        print(mid, e)
        print('写入UP主信息失败')
    finally:
        # 不论try中的代码是否抛出异常，这里都会执行
        # 关闭游标和数据库连接
        yb.close()
        db.close()


# 将获取到的视频信息写进数据库中
@app.task
def writeVideo(title, upstat, like, coins, favorite, share, reply, danmau_num, ctime, mid, tag_list, danmu_data, com_list_json):
    db = connector.connect(host='localhost', user='gaojunbin', passwd='18759799353', database='bilibili')
    yb = db.cursor()
    try:
        # SQL语句, 插入记录
        yb.execute("INSERT INTO video(title, upstat, likes, coins, favorite, share, reply, ctime, tags, uid, danmu_num, danmu, comment) VALUES('%s', %d, %d, %d, %d, %d, %d, %d, '%s', %d, %d, '%s', '%s');" % (title, int(upstat), int(like), int(coins), int(favorite), int(share), int(reply), ctime, tag_list, int(mid), int(danmau_num), danmu_data, com_list_json))
        # 提交执行
        db.commit()
        print('写入视频信息成功')
    except Exception as e:
        # 如果执行sql语句出现问题，则执行回滚操作
        db.rollback()
        print(mid, e)
        print('写入UP主信息失败')
    finally:
        # 不论try中的代码是否抛出异常，这里都会执行
        # 关闭游标和数据库连接
        yb.close()
        db.close()


# 获取UP主所有的视频列表
@app.task
def getVideoList(mid, count):
    # 总页数
    pages = count // 100 + 2
    aid_list = []
    for i in range(2, pages):
        res_video_list = requests.get('https://space.bilibili.com/ajax/member/getSubmitVideos?mid=%s&page=%s&pagesize=100' % (mid, str(i)), headers={'User-Agent': user_agent}, timeout=1.5)
        up_video_list = json.loads(res_video_list.content)
        for d in up_video_list['data']['vlist']:
            aid_list.append(d['aid'])
    return aid_list


# 获取视频信息
@app.task
def getVideoInfo(aid_list: list):
    try:
        for i in aid_list:
            res_info = requests.get(video_info + str(i), headers={'User-Agent': user_agent}, timeout=1.5)
            res_tags = requests.get(video_tags + str(i), headers={'User-Agent': user_agent}, timeout=1.5)
            time.sleep(random.randint(3, 4) / 10.1)  # 延迟时间，避免太快 ip 被封
            info = json.loads(res_info.content)
            sol_tags = json.loads(res_tags.content)
            # 数据
            # 标题
            title = info['data']['title']
            # 播放量
            upstat = info['data']['stat']['view']
            # 点赞量
            like = info['data']['stat']['like']
            # 投币量
            coins = info['data']['stat']['coin']
            # 收藏量
            favorite = info['data']['stat']['favorite']
            # 分享量
            share = info['data']['stat']['share']
            # 评论数
            reply = info['data']['stat']['reply']
            # 弹幕数
            danmau_num = info['data']['stat']['danmaku']
            # 发布时间 时间戳
            ctime = info['data']['ctime']
            # 作者mid
            mid = info['data']['owner']['mid']
            # 标签
            tags = sol_tags['data']
            tag_list = []
            for tag in tags:
                tag_list.append(tag['tag_name'])
            tag_list = json.dumps(tag_list)    # 标签列表 json
            print(tag_list)
            # 获取cid列表
            cid_list = getCid(str(i))
            print('cid:', cid_list)
            # 弹幕列表
            # danmu_list = []
            # 根据cid获取弹幕XML文件,并解析然后添加到弹幕列表
            # for j in cid_list:
            #     res_danmu = requests.get(danmu + j, headers={'User-Agent': user_agent}, timeout=1.5)
            #     dm = parseXml(res_danmu.content, j)
            #     danmu_list = danmu_list + dm
            # 弹幕列表 json类型
            # danmu_data = json.dumps(danmu_list)
            danmu_data = '无'
            # 获取评论列表
            # com_list = getAllCommentList(i)
            # if com_list is not None:
            #     com_list_json = json.dumps(com_list)    # 评论列表 json类型
            # else:
            #     com_list_json = json.dumps('无')
            com_list_json = '无'
            print('res_info:', res_info.status_code)
            print('res_tags', res_tags.status_code)
            print('调用成功')
            # print(title, upstat, like, coins, favorite, share, reply, danmau_num, ctime, mid, tag_list, danmu_data, com_list_json)
            return title, upstat, like, coins, favorite, share, reply, danmau_num, ctime, mid, tag_list, danmu_data, com_list_json
    except Exception as e:
        print(e)
        return None


# 获取视频cid
@app.task
def getCid(aid: str):
    res_getcid = requests.get(get_cid + aid, headers={'User-Agent': user_agent}, timeout=1.5)
    # print(res_getcid.content)
    sol_getcid = json.loads(res_getcid.content)
    cid_list = []
    for i in sol_getcid:
        cid_list.append(str(i['cid']))
    print('成功获取cid')
    return cid_list


# 解析弹幕XML文件
@app.task
def parseXml(xml_file, cid):
    with open(cid + '.xml', 'wb') as f:
        f.write(xml_file)
    danmu = []
    root = ET.parse(cid + '.xml')
    root = root.getroot()
    for child in root:
        if child.tag == 'd':
            danmu.append(child.text)
    print("成功解析xml")
    return danmu


# 获取评论列表
@app.task
def getAllCommentList(aid):
    url = "http://api.bilibili.com/x/reply?type=1&oid=" + str(aid) + "&pn=1&nohot=1&sort=0"
    r = requests.get(url)
    numtext = r.text
    json_text = json.loads(numtext)
    print(json_text['code'] == 12002)
    if json_text['code'] != 12002:
        commentsNum = json_text["data"]["page"]["count"]
        page = commentsNum // 20 + 2
        comment_list = []
        print(page)
        for n in range(1,page):
            time.sleep(random.randint(3, 4) / 10.1)  # 延迟时间，避免太快 ip 被封
            print(n)
            url = "https://api.bilibili.com/x/v2/reply?jsonp=jsonp&pn="+str(n)+"&type=1&oid="+str(aid)
            req = requests.get(url)
            text = req.text
            json_text_list = json.loads(text)
            for i in json_text_list["data"]["replies"]:
                comment_list.append(i["content"]["message"])
        # print(comment_list)
        return comment_list
    else:
        return None