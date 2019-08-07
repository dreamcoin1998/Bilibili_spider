# -*- coding:UTF-8 -*-

import multiprocessing
import spider_bili
import time
import random

# 生产者函数
def productor(start_mid, end_mid):
    t1 = time.time()
    for mid in range(start_mid, end_mid+1):
        info = spider_bili.getUPinfo(str(mid))
        time.sleep(random.randint(3, 4) / 10.1)  # 延迟时间，避免太快 ip 被封
        # 如果不为空，所爬取的信息为所要的信息
        if info is not None:
            # print(info[1])
            # 将信息取出来
            nickname, sex, face_url, partition, follower, upstat, mid = info[1]
            # 存入数据库
            spider_bili.writeUP(nickname, sex, face_url, partition, follower, upstat, mid)
            print('粉丝数：', follower, '数据类型：', type(follower))
            # 如果粉丝数量大于1万,则返回视频aid列表
            if follower >= 10000:
                video_info = spider_bili.getVideoInfo.delay(info[0])
                print('运行', video_info.get())
                # print(video_info.get())
                if video_info.get() is not None:
                    print('写数据库')
                    title, upstat, likes, coins, favorite, share, reply, ctime, tags, uid, danmu_num, danmu, comment = video_info.get()
                    spider_bili.writeVideo.delay(title, upstat, likes, coins, favorite, share, reply, ctime, tags, uid, danmu_num, danmu, comment)
    t2 = time.time()
    avg_timt = (t2 - t1) / 100
    print('平均运行时间', avg_timt)
    print('运行结束')


def main():
    # start_index = [1, 113554607, 340663818, 227109213]
    # end_index = [113554606, 227109212, 454218427, 340663817]
    start_index = [100000000, 100000101, 100000201, 100000301]
    end_index = [100000100, 100000200, 100000300, 100000400]
    print('你可以同时运行 %s 个爬虫程序。' % str(multiprocessing.cpu_count()))
    # pro_count = int(eval(input('你想同时运行几个爬虫程序: ')))
    # for i in range(pro_count):
    #     s = int(eval(input('这是第 %s 个爬虫程序，请输入起点id号: ' % str(i+1))))
    #     start_index.append(s)
    #     e = int(eval(input('这是第 %s 个爬虫程序，请输入终点id号: ' % str(i+1))))
    #     end_index.append(e)
    # 生产者进程
    for j in range(4):
        p = multiprocessing.Process(target=productor, args=(start_index[j], end_index[j]))
        print('进程 %s 启动' % (j+1))
        p.start()


if __name__ == '__main__':
    main()