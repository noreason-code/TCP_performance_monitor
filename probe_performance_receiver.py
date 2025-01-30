import socket
import sys
import redis
import psutil
import time
import pickle
import threading
import os
import multiprocessing

REDIS_HOST = '172.17.0.1'
REDIS_PORT = 6379
REDIS_PASSWORD = 'admin'
REDIS_DB = 0
redis_conn = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, db=REDIS_DB)

def probe_throughput(pair):
    old = 0 #存储上一次发送的数据量
    new_eth_recv = 0
    while True:
        if redis_conn.get('stop_trans_flag') == b'True':
            break
        else:
            new = 0
            data = psutil.net_io_counters(pernic=True, nowrap=True)
            new += data['eth1'].bytes_recv
            current_bytes_recv = new - old
            tp = current_bytes_recv / 125000   #计算吞吐，并把单位换算成Mbps
            t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            print('--------------------------')
            print(t)
            print(f'throughput: {tp}')
            print('--------------------------')
            result = {
                'time': time.time(),
                'TP': tp   #1s统计一次
            }
            li_name = 'throughput_li_receiver_' + str(pair)
            pickled_data = pickle.dumps(result)
            redis_conn.rpush(li_name,pickled_data)
            old = new
            time.sleep(1)

if __name__ == '__main__':
    HOST = sys.argv[1]
    PORT = int(sys.argv[2])
    PAIR = int(sys.argv[3])
    #初始化套接字
    # ---------------------------------------------
    sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sk.bind((HOST, PORT))
    sk.listen(1)
    sk_sender, sender_addr = sk.accept()
    print(f'Accepted from sender_address: {sender_addr}')
    sk_sender.settimeout(60)
    # ---------------------------------------------
    # 开始检测网络接口吞吐
    # ----------------------------------------------------
    tp_handler = threading.Thread(target=probe_throughput, args=(PAIR,))
    tp_handler.start()
    # ----------------------------------------------------
    try:
        while True:
            if redis_conn.get('stop_trans_flag') == b'True':
                break
            else:
                data = sk_sender.recv(100000000)
    except socket.timeout:
        pass
    finally:
        sk.close()
        sk_sender.close()
