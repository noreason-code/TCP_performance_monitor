import socket
import sys
import pickle
import time
import redis

REDIS_HOST = '172.17.0.1'
REDIS_PORT = 6379
REDIS_PASSWORD = 'admin'
REDIS_DB = 0
redis_conn = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, db=REDIS_DB)

if __name__ == '__main__':
    HOST = sys.argv[1]  #套接字ip
    PORT = int(sys.argv[2]) #套接字端口
    PAIR = int(sys.argv[3])
    FLOW_TYPE = sys.argv[4] #数据流类型
    flow_size_li = []
    # 读取flow_size
    # ---------------------------------------------------
    if FLOW_TYPE == 'campus':
        with open('new_flow_school.txt', 'r') as f:
            for line in f.readlines():
                flow_size_li.append(int(line.strip('\n')))
    elif FLOW_TYPE == 'CAIDA':
        with open('new_flow_network.txt', 'r') as f:
            for line in f.readlines():
                flow_size_li.append(int(line.strip('\n')))
    # ---------------------------------------------------
    # 初始化套接字
    # ------------------------------------------------
    sk = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sk.bind((HOST, PORT))
    sk.settimeout(60)
    sk.listen(1)
    sk_sender, sender_addr = sk.accept()
    sk_sender.settimeout(60)
    # ------------------------------------------------
    if PAIR == 0:
        t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        redis_conn.set('begin_time', time.time())
        redis_conn.set('begin_time_str', t)
        redis_conn.ltrim('handover_time', 1, 0)
    received = 0    #已经接收到的字节数
    msg = b''   #用于存储收到的字节
    try:
        i = 0   #记录收到了flow_size_li中第几个数据
        L = len(flow_size_li)   #记录flow_size_li长度，便于循环使用流大小数据
        while True:
            if redis_conn.get('stop_trans_flag') == b'True':
                break
            else:
                target = flow_size_li[i] * 1000 + 40  # Linux中使用pickle封装的额外开销是40字节
                while target > 0:
                    data = sk_sender.recv(target)
                    received = len(data)
                    msg = msg + data
                    target = target - received
                recv_time = time.time()
                try:
                    unpickled_data = pickle.loads(msg)
                    # 统计流完成时间
                    prop_latency = (recv_time - unpickled_data['t']) * 1000  # 单位换算成ms
                    print(f'fct: {prop_latency}')
                    result = {
                        'time': time.time(),
                        'FCT': prop_latency
                    }
                    li_name = 'FCT_li_' + str(PAIR)
                    pickled_data = pickle.dumps(result)
                    redis_conn.rpush(li_name, pickled_data)
                except pickle.UnpicklingError:
                    i += 1
                    if i == L:
                        i = 0
                    received = 0
                    msg = b''
                    continue
                finally:
                    i += 1
                    if i == L:
                        i = 0
                    received = 0
                    msg = b''
    except socket.timeout:
        pass
    finally:
        sk_sender.close()
        sk.close()
