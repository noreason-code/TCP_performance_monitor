import pickle
import socket
import sys
import redis
import psutil
import threading
import time
import ctypes
import os
import re

REDIS_HOST = '172.17.0.1'
REDIS_PORT = 6379
REDIS_PASSWORD = 'admin'
REDIS_DB = 0
redis_conn = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, db=REDIS_DB)

class tcp_info(ctypes.Structure):
    _fields_ = [
        ("tcpi_state", ctypes.c_uint8),
        ("tcpi_ca_state", ctypes.c_uint8),
        ("tcpi_retransmits", ctypes.c_uint8),
        ("tcpi_probes", ctypes.c_uint8),
        ("tcpi_backoff", ctypes.c_uint8),
        ("tcpi_options", ctypes.c_uint8),
        ("tcpi_snd_wscale", ctypes.c_uint8),
        ("tcpi_rcv_wscale", ctypes.c_uint8),
        ("tcpi_rto", ctypes.c_uint32),
        ("tcpi_ato", ctypes.c_uint32),
        ("tcpi_snd_mss", ctypes.c_uint32),
        ("tcpi_rcv_mss", ctypes.c_uint32),
        ("tcpi_unacked", ctypes.c_uint32),
        ("tcpi_sacked", ctypes.c_uint32),
        ("tcpi_lost", ctypes.c_uint32),
        ("tcpi_retrans", ctypes.c_uint32),
        ("tcpi_fackets", ctypes.c_uint32),
        ("tcpi_last_data_sent", ctypes.c_uint32),
        ("tcpi_last_ack_sent", ctypes.c_uint32),
        ("tcpi_last_data_recv", ctypes.c_uint32),
        ("tcpi_last_ack_recv", ctypes.c_uint32),
        ("tcpi_pmtu", ctypes.c_uint32),
        ("tcpi_rcv_ssthresh", ctypes.c_uint32),
        ("tcpi_rtt", ctypes.c_uint32),
        ("tcpi_rttvar", ctypes.c_uint32),
        ("tcpi_snd_ssthresh", ctypes.c_uint32),
        ("tcpi_snd_cwnd", ctypes.c_uint32),
        ("tcpi_advmss", ctypes.c_uint32),
        ("tcpi_reordering", ctypes.c_uint32),
        ("tcpi_rcv_rtt", ctypes.c_uint32),
        ("tcpi_rcv_space", ctypes.c_uint32),
        ("tcpi_pacing_rate", ctypes.c_uint64),
        ("tcpi_max_pacing_rate", ctypes.c_uint64)
    ]


def probe_bbr_info(pair):

    # 定义一个用于获取BBR带宽的命令行字符串
    # command_get_bbr_bw = "ss -t -i | grep -A 1 173.18 | grep bbr | awk \'BEGIN{FS=\"bw:\"}{print $2}\' | awk \'BEGIN{FS=\"Mbps\"}{print $1}\'"
    # 定义一个用于获取BBR pacing_gain 的命令行字符串
    # command_get_bbr_pacing_gain = "ss -t -i | grep -A 1 173.18 | grep bbr | awk \'BEGIN{FS=\"pacing_gain:\"}{print $2}\' | awk \'BEGIN{FS=\",cwnd_gain\"}{print $1}\'"
    # 定义一个用于获取BBR pacing_rate 的命令行字符串
    # command_get_bbr_pacing_rate = "ss -t -i | grep -A 1 173.18 | grep bbr | awk \'BEGIN{FS=\"pacing_rate \"}{print $2}\' | awk \'BEGIN{FS=\"Mbps\"}{print $1}\'"
    # 使用ss命令获取 bbr socket statistics 信息
    command_get_bbr_info_str = "xx"

    while True:
        if redis_conn.get('stop_trans_flag') == b'True':
            break  # 如果收到停止标志，则退出循环
        else:
            # 执行命令行字符串，并获取结果
            bw = ''
            pacing_gain = ''
            pacing_rate = ''
            delivery_rate = ''

            bbr_info_str = os.popen(command_get_bbr_info_str).read().strip()
            if re.search(r'bw:([0-9.]+)', bbr_info_str):
                bw = re.search(r'bw:([0-9.]+)', bbr_info_str).group(1)
            if re.search(r'pacing_gain:([0-9.]+)', bbr_info_str):
                pacing_gain = re.search(r'pacing_gain:([0-9.]+)', bbr_info_str).group(1)
            if re.search(r'pacing_rate ([0-9.]+)', bbr_info_str):
                pacing_rate = re.search(r'pacing_rate ([0-9.]+)', bbr_info_str).group(1)
            if re.search(r'delivery_rate ([0-9.]+)', bbr_info_str):
                delivery_rate = re.search(r'delivery_rate ([0-9.]+)', bbr_info_str).group(1)
            current_time_s = time.time()
            if bw != '' and len(bw) <= 6:
                result_bw = {
                    "time": current_time_s,
                    "bw": float(bw)
                }
                li_name = "bbr_bw_li_" + str(pair)
                redis_conn.rpush(li_name, pickle.dumps(result_bw))  # 将结果添加到列表的末尾
            if pacing_gain != '' and len(pacing_gain) <= 4:
                result_pg = {
                    "time": current_time_s,
                    "pacing_gain": float(pacing_gain)
                }
                li_name = "bbr_pacing_gain_li_" + str(pair)
                redis_conn.rpush(li_name, pickle.dumps(result_pg))
            if pacing_rate != '' and len(pacing_rate) <= 6:
                result_ra = {
                    "time": current_time_s,
                    "pacing_rate": float(pacing_rate)
                }
                li_name = "bbr_pacing_rate_li_" + str(pair)
                redis_conn.rpush(li_name, pickle.dumps(result_ra))
            if delivery_rate != '' and len(delivery_rate) <= 6:
                result_dr = {
                    "time": current_time_s,
                    "delivery_rate": float(delivery_rate)
                }
                li_name = "bbr_delivery_rate_li_" + str(pair)
                redis_conn.rpush(li_name, pickle.dumps(result_dr))

            time.sleep(0.05)


def probe_throughput(pair):
    old = 0     # 存储上一次发送的数据量
    while True:
        if redis_conn.get('stop_trans_flag') == b'True':
            break
        else:
            # 获取接口名
            new = 0
            new_eth_sent = 0
            data = psutil.net_io_counters(pernic=True, nowrap=True)
            new += data['eth1'].bytes_sent
            current_bytes_sent = new - old
            tp = current_bytes_sent / 125000   # 计算吞吐，并把单位换算成Mbps
            current_time_s = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            print('--------------------------')
            print(current_time_s)
            print(f'throughput: {tp}')
            print('--------------------------')
            result = {
                'time': time.time(),
                'TP': tp * 2  # 因为0.5s统计一次所以*2
            }
            li_name = 'throughput_li_sender_' + str(pair)
            pickled_data = pickle.dumps(result)
            redis_conn.rpush(li_name, pickled_data)
            old = new
            time.sleep(0.5)

def probe_cwnd_and_rtt(sk, pair):
    tcp_info_struct = tcp_info()
    tcp_info_size = ctypes.sizeof(tcp_info_struct)
    while True:
        if redis_conn.get('stop_trans_flag') == b'True':
            break
        else:
            tcp_info_raw = sk.getsockopt(socket.IPPROTO_TCP, socket.TCP_INFO, tcp_info_size)
            ctypes.memmove(ctypes.byref(tcp_info_struct), tcp_info_raw, tcp_info_size)
            cwnd = tcp_info_struct.tcpi_snd_cwnd
            unacked = tcp_info_struct.tcpi_unacked
            pacing_rate = tcp_info_struct.tcpi_pacing_rate
            max_pacing_rate = tcp_info_struct.tcpi_max_pacing_rate
            rtt = tcp_info_struct.tcpi_rtt / 1000  # 将微秒转换为毫秒
            t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            print('----------------------------')
            print(t)
            print(f'cwnd: {cwnd}  , rtt: {rtt}, unacked: {unacked}')
            print('----------------------------')
            res_time_s = time.time()
            result_cwnd = {
                'time': res_time_s,
                'cwnd': cwnd
            }
            result_rtt = {
                'time': res_time_s,
                'rtt': rtt
            }
            result_unacked = {
                'time': res_time_s,
                'unacked': unacked
            }
            result_pr = {
                'time': res_time_s,
                'pr': pacing_rate
            }
            result_mpr = {
                'time': res_time_s,
                'mpr': max_pacing_rate
            }
            pickled_data_cwnd = pickle.dumps(result_cwnd)
            pickled_data_rtt = pickle.dumps(result_rtt)
            pickled_data_unacked = pickle.dumps(result_unacked)
            pickled_data_pr = pickle.dumps(result_pr)
            pickled_date_mpr = pickle.dumps(result_mpr)
            li_name_cwnd = 'cwnd_li_' + str(pair)
            li_name_rtt = 'rtt_li_' + str(pair)
            li_name_unacked = 'unacked_li_' + str(pair)
            li_name_pr = 'pr_li_' + str(pair)
            li_name_mpr = 'mpr_li_' + str(pair)
            redis_conn.rpush(li_name_cwnd, pickled_data_cwnd)
            redis_conn.rpush(li_name_rtt, pickled_data_rtt)
            redis_conn.rpush(li_name_unacked, pickled_data_unacked)
            redis_conn.rpush(li_name_pr, pickled_data_pr)
            redis_conn.rpush(li_name_mpr, pickled_date_mpr)
            time.sleep(0.05)

if __name__ == '__main__':
    HOST = sys.argv[1]
    PORT = int(sys.argv[2])
    PAIR = int(sys.argv[3])
    # 初始化时间戳
    if PAIR == 0:
        t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        redis_conn.set('begin_time', time.time())
        redis_conn.set('begin_time_str', t)
        redis_conn.ltrim('handover_time', 1, 0)
    # 创建套接字
    # ----------------------------------------------------
    sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sk.settimeout(60)
    sk.connect((HOST, PORT))
    # ----------------------------------------------------
    msg = b'x' * 4000000  # 每次发送4MB数据
    # 开始监测bw（只有在使用BBR时才需要使用以下线程）
    # ------------------------------------------------------
    bw_handler = threading.Thread(target=probe_bbr_info, args=(PAIR,))
    bw_handler.start()
    # ------------------------------------------------------
    #开始检测网络接口吞吐
    # ----------------------------------------------------
    tp_handler = threading.Thread(target=probe_throughput, args=(PAIR,))
    tp_handler.start()
    # ----------------------------------------------------
    #开始监测cwnd和rtt
    # ----------------------------------------------------
    cwnd_and_rtt_handler = threading.Thread(target=probe_cwnd_and_rtt, args=(sk, PAIR))
    cwnd_and_rtt_handler.start()
    # ----------------------------------------------------

    try:
        while True:
            if redis_conn.get('stop_trans_flag') == b'True':
                break
            else:
                sk.sendall(msg)
                # time.sleep(0.01)
    except socket.timeout:
        pass
    finally:
        sk.close()
