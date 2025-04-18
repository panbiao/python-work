
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# 数据库连接配置（请根据实际情况修改）
DB_CONFIG = {
    'user': 'postgres',
    'password': 'Njzf1984!(*$!!!',
    'host': 'localhost',
    'port': '5432',
    'database': 'aas'
}

# 创建数据库引擎
engine = create_engine(f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")

# ----------------------------
# 数据分段读取函数
# ----------------------------
def read_logs_by_device_and_date(engine, device_id, date_str):
    query = f"""
        SELECT device_id ,
        sip , sport , smac,dip,dport,dmac,
        transport_protocol, app_protocol, app_name,
        tcp_flag, in_bytes,out_bytes,in_pkts, out_pkts, 
        start_time, end_time, sess_id   FROM audit_net_log
        WHERE device_id = '{device_id}'
        AND start_time::date = '{date_str}'
    """
    return pd.read_sql(query, engine)

# ----------------------------
# 僵尸检测
# ----------------------------
def detect_zombie(df):
    df['start_time'] = pd.to_datetime(df['start_time'])
    df['minute'] = df['start_time'].dt.floor('min')
    external = df[(df['transport_protocol'].str.lower() == 'tcp') &
                  (df['dip'].str.match(r'^((?!192\.168|10\.|172\.(1[6-9]|2[0-9]|3[01])).)*$'))]

    zombie_candidates = []
    grouped = external.groupby(['sip', 'dip'])
    for (src, dst), group in grouped:
        if len(group) >= 3:
            intervals = group['start_time'].sort_values().diff().dropna()
            if intervals.mean().seconds < 900:
                zombie_candidates.append((src, dst, len(group)))
    return pd.DataFrame(zombie_candidates, columns=['sip', 'dip', 'connection_count'])

# ----------------------------
# 木马检测
# ----------------------------
def detect_trojan(df):
    high_ports = df[(df['dport'] > 1024) & (df['transport_protocol'].str.lower() == 'tcp')]
    common_ports = {3306, 5432, 8080}
    high_ports = high_ports[~high_ports['dport'].isin(common_ports)]
    suspicious = high_ports[(high_ports['in_bytes'] > 10000) | (high_ports['out_bytes'] > 10000)]
    return suspicious[['sip', 'dip', 'dport', 'in_bytes', 'out_bytes']].drop_duplicates()

# ----------------------------
# 蠕虫检测
# ----------------------------
def detect_worm(df):
    smb = df[df['dport'].isin([445, 139])]
    scan_count = smb.groupby('sip')['dip'].nunique()
    suspects = scan_count[scan_count > 10]
    return suspects.reset_index().rename(columns={'dip': 'target_count'})


# ----------------------------
# 端口扫描检测
# ----------------------------
def detect_port_scan(df):
    scan_df = df.groupby(['src_ip', 'dest_ip'])['dest_port'].nunique().reset_index()
    scan_df = scan_df[scan_df['dest_port'] > 20]  # 一次连接20个以上端口为异常
    scan_df = scan_df.rename(columns={'dest_port': 'port_count'})
    return scan_df

# ----------------------------
# IP扫描检测
# ----------------------------
def detect_ip_scan(df):
    ip_df = df.groupby('src_ip')['dest_ip'].nunique().reset_index()
    ip_df = ip_df[ip_df['dest_ip'] > 50]  # 一次访问超过50个IP为异常
    ip_df = ip_df.rename(columns={'dest_ip': 'ip_count'})
    return ip_df


# ----------------------------
# 主执行逻辑
# ----------------------------
if __name__ == '__main__':
    # 示例：按设备和日期读取日志
    device_ids = ['250301010341']  # 可扩展为多设备
    dates = ['2025-04-09']         # 可扩展为多个日期

    for device_id in device_ids:
        for date_str in dates:
            print(f"\n[+] 正在处理设备 {device_id} 日期 {date_str} 的数据...")
            df = read_logs_by_device_and_date(engine, device_id, date_str)

            if df.empty:
                print("[-] 无数据，跳过。")
                continue

            print("[+] 僵尸检测结果：")
            print(detect_zombie(df))

            print("\n[+] 木马检测结果：")
            print(detect_trojan(df))

            print("\n[+] 蠕虫检测结果：")
            print(detect_worm(df))

