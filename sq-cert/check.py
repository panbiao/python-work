
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# 数据库连接配置（请根据实际情况修改）
DB_CONFIG = {
    'user': 'postgresql',
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
        SELECT * FROM netflow_logs
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
                  (df['dest_ip'].str.match(r'^((?!192\.168|10\.|172\.(1[6-9]|2[0-9]|3[01])).)*$'))]

    zombie_candidates = []
    grouped = external.groupby(['src_ip', 'dest_ip'])
    for (src, dst), group in grouped:
        if len(group) >= 3:
            intervals = group['start_time'].sort_values().diff().dropna()
            if intervals.mean().seconds < 900:
                zombie_candidates.append((src, dst, len(group)))
    return pd.DataFrame(zombie_candidates, columns=['src_ip', 'dest_ip', 'connection_count'])

# ----------------------------
# 木马检测
# ----------------------------
def detect_trojan(df):
    high_ports = df[(df['dest_port'] > 1024) & (df['transport_protocol'].str.lower() == 'tcp')]
    common_ports = {3306, 5432, 8080}
    high_ports = high_ports[~high_ports['dest_port'].isin(common_ports)]
    suspicious = high_ports[(high_ports['send_bytes'] > 10000) | (high_ports['recv_bytes'] > 10000)]
    return suspicious[['src_ip', 'dest_ip', 'dest_port', 'send_bytes', 'recv_bytes']].drop_duplicates()

# ----------------------------
# 蠕虫检测
# ----------------------------
def detect_worm(df):
    smb = df[df['dest_port'].isin([445, 139])]
    scan_count = smb.groupby('src_ip')['dest_ip'].nunique()
    suspects = scan_count[scan_count > 10]
    return suspects.reset_index().rename(columns={'dest_ip': 'target_count'})

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

