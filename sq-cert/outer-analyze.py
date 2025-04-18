
import psycopg2
import geoip2.database
from psycopg2.extras import execute_values

# === 配置项 ===
DB_CONFIG = {
    'host': '20.20.100.162',
    'port': 5432,
    'dbname': 'cert',
    'user': 'postgres',
    'password': 'Njzf1984!(*$!!!'
}
MMDB_PATH = '/home/njzf/Downloads/GeoLite2-Country.mmdb'
BATCH_SIZE = 10000

# === 连接数据库和 GeoLite2 ===
conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()
reader = geoip2.database.Reader(MMDB_PATH)

# === 创建临时表（IP + Country） ===
cursor.execute("""
    DROP TABLE IF EXISTS ip_country_temp;
    CREATE TEMP TABLE ip_country_temp (
        ip TEXT PRIMARY KEY,
        country TEXT
    );
""")
conn.commit()


# === 获取唯一 IP ===
# === 使用服务端游标逐批读取 IP ===
cursor = conn.cursor(name='ip_cursor')
cursor.execute("""
    SELECT DISTINCT ip FROM net_flow_stat2 
    WHERE country IS NULL
""")

insert_cursor = conn.cursor()
total_processed = 0

while True:
    rows = cursor.fetchmany(BATCH_SIZE)
    if not rows:
        break
    total_processed += len(rows)

    batch_data = []
    for (ip,) in rows:
        try:
            response = reader.country(ip)
            country = response.country.name or 'Unknown'
        except:
            country = 'Unknown'
        batch_data.append((ip, country))

    # 批量插入临时表
    execute_values(
        insert_cursor,
        "INSERT INTO ip_country_temp (ip, country) VALUES %s ON CONFLICT (ip) DO NOTHING",
        batch_data
        )
        
    print(f"已处理 {total_processed} 条数据")
# 提交批量插入
conn.commit()
# === 清理游标 ===
#cursor.close()
insert_cursor.close()
reader.close()
# === 更新主表 ===
with conn.cursor() as cur:
    print("🚀 开始批量更新 net_flow_stat2 表...")
    cur.execute("""
        UPDATE net_flow_stat2 AS n
        SET country = t.country
        FROM ip_country_temp AS t
        WHERE n.ip = t.ip AND n.country IS NULL;
        """)
    conn.commit()
    print("✅ 国家字段批量更新完成。")

# === 清理临时表 ===
with conn.cursor() as cur:
    cur.execute("DROP TABLE IF EXISTS ip_country_temp;")
    conn.commit()
    print("✅ 临时表已删除。")
# === 关闭连接 ===
conn.close()

# === 完成 ===
print("🚀 批量更新完成。")