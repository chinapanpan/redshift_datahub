import psycopg2
import boto3
import logging
from sqllineage.runner import LineageRunner
from typing import Optional, List, Dict, Any
import os
import concurrent.futures
from datetime import datetime

# 配置日志
logger = logging.getLogger()
logger.setLevel("ERROR")

# 从环境变量获取Redshift连接配置
REDSHIFT_CONFIG = {
    "host": os.environ['REDSHIFT_HOST'],
    "port": int(os.environ['REDSHIFT_PORT']),
    "database": os.environ['REDSHIFT_DATABASE'],
    "user": os.environ['REDSHIFT_USER'],
    "password": os.environ['REDSHIFT_PASSWORD']
}

project_name = os.environ['PROJECT_NAME']
bucket_name = os.environ['S3_BUCKET']
batch_size = int(os.environ.get('BATCH_SIZE', 500))
last_hours = int(os.environ.get('LAST_HOURS', 0))
query_time_cond="AND start_time >= TRUNC(SYSDATE)"
# 根据last_hours设置查询时间条件
if last_hours > 0:
    query_time_cond = f"AND start_time >= DATEADD(hour, {0-last_hours}, GETDATE())"


processed_tables = set()  # 用于记录已处理的目标表
# 用于缓存表DDL的字典
table_ddl_cache = {}
def get_table_ddl(conn, table_name: str) -> Optional[str]:
    """获取Redshift表的DDL"""
    global table_ddl_cache
    # 检查缓存中是否已存在
    if table_name in table_ddl_cache:
        print(f"从缓存获取表 {table_name} 的DDL")
        return table_ddl_cache[table_name]
    try:
        with conn.cursor() as cur:
            db_name = REDSHIFT_CONFIG['database']
            if '.' in table_name and len(table_name.split('.')) == 3:
                query = f'SHOW TABLE {table_name};'
            else:
                query = f'SHOW TABLE {db_name}.{table_name};'
            # 将默认schema '<default>'替换为'public'
            query = query.replace('<default>', 'public')

            print(f"执行查询: {query}")

            cur.execute(query)
            result = cur.fetchone()
            if not result:
                logger.warning(f"表 {table_name} 不存在或无法获取DDL")
                return None

            ddl = result[0]
            # 替换DISTSTYLE AUTO为分号
            ddl = ddl.replace("DISTSTYLE AUTO;", ";")
            # 将结果存入缓存
            table_ddl_cache[table_name] = ddl
            return ddl
    except Exception as e:
        logger.error(f"获取表 {table_name} DDL失败: {str(e)}")
        return None

# 针对部分sql被截断的场景，获取完整的SQL语句
def get_full_sql(conn, query_id: int) -> Optional[str]:
    try:
        with conn.cursor() as cur:
            query = """
            SELECT LISTAGG(text, '') WITHIN GROUP (ORDER BY sequence) AS full_query
            FROM SYS_QUERY_TEXT
            WHERE query_id = %s
            """
            cur.execute(query, (query_id,))
            result = cur.fetchone()

            if result:
                return result[0]  # 返回完整的SQL文本
            return None
    except Exception as e:
        logger.error(f"获取完整SQL失败: {str(e)}")
        return None

def count_today_insert_queries(conn) -> int:
    """获取今天执行的所有INSERT语句数量"""
    try:
        with conn.cursor() as cur:
            query = """
            SELECT count(1)
            FROM SYS_QUERY_HISTORY 
            WHERE query_type = 'INSERT'  
            """ + query_time_cond + """
            AND status = 'success'  -- 只获取执行成功的语句
            AND LOWER(query_text) not LIKE '%mv_tbl%'
            """
            cur.execute(query)
            results = cur.fetchall()

            # 直接返回查询结果的第一行第一列（count值）
            if results and len(results) > 0:
                return results[0][0]
            return 0
    except Exception as e:
        logger.error(f"获取INSERT语句数量失败: {str(e)}")
        return 0

def get_today_insert_queries(conn, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
    """获取今天执行的所有INSERT语句，返回包含多个字段的字典列表"""
    try:
        with conn.cursor() as cur:
            query = """
            SELECT query_id, transaction_id, query_text 
            FROM SYS_QUERY_HISTORY 
            WHERE query_type = 'INSERT' 
            """ + query_time_cond + """
            AND status = 'success'  -- 只获取执行成功的语句
            AND LOWER(query_text) not LIKE '%mv_tbl%'
            ORDER BY end_time DESC
            """

            limitcond=f"LIMIT {limit} OFFSET {offset};"
            cur.execute(f"{query} {limitcond}")
            results = cur.fetchall()

            # 将结果转换为字典列表
            return [
                {
                    "query_id": row[0],
                    "transaction_id": row[1],
                    "query_text": row[2]
                }
                for row in results
            ]
    except Exception as e:
        logger.error(f"获取INSERT语句失败: {str(e)}")
        return []

# 创建全局缓存字典，用于存储已解析的SQL结果
sql_cache = {}

def parseSQL(query):
    """解析SQL语句，如果SQL较长且已缓存，则直接返回缓存结果"""
    global sql_cache
    cache_key=''
    # 如果SQL长度大于200字符，使用前200字符作为缓存键
    if len(query) > 200:
        cache_key = query[:200]
        if cache_key in sql_cache:
            print("命中缓存，直接返回解析结果")
            return sql_cache[cache_key]
    else:
        cache_key = query
        if cache_key in sql_cache:
            print("命中缓存，直接返回解析结果")
            return sql_cache[cache_key]

    # 如果没有命中缓存，解析SQL并存入缓存
    result = LineageRunner(query, dialect="redshift")
    sql_cache[cache_key] = result
    return result
def save_queries_to_s3(insert_queries: List[Dict[str, Any]], batch_num: int = 1) -> None:
    """将INSERT语句保存到本地文件并上传到S3"""
    try:
        global processed_tables
        ddl_tables = set()  # 用于记录已获取过DDL的表
        # 为DDL查询创建独立连接
        with psycopg2.connect(**REDSHIFT_CONFIG) as ddl_conn:
            
            # Lambda中使用/tmp目录进行临时文件存储
            current_date = datetime.now().strftime('%Y-%m-%d')
            local_path = f'/tmp/sql_queries_{project_name}_{current_date}_batch{batch_num}.sql'

            with open(local_path, 'w', encoding='utf-8') as f:
                seq = 1
                for query_info in insert_queries:
                    try:
                        print(f"处理第{batch_num}批次第{seq}条sql")
                        seq = seq + 1
                        query = query_info["query_text"]
                        query_id = query_info["query_id"]

                        # 检查SQL长度是否超过3990字符(超过 4000 会被截断)，如果超过则获取完整SQL
                        if len(query) > 3990:
                            print(f"SQL长度超过3990字符，尝试获取完整SQL，query_id: {query_id}")
                            full_sql = get_full_sql(ddl_conn, query_id)
                            if full_sql:
                                query = full_sql
                                print("成功获取完整SQL")
                            else:
                                logger.warning(f"无法获取完整SQL，使用原始截断SQL，query_id: {query_id}")

                        # 将\n转换为实际换行符
                        query = query.replace('\\n', '\n')
                        query = f"{query};"  # 加上分号结尾

                        # 分析SQL依赖的表
                        result = parseSQL(query)
                        tables = result.source_tables
                        print(f"SQL涉及的表: {tables}")

                        # 获取目标表
                        target_tables = result.target_tables
                        print(f"目标表: {target_tables}")

                        should_skip = False

                        if target_tables:
                            table_str = target_tables[0].__str__()
                            if table_str in processed_tables:
                                print(f"目标表 {table_str} 已处理过,跳过该SQL")
                                should_skip = True
                            processed_tables.add(table_str)

                        if should_skip:
                            continue

                        # 获取每张表的DDL
                        for table in tables:
                            # 将table对象转换为字符串
                            table_name = table.__str__()
                            # 检查表是否已经获取过DDL
                            if table_name in ddl_tables:
                                print(f"表 {table_name} 的DDL已获取过,跳过")
                                continue
                            ddl_tables.add(table_name)
                            ddl = get_table_ddl(ddl_conn, table_name)

                            if ddl:  # 修复了缩进问题
                                f.write(f"\n-- DDL for table {table_name}:\n{ddl}\n")
                            else:
                                f.write(f"\n-- Failed to get DDL for table {table_name}\n")
                            f.write("-" * 80 + "\n")

                        f.write(f"{query}\n{'-' * 80}\n")
                    except Exception as e:
                        logger.error(f"处理SQL查询时出错: {str(e)}")
                        logger.error(f"出错的SQL: {query_info['query_text'][:200]}...")
                        # 继续处理下一个查询
                        continue

            # 上传到S3
            print(f"文件准备上传到S3: {local_path}")
            s3_key = f'sql-scripts/sql_queries_{project_name}_{current_date}_batch{batch_num}.sql'
            s3_client = boto3.client('s3')
            s3_client.upload_file(local_path, bucket_name, s3_key)

            print(f"查询已保存到本地文件: {local_path}")
            print(f"文件已上传到S3: s3://{bucket_name}/{s3_key}")

    except Exception as e:
        logger.error(f"保存查询失败: {str(e)}")
        raise

def process_batch(batch_num, offset, batch_size):
    """处理单个批次的查询"""
    try:
        # 创建独立连接以支持并行处理
        with psycopg2.connect(**REDSHIFT_CONFIG) as conn:
            print(f"处理第 {batch_num} 批次，偏移量: {offset}")

            # 获取当前批次的查询
            batch_queries = get_today_insert_queries(conn, batch_size, offset)
            if not batch_queries:
                logger.warning(f"第 {batch_num} 批次未获取到查询")
                return False

            # 保存当前批次的查询
            save_queries_to_s3(batch_queries, batch_num)
            print(f"第 {batch_num} 批次处理完成")
            return True
    except Exception as e:
        logger.error(f"处理第 {batch_num} 批次失败: {str(e)}")
        return False

def lambda_handler(event, context):
    """Lambda处理函数 - 并行执行批次处理"""
    try:
        # 每次执行时重置全局变量
        global processed_tables
        processed_tables = set()

        # 查询INSERT语句使用独立连接
        with psycopg2.connect(**REDSHIFT_CONFIG) as conn:
            # 先获取今日插入查询的总数
            total_queries = count_today_insert_queries(conn)
            if total_queries == 0:
                logger.warning("未找到今天的INSERT语句")
                return {
                    'statusCode': 200,
                    'body': '未找到今天的INSERT语句'
                }

            print(f"今日共有 {total_queries} 条INSERT语句")

            # 按照batch_size条一个批次处理
            global batch_size
            batch_count = (total_queries + batch_size - 1) // batch_size  # 向上取整计算批次数

            # 设置最大并行数，避免创建过多连接
            max_workers = min(10, batch_count)  # 最多10个并行任务

            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 创建任务列表
                futures = []
                for batch_num in range(1, batch_count + 1):
                    offset = (batch_num - 1) * batch_size
                    futures.append(
                        executor.submit(process_batch, batch_num, offset, batch_size)
                    )

                # 等待所有任务完成并收集结果
                completed_batches = 0
                for future in concurrent.futures.as_completed(futures):
                    if future.result():
                        completed_batches += 1

        return {
            'statusCode': 200,
            'body': f'SQL查询已成功保存到S3，共处理 {completed_batches}/{batch_count} 个批次'
        }

    except Exception as e:
        logger.error(f"生成SQL失败: {str(e)}")
        raise e
