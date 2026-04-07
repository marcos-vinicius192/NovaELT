from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

table = "yt_api"

def get_conn_cursor():
    hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt", database="elt_db")
    conn = hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
def close_conn_cursor(conn, cur):
    cur.close()
    conn.close()
    
def create_schema(schema):
    conn, cur = get_conn_cursor()
    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema}"
    cur.execute(schema_sql)
    conn.commit()
    close_conn_cursor(conn, cur)
    
def create_table(schema):
    conn, cur = get_conn_cursor()
    if schema == "staging":
        table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                id SERIAL PRIMARY KEY,
                video_id VARCHAR(255),
                title TEXT,
                description TEXT,
                published_at TIMESTAMP,
                channel_id VARCHAR(255),
                channel_title VARCHAR(255),
                view_count INTEGER,
                like_count INTEGER,
                dislike_count INTEGER,
                comment_count INTEGER
            );
        """
    cur.execute(table_sql)
    conn.commit()
    close_conn_cursor(conn, cur)
    
def get_video_ids(cur, schema):
    cur.execute(f"SELECT video_id FROM {schema}.{table}")
    ids = cur.fetchall()
    
    video_ids = [row["Video_ID"] for row in ids]
    
    [{'Video_ID': 'abc123'}, {'Video_ID': 'xyz456'}, {'Video_ID': 'def789'}]