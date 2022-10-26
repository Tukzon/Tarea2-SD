import psycopg2

conn = psycopg2.connect(
    host="postgres",
    database="tarea2",
    user="postgres",
    password="postgres",
    port=5432
)

def query(query):
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()