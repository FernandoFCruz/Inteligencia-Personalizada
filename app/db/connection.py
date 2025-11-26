import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from app.core.config import settings

# --- Connection Pool Global ---
pool = None

def init_connection_pool():
    global pool

    if pool is None:
        try:
            pool = ThreadedConnectionPool(
                minconn=1,
                maxconn=10,  # ajuste conforme necessidade
                dsn=settings.database_url
            )
            print("🔌 PostgreSQL pool inicializado.")
        except Exception as e:
            print("❌ Erro ao inicializar o pool do PostgreSQL:", e)
            raise e


def get_connection():
    """Retorna uma conexão do pool."""
    if pool is None:
        init_connection_pool()

    try:
        conn = pool.getconn()
        return conn
    except Exception as e:
        print("❌ Erro ao obter conexão:", e)
        raise e


def release_connection(conn):
    """Devolve a conexão ao pool."""
    try:
        if pool and conn:
            pool.putconn(conn)
    except Exception as e:
        print("❌ Erro ao devolver conexão ao pool:", e)