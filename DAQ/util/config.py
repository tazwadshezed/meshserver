import os
import redis
import psycopg2
from psycopg2.extras import DictCursor

def load_config():
    """Shim for compatibility with legacy load_config(). Returns dict of all env vars used."""
    return {
        "nats": {
            "internal_mesh_topic": os.getenv("INTERNAL_MESH_TOPIC", "mesh.internal"),
            "external_mesh_topic": os.getenv("EXTERNAL_MESH_TOPIC", "mesh.external"),
            "publish_topic": os.getenv("PUBLISH_TOPIC", "daq.publish"),
            "command_topic": os.getenv("COMMAND_TOPIC", "daq.command"),
            "response_topic": os.getenv("RESPONSE_TOPIC", "daq.response"),
            "emulator_topic": os.getenv("EMULATOR_TOPIC", "daq.emulator"),
        },
        "database": {
            "redis": {
                "host": os.getenv("REDIS_HOST", "localhost"),
                "port": int(os.getenv("REDIS_PORT", 6379)),
                "db": int(os.getenv("REDIS_DB", 0))
            },
            "postgres": {
                "dbname": os.getenv("POSTGRES_DB", "postgres"),
                "user": os.getenv("POSTGRES_USER", "postgres"),
                "password": os.getenv("POSTGRES_PASSWORD", ""),
                "host": os.getenv("POSTGRES_HOST", "localhost"),
                "port": os.getenv("POSTGRES_PORT", 5432),
            }
        },
        "paths": {
            "templates_dir": os.getenv("TEMPLATES_DIR", "apps/templates")
        }
    }

def get_topic(name: str) -> str:
    """Unified NATS topic resolver."""
    config = load_config()
    topic_map = config["nats"]
    return topic_map.get(f"{name}_topic") or topic_map.get(name)

def get_redis_conn(db=None):
    """Returns a Redis connection using environment variables."""
    cfg = load_config()["database"]["redis"]
    return redis.StrictRedis(
        host=cfg["host"],
        port=cfg["port"],
        db=db if db is not None else cfg["db"],
        decode_responses=True
    )

def get_postgres_conn():
    """Returns a PostgreSQL connection using environment variables."""
    pg = load_config()["database"]["postgres"]
    return psycopg2.connect(
        dbname=pg["dbname"],
        user=pg["user"],
        password=pg["password"],
        host=pg["host"],
        port=pg["port"],
        cursor_factory=DictCursor
    )

def get_templates_dir():
    """Returns the Jinja2 templates directory (for dataserver)."""
    return load_config()["paths"]["templates_dir"]

def get_local_path(global_path, local_path, fname=None):
    """Preserved for legacy fallback logic."""
    from os.path import join, exists, normpath
    g = normpath(global_path)
    l = normpath(local_path)
    return normpath(join(g, fname)) if fname and exists(join(g, fname)) else l

def local_config():
    """Just returns env-based config (alias)."""
    return load_config()

def read_pkginfo():
    """Stub for embedded build metadata."""
    return {}
