from cachelib.redis import RedisCache

ROW_LIMIT = 5000

SECRET_KEY = "hK8#mP2$vL9@qR4_xT7*zW1!nB5-yC6&jF3%dH8"

# PostgreSQL metadata database
SQLALCHEMY_DATABASE_URI = (
    "postgresql+psycopg2://superset:superset@superset-postgres:5432/superset"
)

# Redis
REDIS_HOST = "superset-redis"
REDIS_PORT = 6379

RATELIMIT_STORAGE_URI = "redis://superset-redis:6379/0"

# Results backend
RESULTS_BACKEND = RedisCache(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=1,  # Možete staviti bazu po izboru, npr. 1
    key_prefix="superset_results_",
)

# General cache
CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 60,
    "CACHE_KEY_PREFIX": "superset_cache_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
}