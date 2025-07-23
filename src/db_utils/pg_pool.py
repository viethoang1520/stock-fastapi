import asyncpg

DB_CONFIG = {
    'user': 'swd_stockintel_user',
    'password': '04e2ERorKhHYJBHjvEC9poakSgcGYW1F',
    'database': 'swd_stockintel_mmm0',
    'host': 'dpg-d1fqcrili9vc739rk3ug-a.singapore-postgres.render.com',
}

_db_pool = None

async def init_db_pool():
    global _db_pool
    if _db_pool is None:
        _db_pool = await asyncpg.create_pool(**DB_CONFIG)
    return _db_pool

def get_pool():
    return _db_pool
