from asyncpg.pool import Pool


class DataBase:
    pool: Pool = None


db = DataBase()
