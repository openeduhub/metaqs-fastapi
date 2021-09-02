from asyncpg.pool import Pool


class Postgres:
    pool: Pool = None


postgres = Postgres()
