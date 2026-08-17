from contextlib import contextmanager
from typing import Generator

from psycopg import Connection
from psycopg_pool import ConnectionPool


class PgConnect:
    def __init__(
        self,
        host: str,
        port: int,
        db_name: str,
        user: str,
        pw: str,
        sslmode: str = "require"
    ) -> None:
        self.host = host
        self.port = port
        self.db_name = db_name
        self.user = user
        self.pw = pw
        self.sslmode = sslmode

        self._pool = ConnectionPool(
            conninfo=self.url(),
            min_size=1,
            max_size=5
        )

    def url(self) -> str:
        return """
            host={host}
            port={port}
            dbname={db_name}
            user={user}
            password={pw}
            target_session_attrs=read-write
            sslmode={sslmode}
        """.format(
            host=self.host,
            port=self.port,
            db_name=self.db_name,
            user=self.user,
            pw=self.pw,
            sslmode=self.sslmode
        )

    @contextmanager
    def connection(self) -> Generator[Connection, None, None]:
        with self._pool.connection() as conn:
            try:
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise