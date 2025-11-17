from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import OperationalError, IntegrityError, ProgrammingError
from sqlalchemy.engine import URL
from typing import Optional


class SessionManager:

    def __init__(
        self,
        user: str,
        password: str,
        host: str,
        port: int,
        database: str,
        echo: bool = False,
        pool_size: int = 10,
        max_overflow: int = 20,
        pool_timeout: int = 30,
        pool_recycle: int = 1800,
    ) -> None:

        self.db_url = URL.create(
            drivername="postgresql+psycopg2",
            username=user,
            password=password,
            host=host,
            port=port,
            database=database,
        )

        self.engine = create_engine(
            self.db_url,
            echo=echo,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            pool_recycle=pool_recycle,
        )

        self.SessionLocal = sessionmaker(
            bind=self.engine,
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
        )


    def get_session(self) -> Session:
        return self.SessionLocal()

    def commit(self, session: Session) -> None:
        try:
            session.commit()
        except Exception:
            session.rollback()
            raise

    def rollback(self, session: Session) -> None:
        session.rollback()

    def close(self, session: Session) -> None:
        session.close()