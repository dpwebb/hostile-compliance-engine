import logging
import time

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import declarative_base, sessionmaker

from app.settings import settings

logger = logging.getLogger(__name__)

engine = create_engine(settings.database_url, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def init_db() -> None:
    retries = 6
    delay = 2
    for attempt in range(retries):
        try:
            Base.metadata.create_all(bind=engine)
            return
        except OperationalError as exc:
            logger.warning("Database not ready (attempt %s): %s", attempt + 1, exc)
            time.sleep(delay)
            delay = min(delay * 2, 20)
    raise RuntimeError("Database not ready after retries.")


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
