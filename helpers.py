from models import Base
from sqlalchemy import (
    Date,
    DateTime,
    Float,
    PrimaryKeyConstraint,
    String,
    create_engine,
)
from sqlalchemy.orm import sessionmaker


def make_engine(db_url: str, echo: bool = False):
    engine = create_engine(db_url, echo=echo, future=True)
    Base.metadata.create_all(engine)
    return engine


def make_session_factory(engine):
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
