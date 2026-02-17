import os

from dotenv import find_dotenv, load_dotenv
from models import Base
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

load_dotenv(find_dotenv())

PROJECT_ROOT = os.path.dirname(os.path.realpath(__file__))
DATABASE = os.path.join(PROJECT_ROOT, os.getenv("DATABASE_NAME", "nyfed_stress.db"))
DEFAULT_DB_URL = "sqlite:///data/{0}".format(DATABASE)

engine = create_engine(DEFAULT_DB_URL, echo=True, future=True)
db_session = scoped_session(
    sessionmaker(autocommit=False, autoflush=False, bind=engine, future=True)
)

Base.query = db_session.query_property()


def make_engine(db_url: str = DEFAULT_DB_URL, echo: bool = False):
    eng = create_engine(db_url, echo=echo, future=True)
    Base.metadata.create_all(eng)
    return eng


def make_session_factory(eng):
    return sessionmaker(bind=eng, autoflush=False, autocommit=False, future=True)


def init_db(eng=None):
    Base.metadata.create_all(bind=eng or engine)
