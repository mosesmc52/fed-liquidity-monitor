# store.py
from __future__ import annotations

import datetime as dt
from typing import Iterable, List, Optional, Tuple

from sqlalchemy import (
    Date,
    DateTime,
    Float,
    PrimaryKeyConstraint,
    String,
    create_engine,
    func,
    select,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

# -----------------------------
# ORM Models
# -----------------------------


class Base(DeclarativeBase):
    pass


class Observation(Base):
    __tablename__ = "observations"

    series_id: Mapped[str] = mapped_column(String, nullable=False)
    obs_date: Mapped[dt.date] = mapped_column(Date, nullable=False)
    value: Mapped[float] = mapped_column(Float, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint("series_id", "obs_date", name="pk_observations"),
    )


class Alert(Base):
    __tablename__ = "alerts"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    alert_ts: Mapped[dt.datetime] = mapped_column(DateTime, nullable=False, index=True)
    series_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    level: Mapped[str] = mapped_column(String, nullable=False)
    message: Mapped[str] = mapped_column(String, nullable=False)


# -----------------------------
# Store
# -----------------------------


class Store:
    def __init__(self, db_url: str, echo: bool = False):
        """
        db_url examples:
          sqlite:///nyfed_stress.sqlite
          postgresql+psycopg2://user:pass@host:5432/dbname
        """
        self.engine = create_engine(db_url, echo=echo, future=True)
        Base.metadata.create_all(self.engine)

    def upsert_observations(
        self, series_id: str, rows: Iterable[Tuple[dt.date, float]]
    ) -> int:
        """
        Upsert by (series_id, obs_date).
        Works on SQLite via merge() (safe, not fastest; good for daily polling volumes).
        """
        n = 0
        with Session(self.engine) as session:
            for d, v in rows:
                # merge() does SELECT then INSERT/UPDATE as needed
                session.merge(
                    Observation(series_id=series_id, obs_date=d, value=float(v))
                )
                n += 1
            session.commit()
        return n

    def load_series(
        self,
        series_id: str,
        start_date: dt.date,
        end_date: dt.date,
    ) -> List[Tuple[dt.date, float]]:
        with Session(self.engine) as session:
            stmt = (
                select(Observation.obs_date, Observation.value)
                .where(Observation.series_id == series_id)
                .where(Observation.obs_date >= start_date)
                .where(Observation.obs_date <= end_date)
                .order_by(Observation.obs_date.asc())
            )
            rows = session.execute(stmt).all()
        return [(d, float(v)) for d, v in rows]

    def latest_date(self, series_id: str) -> Optional[dt.date]:
        """Return latest stored obs_date for the series, if any."""
        with Session(self.engine) as session:
            stmt = select(func.max(Observation.obs_date)).where(
                Observation.series_id == series_id
            )
            (mx,) = session.execute(stmt).one()
        return mx

    def insert_alert(
        self, ts: dt.datetime, series_id: str, level: str, message: str
    ) -> None:
        with Session(self.engine) as session:
            session.add(
                Alert(alert_ts=ts, series_id=series_id, level=level, message=message)
            )
            session.commit()
