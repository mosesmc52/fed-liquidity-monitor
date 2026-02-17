# models.py
from __future__ import annotations

import datetime as dt

from sqlalchemy import Date, DateTime, Float, PrimaryKeyConstraint, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


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
