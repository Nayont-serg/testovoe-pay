FROM python:3.12-slim AS base

ENV PYTHONUNBUFFERED=1
WORKDIR /srv/app

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY app ./app
COPY alembic ./alembic
COPY alembic.ini ./

RUN uv sync --frozen --no-dev

ENV PATH="/srv/app/.venv/bin:${PATH}"
