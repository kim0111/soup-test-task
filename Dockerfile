# One image for every role (api, worker, tests, chaos, load); compose picks the command.
FROM python:3.12-slim

# uv binary, pinned to the version that wrote uv.lock.
COPY --from=ghcr.io/astral-sh/uv:0.13.7 /uv /uvx /bin/

ENV PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

# Dependencies first, so code changes don't invalidate this layer.
# --locked: fail the build if uv.lock is out of date with pyproject.toml.
COPY pyproject.toml uv.lock .python-version ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-install-project

COPY app ./app
COPY migrations ./migrations
COPY tests ./tests

CMD ["uvicorn", "app.api:app", "--host", "0.0.0.0", "--port", "8000"]
