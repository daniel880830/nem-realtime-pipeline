# syntax=docker/dockerfile:1
# Two-stage build: dependencies are installed into a virtualenv in the builder and copied
# into a slim runtime image that runs as a non-root user.
ARG PYTHON_VERSION=3.13

FROM python:${PYTHON_VERSION}-slim AS builder
# SLIM=1 strips files that are never imported at runtime (set SLIM=0 to measure the difference).
ARG SLIM=1
ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
RUN python -m venv /opt/venv
ENV PATH=/opt/venv/bin:$PATH
WORKDIR /src
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-deps . \
 && if [ "$SLIM" = "1" ]; then \
      find /opt/venv -depth -type d \( -name tests -o -name test -o -name __pycache__ \) -exec rm -rf {} + ; \
      find /opt/venv \( -name '*.pyi' -o -name '*.pxd' -o -name '*.pyx' -o -name '*.h' -o -name '*.cpp' \) -delete ; \
      SP=$(python -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])') ; \
      # C++ headers, Arrow Flight (RPC) and the JS bundle of pydeck's Jupyter widget are never used here.
      rm -rf "$SP/pyarrow/include" "$SP"/pyarrow/_pyarrow_cpp_tests* "$SP"/pyarrow/_flight* \
             "$SP"/pyarrow/libarrow_flight* "$SP"/pyarrow/libarrow_python_flight* "$SP/pydeck/nbextension/static" ; \
      pip uninstall -y pip setuptools wheel || true ; \
    fi

FROM python:${PYTHON_VERSION}-slim AS runtime
ARG SLIM=1
ENV PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    APP_DATA_DIR=/app/data \
    APP_REPORTS_DIR=/app/reports \
    APP_CONFIG_FILE=/app/config/pipeline.toml
RUN if [ "$SLIM" = "1" ]; then \
      rm -rf /usr/local/lib/python3*/site-packages/pip* /usr/local/bin/pip* ; \
    fi \
 && groupadd --system app && useradd --system --gid app --home-dir /app app
WORKDIR /app
COPY --from=builder /opt/venv /opt/venv
COPY config ./config
COPY data/raw/cer_snapshot_2025 ./data/raw/cer_snapshot_2025
RUN mkdir -p data/cache data/raw/oe data/warehouse reports && chown -R app:app /app
USER app
EXPOSE 8501
# Probes the dashboard; the compose file disables it for the one-off pipeline and publisher.
HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=3 \
  CMD ["python", "-c", "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8501/_stcore/health', timeout=4).status == 200 else 1)"]
ENTRYPOINT ["nem-pipeline"]
CMD ["dashboard"]
