# Build the patched Superset frontend.
FROM node:22.22.0-bookworm AS frontend-build

RUN apt-get update \
    && apt-get install -y --no-install-recommends git zstd \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

RUN git clone \
    --depth 1 \
    --branch 6.1.0 \
    https://github.com/apache/superset.git .

WORKDIR /workspace/superset-frontend

RUN npm ci --no-audit --no-fund

COPY superset-cross-filter.patch /tmp/superset-cross-filter.patch

WORKDIR /workspace

RUN git apply --check /tmp/superset-cross-filter.patch \
    && git apply /tmp/superset-cross-filter.patch

WORKDIR /workspace/superset-frontend

ENV NODE_ENV=production
ENV NODE_OPTIONS=--max-old-space-size=8192

RUN NODE_ENV=test \
    NODE_OPTIONS=--max-old-space-size=8192 \
    ./node_modules/.bin/jest --runInBand \
    plugins/plugin-chart-echarts/src/Timeseries/EchartsTimeseries.test.tsx

RUN npm run build


# Package the compiled frontend into the official Superset image.
FROM apache/superset:6.1.0

USER root

RUN uv pip install \
    --python /app/.venv/bin/python \
    --system \
    psycopg2-binary \
    "pymongosql>=0.7.4,<1"

COPY --from=frontend-build \
    /workspace/superset/static/assets/ \
    /tmp/crossfilter-assets/

RUN set -eux; \
    test -s /tmp/crossfilter-assets/manifest.json; \
    test -s /tmp/crossfilter-assets/pwa-manifest.json; \
    rm -rf /app/superset/static/assets; \
    mkdir -p /app/superset/static/assets; \
    cp -a /tmp/crossfilter-assets/. /app/superset/static/assets/; \
    rm -rf /tmp/crossfilter-assets; \
    chown -R root:root /app/superset/static/assets

USER superset
