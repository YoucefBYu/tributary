# Generic multi-stage image for one workspace package (packages/tributary-*), built with uv.
#
# Build with:
#   docker build -f deploy/compose/images/python-app.Dockerfile \
#     --build-arg TB_PACKAGE=tributary-receivers \
#     -t tributary/tributary-receivers .
# from the repo root, so uv.lock and every packages/*/pyproject.toml are in the build context.
#
# `--build-arg TB_PACKAGE` selects the workspace package to install with `uv sync --package`, which
# pulls in that package's own dependencies (and tributary-sdk, via `workspace = true`) but not the
# other three siblings. Runs as a non-root user, read-only capable (see docker-compose.yml
# `read_only: true` and its `/tmp` tmpfs), with no shell needed at runtime beyond the entrypoint.
#
# Base images are pinned by digest (Phase 1 requirement 7 / gate requirement SR-6), not by tag: a tag
# can move under us, a digest cannot. Bump both lines together and record why in the commit message.
FROM python:3.12-slim@sha256:2f17fc044b579bab302c2e8054d3a686e2cb9a83de48e70534b94cd8ebbe06a9 AS build
ARG TB_PACKAGE

# ca-certificates: outbound TLS from the container (Kafka SASL_SSL, HTTPS loaders) needs a trust
# store; there is no compiler here on purpose, so a package needing one must ship wheels.
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.12.17@sha256:10787c682e4184e4f290de1171fd4703dc63de99221f10fe1c99002ce7fa9acc /uv /usr/local/bin/uv

WORKDIR /src
# Full workspace context: uv needs every packages/*/pyproject.toml to resolve, even though only one
# package's dependency closure gets installed below.
COPY pyproject.toml uv.lock ./
COPY packages/ packages/

ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_FROZEN=1 \
    UV_NO_CACHE=1 \
    UV_PYTHON_DOWNLOADS=never
# Optional build secret `ca`: an extra CA bundle for networks that re-terminate TLS (corporate or CI
# egress proxies). Never copied into a layer; absent in normal builds:
#   docker build --secret id=ca,src=/path/ca-bundle.crt ...
#
# TB_EXTRAS: an optional extra of TB_PACKAGE itself. Only tributary-archiver has one in use (`clickhouse`, the
# driver and its certifi dependency, MPL-2.0 approved for the archiver only). Any other package asking for an
# extra fails the build, so the API image can never pick up certifi through this arg.
ARG TB_EXTRAS=""
RUN --mount=type=secret,id=ca,required=false \
    if [ -n "${TB_EXTRAS}" ] && { [ "${TB_PACKAGE}" != "tributary-archiver" ] || [ "${TB_EXTRAS}" != "clickhouse" ]; }; then \
      echo "TB_EXTRAS=${TB_EXTRAS} is allowed only as clickhouse for tributary-archiver" >&2; exit 2; fi; \
    if [ -s /run/secrets/ca ]; then export SSL_CERT_FILE=/run/secrets/ca UV_SYSTEM_CERTS=1; fi; \
    uv sync --frozen --no-dev --no-editable --package "${TB_PACKAGE}" ${TB_EXTRAS:+--extra "${TB_EXTRAS}"}

FROM python:3.12-slim@sha256:2f17fc044b579bab302c2e8054d3a686e2cb9a83de48e70534b94cd8ebbe06a9 AS runtime
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 tributary \
    && useradd --uid 10001 --gid tributary --no-create-home --shell /usr/sbin/nologin tributary

COPY --from=build /opt/venv /opt/venv
# contracts/ ships in every image (W3 ruling H4): topic-defaults.json carries invariant-1 producer values
# pinned to this code version, so it must travel with the code, not come from an operator-built ConfigMap.
# Compose still bind-mounts ../../contracts over this path for development.
COPY contracts/ /contracts/
ENV TB_CONTRACTS_DIR=/contracts \
    PATH=/opt/venv/bin:$PATH \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

USER tributary:tributary
WORKDIR /home/tributary
ENTRYPOINT ["python", "-m"]
