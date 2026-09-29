# One-shot job: creates the lite stack's topics from contracts/topics/v0/topic-defaults.json, then
# exits. Not a long-running service (see docker-compose.yml: restart: "no", no healthcheck).
#
# Pinned by digest, not tag (Phase 1 requirement 7). pip install is pinned and hashed
# (requirements.txt, generated with `uv pip compile requirements.in --generate-hashes`); `--require-
# hashes` makes pip refuse to install anything that doesn't match.
FROM python:3.12-slim@sha256:2f17fc044b579bab302c2e8054d3a686e2cb9a83de48e70534b94cd8ebbe06a9

RUN groupadd --gid 10001 tributary \
    && useradd --uid 10001 --gid tributary --no-create-home --shell /usr/sbin/nologin tributary

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --require-hashes -r requirements.txt \
    && rm requirements.txt
COPY bootstrap_topics.py add_scram_users.py .
# Shared with channel-publish via the compose `additional_contexts: common` (deploy/compose/common).
COPY --from=common kafka_conn.py .

USER tributary:tributary
ENTRYPOINT ["python", "bootstrap_topics.py"]
