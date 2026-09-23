# One-shot job: publishes the demo channel spec to _tb.<env>.channels, then exits. Not a long-running
# service (see docker-compose.yml: restart: "no", no healthcheck). Pinned and hashed like
# topic-bootstrap/Dockerfile; see that file's comment.
FROM python:3.12-slim@sha256:2f17fc044b579bab302c2e8054d3a686e2cb9a83de48e70534b94cd8ebbe06a9

RUN groupadd --gid 10001 tributary \
    && useradd --uid 10001 --gid tributary --no-create-home --shell /usr/sbin/nologin tributary

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --require-hashes -r requirements.txt \
    && rm requirements.txt
COPY publish_channel.py .

USER tributary:tributary
ENTRYPOINT ["python", "publish_channel.py"]
