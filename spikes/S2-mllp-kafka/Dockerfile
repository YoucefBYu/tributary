# S2/S3 spike image: Linux + uvloop (uvloop has no Windows support). Throwaway.
FROM python:3.12-slim
RUN pip install --no-cache-dir "confluent-kafka>=2.6" "uvloop>=0.20"
WORKDIR /app
COPY *.py /app/
EXPOSE 2575
ENTRYPOINT ["python"]
