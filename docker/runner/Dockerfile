FROM apache/spark:3.5.1
USER root
RUN pip3 install --no-cache-dir kafka-python==3.0.13 psycopg2-binary==2.9.10
ENV PYTHONPATH=/opt/project PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /opt/project
USER 185:0
ENTRYPOINT ["python3", "-m", "orchestration.runner"]
