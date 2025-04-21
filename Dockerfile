FROM python:3.10-slim

RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    libaio1 \
 && rm -rf /var/lib/apt/lists/*

# Install Oracle Instant Client
RUN curl -o instantclient-basic-linux.x64-19.19.0.0.0dbru.zip \
    https://download.oracle.com/otn_software/linux/instantclient/1919000/instantclient-basic-linux.x64-19.19.0.0.0dbru.zip && \
    unzip instantclient-basic-linux.x64-19.19.0.0.0dbru.zip -d /opt && \
    rm instantclient-basic-linux.x64-19.19.0.0.0dbru.zip

ENV LD_LIBRARY_PATH=/opt/instantclient_19_19
ENV PATH="$PATH:/opt/instantclient_19_19"
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

CMD ["python", "main.py"]