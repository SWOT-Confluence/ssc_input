# Stage 0 - Create from Python3.9.5 image
FROM python:3.12 as stage0
# FROM python:3.9-slim-buster

# Stage 1 - Create virtual environment and install dependencies
FROM stage0 as stage1
COPY requirements.txt /app/requirements.txt
RUN /usr/local/bin/python3 -m venv /app/env
RUN /app/env/bin/pip install -r /app/requirements.txt

# Stage 2 - Copy and execute module
FROM stage1 as stage2
COPY ./ssc_input.py /app/ssc_input.py

LABEL version="1.0" \
      description="Containerized model to input ssc data."
ENTRYPOINT ["/app/env/bin/python3", "/app/ssc_input.py"]