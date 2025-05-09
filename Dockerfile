FROM python:3.12-alpine3.21

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PIP_CACHE_DIR=/.cache/pip

RUN apk add --no-cache gcc musl-dev libffi-dev

COPY ./ ./

RUN --mount=type=cache,target=/.cache/pip \
    pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python", "main.py"]