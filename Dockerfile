FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    WEB_HOST=0.0.0.0 \
    WEB_PORT=5000 \
    PORT=5000 \
    WEB_DEBUG=0

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

RUN mkdir -p /app/data/runtime /app/data/drafts/web /app/saidas /app/BASE_MESTRA

EXPOSE 5000

CMD ["python", "run_web.py"]
