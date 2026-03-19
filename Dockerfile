FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV WEB_HOST=0.0.0.0
ENV WEB_PORT=5000
ENV WEB_DEBUG=0

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

RUN mkdir -p /app/data/runtime /app/data/drafts/web /app/saidas /app/BASE_MESTRA

EXPOSE 5000

CMD ["python", "run_web.py", "--host", "0.0.0.0", "--port", "5000"]
