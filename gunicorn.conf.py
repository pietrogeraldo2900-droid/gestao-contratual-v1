from __future__ import annotations

import multiprocessing
import os


bind = f"0.0.0.0:{os.getenv('PORT', os.getenv('WEB_PORT', '5000'))}"
workers = int(os.getenv('GUNICORN_WORKERS', max(2, multiprocessing.cpu_count())))
threads = int(os.getenv('GUNICORN_THREADS', '2'))
timeout = int(os.getenv('GUNICORN_TIMEOUT', '120'))
graceful_timeout = int(os.getenv('GUNICORN_GRACEFUL_TIMEOUT', '30'))
keepalive = int(os.getenv('GUNICORN_KEEPALIVE', '5'))
accesslog = '-'
errorlog = '-'
loglevel = os.getenv('GUNICORN_LOG_LEVEL', 'info')
worker_tmp_dir = '/dev/shm'
