FROM python:3.13-slim

WORKDIR /srv

COPY requirements.txt /srv/requirements.txt
COPY requirements-dev.txt /srv/requirements-dev.txt
RUN pip install --no-cache-dir -r /srv/requirements.txt -r /srv/requirements-dev.txt

COPY app /srv/app
COPY fastapi/main.py /srv/main.py
COPY tests /srv/tests

ENV PYTHONPATH=/srv

EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
