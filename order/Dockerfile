FROM python:3.11-slim

WORKDIR /home/flask-app

COPY ./requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5000

CMD ["hypercorn", "-b", "0.0.0.0:5000", "--workers", "2", "--timeout", "30", "--log-level", "info", "app:app"]
