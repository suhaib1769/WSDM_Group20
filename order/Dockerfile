FROM python:3.11-slim

WORKDIR /home/flask-app

COPY ./requirements.txt .

#COPY wait-for-kafka.sh /wait-for-kafka.sh

RUN apt-get update && \
    apt-get install -y curl

# Install netcat
RUN apt-get update && \
    apt-get install -y curl netcat-traditional  # Added netcat here

RUN pip install -r requirements.txt

COPY . .

RUN chmod +x wait-for-kafka.sh
EXPOSE 5000
CMD ["./wait-for-kafka.sh", "python", "-u", "app.py"]