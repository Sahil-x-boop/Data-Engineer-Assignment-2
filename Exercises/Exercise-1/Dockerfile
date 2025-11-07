# Use official lightweight Python base image
FROM python:latest

WORKDIR /app

COPY . .

RUN python3 -m pip install --no-cache-dir -r requirements.txt 

CMD ["python3", "main.py"]
