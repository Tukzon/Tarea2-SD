FROM python:3.9.6-slim-buster

WORKDIR /Consumer

COPY requirements.txt /Consumer

RUN python -m pip install --upgrade pip

RUN pip install -r requirements.txt

EXPOSE 5000

COPY . .

CMD ["python", "app.py"]