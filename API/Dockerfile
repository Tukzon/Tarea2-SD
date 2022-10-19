FROM python:3.9.6-slim-buster

WORKDIR /API

COPY requirements.txt /API

RUN python -m pip install --upgrade pip

RUN pip install -r requirements.txt

COPY . .

CMD ["flask", "run"]