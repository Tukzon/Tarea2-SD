FROM python:3.9.6-slim-buster

WORKDIR /Producer

COPY requirements.txt /Producer

RUN python -m pip install --upgrade pip

RUN pip install -r requirements.txt

EXPOSE 8000

COPY . .

CMD ["flask", "run"]