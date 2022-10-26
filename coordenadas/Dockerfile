FROM python:3.9.6-slim-buster

WORKDIR /coordenadas

COPY requirements.txt /coordenadas

RUN python -m pip install --upgrade pip

RUN pip install -r requirements.txt

EXPOSE 5002

COPY . .

CMD ["python", "app.py"]