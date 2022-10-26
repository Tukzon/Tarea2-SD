FROM python:3.9.6-slim-buster

WORKDIR /ventas

COPY requirements.txt /ventas

RUN python -m pip install --upgrade pip

RUN pip install -r requirements.txt

EXPOSE 5003

COPY . .

CMD ["python", "app.py"]