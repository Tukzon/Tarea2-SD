FROM python:3.9.6-slim-buster

WORKDIR /nuevosMiembros

COPY requirements.txt /nuevosMiembros

RUN python -m pip install --upgrade pip

RUN pip install -r requirements.txt

EXPOSE 5000

COPY . .

CMD ["python", "app.py"]