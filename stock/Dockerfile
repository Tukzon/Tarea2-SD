FROM python:3.9.6-slim-buster

WORKDIR /stock

COPY requirements.txt /stock

RUN python -m pip install --upgrade pip

RUN pip install -r requirements.txt

EXPOSE 5001

COPY . .

CMD ["python", "app.py"]