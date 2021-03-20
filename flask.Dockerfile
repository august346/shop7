FROM python:3.8
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
WORKDIR /app
COPY app /app
EXPOSE 80
CMD ["gunicorn", "app:app", "-w", "4", "-b", "0.0.0.0:80"]