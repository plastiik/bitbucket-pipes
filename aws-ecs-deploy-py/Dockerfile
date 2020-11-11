FROM python:3.7

COPY requirements.txt /usr/bin
WORKDIR /usr/bin
RUN pip install -r requirements.txt
COPY pipe.yml /usr/bin
COPY pipe /usr/bin/

ENTRYPOINT ["python3", "/usr/bin/main.py"]
