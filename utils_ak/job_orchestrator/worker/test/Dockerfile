FROM python:3.8
WORKDIR /app
COPY . /app

ENV PYTHONPATH "${PYTHONPATH}:/utils/python-utils-ak"
SHELL ["/bin/bash", "-c"]
RUN mkdir /utils && cd /utils && git clone https://github.com/akadaner/python-utils-ak.git
RUN pip install -r /app/requirements.txt
ENTRYPOINT ["python", "main.py"]
