FROM python:3

RUN curl -OLs http://cdn.mysql.com/Downloads/Connector-Python/mysql-connector-python-2.1.5.tar.gz \
 && tar -xzf mysql-connector-python-2.1.5.tar.gz \
 && cd mysql-connector-python-2.1.5 \
 && python setup.py install \
 && cd .. \
 && rm -rf mysql-connector-python-2.1.5 \
 && rm mysql-connector-python-2.1.5.tar.gz

COPY . /app

WORKDIR /app
RUN pip install -r requirements.txt

ENV FLASK_APP=api.py

EXPOSE 5000

CMD ["flask", "run", "--host=0.0.0.0"]