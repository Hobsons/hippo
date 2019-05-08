FROM python:3.5

RUN apt-get update \
 && apt-get install -y libsasl2-dev python-dev libldap2-dev libssl-dev nginx git gcc supervisor \
 && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN curl -OLs https://dev.mysql.com/get/Downloads/Connector-Python/mysql-connector-python-2.1.7.tar.gz \
 && tar -xzf mysql-connector-python-2.1.7.tar.gz \
 && cd mysql-connector-python-2.1.7 \
 && python setup.py install \
 && cd .. \
 && rm -rf mysql-connector-python-2.1.7 \
 && rm mysql-connector-python-2.1.7.tar.gz

COPY . /app

WORKDIR /app
RUN pip install -r requirements.txt

ENV FLASK_APP=api.py

EXPOSE 5000

RUN echo "daemon off;" >> /etc/nginx/nginx.conf \
 && rm /etc/nginx/sites-enabled/default \
 && ln -s /app/nginx/hippo-nginx.conf /etc/nginx/sites-enabled/ \
 && ln -s /app/nginx/supervisord.conf /etc/supervisor/conf.d/ \
 && ln -sf /dev/stdout /var/log/nginx/access.log \
 && ln -sf /dev/stderr /var/log/nginx/error.log

CMD ["/usr/bin/supervisord","-n"]