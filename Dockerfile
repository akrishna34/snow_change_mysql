# syntax=docker/dockerfile:1

FROM python:3.8-slim-buster

WORKDIR /app

COPY requirement.txt requirement.txt
RUN pip install -r requirement.txt

COPY . .


ARG MYSQL_PASSWORD
ARG account
ARG user


ENV account=$account
ENV user=$user


ENV MYSQL_PASSWORD=$MYSQL_PASSWORD


ENTRYPOINT  python schemachange_mysql.py  -f './sql_changes/'  -a $account -u $user  -ac -d 'metadata' -c 'metadata.change_history'