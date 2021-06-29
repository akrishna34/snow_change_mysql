# syntax=docker/dockerfile:1

FROM python:3.8-slim-buster

WORKDIR /app

COPY requirement.txt requirement.txt
RUN pip install -r requirement.txt

COPY . .


ARG MYSQL_PASSWORD
ARG account
ARG user
ARG database
ARG table


ENV account=$account
ENV user=$user
ENV database=$database
ENV table=$table


ENV MYSQL_PASSWORD=$MYSQL_PASSWORD


ENTRYPOINT  python schemachange.py  -f './mantain_schema/'  -a $account -u $user  -ac -d $database -c $database.$table