FROM nginx:1.19-alpine

RUN rm /etc/nginx/conf.d/default.conf
ARG NGINX_CONFIG_PATH
COPY $NGINX_CONFIG_PATH /etc/nginx/conf.d