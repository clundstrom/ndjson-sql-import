FROM mariadb:10.5.5-focal

ARG setup_file

# Copy setup file
COPY ${setup_file} /docker-entrypoint-initdb.d/dump.sql

# Custom config
COPY custom.cnf /etc/mysql/conf.d/custom.cnf