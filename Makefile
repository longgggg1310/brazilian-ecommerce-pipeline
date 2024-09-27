include .env


install:
	python3 -V \
	&& python3 -m venv venv \
	&& . venv/bin/activate \
	&& pip install --upgrade pip && pip install -r requirements.txt
build:
	docker-compose build
to_mysql:
	docker exec -it mysql mysql --local-infile=1 -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" ${MYSQL_DATABASE}
to_mysql_root:
	docker exec -it mysql mysql -u"root" -p"${MYSQL_ROOT_PASSWORD}" ${MYSQL_DATABASE}

to_psql:
	docker exec -ti de_psql psql postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}
