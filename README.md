## Mintaka: NGSI-LD-Temporal-App
This app is for NGSI-LD temporal queries.
It is developed using python-flask and postgres db.
The server is running using nginx.

## Installation
It can run directly on Ubuntu 18.04 or can be run using docker also
First pull the repo in your local machine either using git or svn
git clone https://github.com/Trigyn-Technologies/Mintaka.git

### Docker Installation
In the directory open docker-compose.yml and edit as below
See the sample docker-compose.yml
```
version: '3'
services:
  db:
    image: timescale/timescaledb-postgis:latest-pg12
    ports:
      - 5432:5432
    restart: always
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: password
      POSTGRES_DB: orion_ld
  flask:
    image: ngsild-flask
    build:
      context: .
      dockerfile: Dockerfile
    volumes:
      - "./:/app"
    command: flask run --host=0.0.0.0
    networks:
      my-network:
        aliases:
          - flask-app
    ports:
        - 8080:5000
    environment:
      - PYTHONUNBUFFERED=1
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=password
      - POSTGRES_DB=orion_ld
      - POSTGRES_HOST=192.168.0.113
      - FLASK_APP=/app.py

    depends_on:
      - db
  nginx:
    image: nginx:1.13.7
    container_name: nginx
    depends_on:
        - flask
    volumes:
        - ./nginx.conf:/etc/nginx/conf.d/default.conf
    networks:
        - my-network
    ports:
        - 8000:80
networks:
    my-network:
```

Here change the POSTGRES_USER, POSTGRES_PASSWORD and POSTGRES_HOST(Your Ip). If you want the default one just change POSTGRES_HOST.
And run the docker-compose

### Installation on Ubuntu 18.04

Add the postgres repo
```bash
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
echo "deb http://apt.postgresql.org/pub/repos/apt/ `lsb_release -cs`-pgdg main" |sudo tee  /etc/apt/sources.list.d/pgdg.list
```

#### Install Postgres
```bash
sudo apt update
sudo apt -y install postgresql-12 postgresql-client-12
sudo apt install postgis postgresql-12-postgis-3
```

Add timescale db and posgis
```bash
sudo add-apt-repository ppa:timescale/timescaledb-ppa
sudo apt-get update
sudo apt install timescaledb-postgresql-12
```

Command for checking postgres
```bash
systemctl status postgresql.service
```
The output will be something like this
```text
postgresql.service - PostgreSQL RDBMS
    Loaded: loaded (/lib/systemd/system/postgresql.service; enabled; vendor preset: enabled)
    Active: active (exited) since Sun 2019-10-06 10:23:46 UTC; 6min ago
  Main PID: 8159 (code=exited, status=0/SUCCESS)
     Tasks: 0 (limit: 2362)
    CGroup: /system.slice/postgresql.service
 Oct 06 10:23:46 ubuntu18 systemd[1]: Starting PostgreSQL RDBMS…
 Oct 06 10:23:46 ubuntu18 systemd[1]: Started PostgreSQL RDBMS.
```

```bash
systemctl status postgresql@12-main.service 
```
The output will be something like this
```text
postgresql@12-main.service - PostgreSQL Cluster 12-main
    Loaded: loaded (/lib/systemd/system/postgresql@.service; indirect; vendor preset: enabled)
    Active: active (running) since Sun 2019-10-06 10:23:49 UTC; 5min ago
  Main PID: 9242 (postgres)
     Tasks: 7 (limit: 2362)
    CGroup: /system.slice/system-postgresql.slice/postgresql@12-main.service
            ├─9242 /usr/lib/postgresql/12/bin/postgres -D /var/lib/postgresql/12/main -c config_file=/etc/postgresql/12/main/postgresql.conf
            ├─9254 postgres: 12/main: checkpointer   
            ├─9255 postgres: 12/main: background writer   
            ├─9256 postgres: 12/main: walwriter   
            ├─9257 postgres: 12/main: autovacuum launcher   
            ├─9258 postgres: 12/main: stats collector   
            └─9259 postgres: 12/main: logical replication launcher   
 Oct 06 10:23:47 ubuntu18 systemd[1]: Starting PostgreSQL Cluster 12-main…
 Oct 06 10:23:49 ubuntu18 systemd[1]: Started PostgreSQL Cluster 12-main.
 ```
 
 Enable postgres
 ```bash
 systemctl is-enabled postgresql
```

Edit postgresql.conf
```bash
sudo nano /etc/postgresql/12/main/postgresql.conf
```
Add this line and save it
```bash
shared_preload_libraries = 'timescaledb'
```
Start the postgres and create db and tables
```bash
sudo /etc/init.d/postgresql restart
sudo su - postgres
psql
ALTER USER postgres WITH PASSWORD 'password';
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS timescaledb;
create database orion_ld;
\c orion_ld;
drop table attribute_sub_properties_table; 
drop table attributes_table;
drop type attribute_value_type_enum;
drop table entity_table;
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE TABLE IF NOT EXISTS entity_table (entity_id TEXT NOT NULL,entity_type TEXT, geo_property GEOMETRY,created_at TIMESTAMP,modified_at TIMESTAMP, observed_at TIMESTAMP,PRIMARY KEY (entity_id));

CREATE EXTENSION IF NOT EXISTS timescaledb;

create type attribute_value_type_enum as enum ('value_string', 'value_number', 'value_boolean', 'value_relation', 'value_object', 'value_datetime', 'value_geo');


CREATE TABLE IF NOT EXISTS attributes_table (entity_id TEXT NOT NULL REFERENCES entity_table(entity_id),id TEXT NOT NULL, name TEXT, value_type attribute_value_type_enum, sub_property BOOL, unit_code TEXT, data_set_id TEXT, instance_id bigint GENERATED BY DEFAULT AS IDENTITY(START WITH 1 INCREMENT BY 1), value_string TEXT, value_boolean BOOL, value_number float8, value_relation TEXT, value_object TEXT, value_datetime TIMESTAMP,geo_property GEOMETRY,created_at TIMESTAMP NOT NULL,modified_at TIMESTAMP NOT NULL, observed_at TIMESTAMP NOT NULL,PRIMARY KEY (entity_id,id,observed_at,created_at,modified_at));
SELECT create_hypertable('attributes_table', 'modified_at'); 
   
CREATE TABLE IF NOT EXISTS attribute_sub_properties_table (entity_id TEXT NOT NULL,attribute_id TEXT NOT NULL,attribute_instance_id bigint, id TEXT NOT NULL, value_type attribute_value_type_enum,value_string TEXT, value_boolean BOOL, value_number float8, value_relation TEXT,name TEXT,geo_property GEOMETRY,unit_code TEXT, value_object TEXT, value_datetime TIMESTAMP,instance_id bigint GENERATED BY DEFAULT AS IDENTITY(START WITH 1 INCREMENT BY 1),PRIMARY KEY (instance_id));


\q
ctrl + d
```

#### Install nginx
```bash
sudo apt-get update
sudo apt-get install nginx
```

#### Install python packages
```bash
sudo apt install python3-pip python3-dev build-essential libssl-dev libffi-dev python3-setuptools
sudo apt-get install libpq-dev python-dev
pip3 install virtualenv
```

#### Here my ubuntu username is ubuntu. - So after this replace ubuntu with your username whereever you see ubuntu
```bash
cd /home/ubuntu
mkdir mintaka
git clone https://github.com/Trigyn-Technologies/Mintaka.git
cd Mintaka
python3 -m virtualenv mintakaenv
source mintakaenv/bin/activate
pip3 install wheel
pip3 install uwsgi flask
pip3 install --no-cache-dir lxml>=3.5.0
pip3 install PyLD
pip3 install validators
pip3 install -U flask-cors
pip3 install requests
pip3 install psycopg2
```

Create mintaka.service file
```bash
sudo nano /etc/systemd/system/mintaka.service
```
Add the below code in the file
```text
[Unit]
Description=uWSGI instance to serve mintaka
After=network.target
[Service]
User=ubuntu   
Group=www-data
WorkingDirectory=/home/ubuntu/mintaka/Mintaka
Environment="PATH=/home/ubuntu/mintaka/Mintaka/mintakaenv/bin"
ExecStart=/home/ubuntu/mintaka/Mintaka/mintakaenv/bin/uwsgi --ini mintaka.ini
[Install]
WantedBy=multi-user.target
```
Start mintaka service
```bash
sudo systemctl start mintaka
sudo systemctl enable mintaka
```
Check mintaka service
```bash
sudo systemctl status mintaka
```
The output will be like this
```text
mintaka.service - uWSGI instance to serve mintaka
   Loaded: loaded (/etc/systemd/system/mintaka.service; enabled; vendor preset: enabled)
   Active: active (running) since Wed 2020-08-05 11:05:56 UTC; 11s ago
 Main PID: 13653 (uwsgi)
    Tasks: 6 (limit: 4703)
   CGroup: /system.slice/mintaka.service
           ├─13653 /home/ubuntu/mintaka/Mintaka/mintakaenv/bin/uwsgi --ini mintaka.ini
           ├─13669 /home/ubuntu/mintaka/Mintaka/mintakaenv/bin/uwsgi --ini mintaka.ini
           ├─13670 /home/ubuntu/mintaka/Mintaka/mintakaenv/bin/uwsgi --ini mintaka.ini
           ├─13672 /home/ubuntu/mintaka/Mintaka/mintakaenv/bin/uwsgi --ini mintaka.ini
           ├─13674 /home/ubuntu/mintaka/Mintaka/mintakaenv/bin/uwsgi --ini mintaka.ini
           └─13675 /home/ubuntu/mintaka/Mintaka/mintakaenv/bin/uwsgi --ini mintaka.ini

Aug 05 11:05:56 mintaka systemd[1]: Started uWSGI instance to serve mintaka.
Aug 05 11:05:56 mintaka uwsgi[13653]: [uWSGI] getting INI configuration from mintaka.ini
```

Create file in nginx/sites-available
```bash
sudo nano /etc/nginx/sites-available/mintaka
```
Add the below code and save it
```text
server {
    listen 8000;
    server_name 0.0.0.0;
location / {
        include uwsgi_params;
        uwsgi_pass unix:/home/ubuntu/mintaka/Mintaka/mintaka.sock;
    }
}
```
Enable
```bash
sudo ln -s /etc/nginx/sites-available/mintaka /etc/nginx/sites-enabled
sudo nginx -t
```

Start nginix
```bash
sudo systemctl restart nginx
```

## Run using Docker-compose
```bash
docker-compose -f docker-compose.yml up -d
```
## Run on ubuntu 18.04
```bash
sudo systemctl restart nginx
sudo systemctl start mintaka
sudo /etc/init.d/postgresql restart
```

It will be running on your IP:8000
And you will see the output like
```text
Mintaka is running
````

## Below are the apis supported
```text
Resource URI: /temporal/entities/
Example: /temporal/entities?timerel=between&time=2020-01-23T14:20:00Z&endtime==2020-01-23T14:40:00Z
Method: GET

Resource URI:/temporal/entities/{entityId}
Example: /temporal/entities/{entityID}?timerel=between&time=2020-01-23T14:20:00Z&endtime==2020-01-23T14:40:00Z 
Method: GET
```

