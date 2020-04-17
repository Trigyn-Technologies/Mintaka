# NGSI-LD-Temporal-App
This app is for NGSI-LD temporal queries.
It is developed using python-flask and postgres db.
The server is running using nginx.
To run it, enter your postgres detials in DockerFile.
Now run using docker-compose and check the app on localhost.
Also go in postgres bash and enter to database and run these commands CREATE EXTENSION postgis; and create hypertable also for timescale.