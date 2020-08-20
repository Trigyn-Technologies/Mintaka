########################################################################################
#
# This build stage builds the sources
#
######################################################################################## 

FROM python:3.7-slim AS builder

# Create the directory and instruct Docker to operate
# from there from now on
RUN mkdir /app
WORKDIR /app
# Copy the requirements file in order to install
# Python dependencies
COPY requirements.txt .
# Install Python dependencies
RUN pip install -r requirements.txt                             
RUN apt-get update && \
	apt-get install -y --no-install-recommends postgresql gcc python3-dev python-psycopg2 libpq-dev
RUN pip3 install psycopg2
RUN pip install --no-cache-dir lxml>=3.5.0 
RUN pip3 install PyLD
RUN pip3 install validators
RUN pip3 install -U flask-cors
RUN pip3 install requests
# We copy the rest of the codebase into the image
COPY . .

########################################################################################
#
# This build stage creates an anonymous user to be used with the distroless build
# as defined below.
#
########################################################################################
FROM python:3.7-slim AS anon-user
RUN sed -i -r "/^(root|nobody)/!d" /etc/passwd /etc/shadow /etc/group \
    && sed -i -r 's#^(.*):[^:]*$#\1:/sbin/nologin#' /etc/passwd

########################################################################################
#
# This build stage creates a distroless image for production.
#
##############################################################################

FROM gcr.io/distroless/python3
COPY --from=builder /app /app
COPY --from=builder /usr/local/lib/python3.7/site-packages /usr/local/lib/python3.7/site-packages
COPY --from=anon-user /etc/passwd /etc/shadow /etc/group /etc/

USER nobody
WORKDIR /app
# Set an environment variable with the directory
# where we'll be running the app
ENV APP /app
ENV PYTHONPATH=$PWD:$PYTHONPATH

LABEL "maintainer"="Trigyn Technologies"
LABEL "org.opencontainers.image.authors"="Ravishankar Jethani"
LABEL "org.opencontainers.image.documentation"="https://github.com/Trigyn-Technologies/Mintaka/README.md"
LABEL "org.opencontainers.image.vendor"="Trigyn-Technologies."
LABEL "org.opencontainers.image.licenses"=""
LABEL "org.opencontainers.image.title"="Mintaka"
LABEL "org.opencontainers.image.description"="A FIWARE Generic Enabler to support the usage of NGSI-LD data in time-series databases and serve temporal APIs"
LABEL "org.opencontainers.image.source"="https://github.com/Trigyn-Technologies/Mintaka"

CMD ["app.py"]
EXPOSE 5000
