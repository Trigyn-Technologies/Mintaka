FROM python:3-alpine
# Set an environment variable with the directory
# where we'll be running the app
ENV APP /app
# Create the directory and instruct Docker to operate
# from there from now on
RUN mkdir $APP
WORKDIR $APP
EXPOSE 5000
# Copy the requirements file in order to install
# Python dependencies
COPY requirements.txt .
# Install Python dependencies
RUN pip install -r requirements.txt                             
RUN apk update && apk add postgresql-dev gcc python3-dev musl-dev
RUN pip3 install psycopg2
RUN apk add --no-cache --virtual .build-deps gcc libc-dev libxslt-dev && \
    apk add --no-cache libxslt && \
    pip install --no-cache-dir lxml>=3.5.0 && \
    apk del .build-deps
RUN pip3 install PyLD
RUN pip3 install validators
RUN pip3 install -U flask-cors
RUN pip3 install requests
# We copy the rest of the codebase into the image
COPY . .
ENV PYTHONPATH=$PWD:$PYTHONPATH
#CMD python app.py 