from flask import Flask, request, Response, abort
import json
import datetime
import traceback
from pyld import jsonld
import validators
from flask_cors import CORS
import logging
from resources.postgres import *
from resources.context import *
from resources.entity import *
from resources.entities import *

app = Flask(__name__)
app.debug = True
CORS(app)
jsonld.set_document_loader(jsonld.requests_document_loader(timeout=4))
logging.basicConfig(level=logging.DEBUG)
app.context_dict = {}

@app.route('/temporal/entities/', methods=['GET'])
def get_temporal_entities():
  response_data = []
  try:
    if request.method != 'GET':
      return Response("No get data", status=400, )
    args = request.args
    context, status, error = get_context(request, app)
    if not status:
      return Response(error, status=400, )
    conn, status, error = create_postgres_connection(request, app)
    if not status:
      return Response(error, status=400, )
    cursor = conn.cursor()
    data = get_temporal_entities_parameters(args, context, app)
    if data['timerel'] not in ['after','before', 'between']:
      return Response("Wrong timerel property", status=400, )
    if data['timerel'] == 'between' and (not data['endtime']):
      return Response("Wrong endtime value", status=400, )
    statement, params = build_sql_query_for_entities(data, app)
    cursor.execute(statement, params)
    for i, record in enumerate(cursor):
      record = list(record)
      record[0] = record[0].strftime("%Y-%m-%dT%H:%M:%SZ")
      response_data.append(record)
    response = app.response_class(response=json.dumps(response_data, indent=2), status=200,mimetype='application/json')
    close_postgres_connection(cursor, conn, app)
    return response
  except Exception as e:
    close_postgres_connection(cursor, conn, app)
    app.logger.error("Error: get_temporal_entity")
    app.logger.error(e)
    app.logger.error(traceback.format_exc())
    abort(400)



@app.route('/temporal/entities/<entity_id>/', methods=['GET'])
def get_temporal_entity(entity_id):
  response_data = []
  try:
    if request.method != 'GET':
      return Response("No get data", status=400, )
    args = request.args
    context, status, error = get_context(request, app)
    if not status:
      return Response(error, status=400, )
    conn, status, error = create_postgres_connection(request, app)
    if not status:
      return Response(error, status=400, )
    cursor = conn.cursor()
    data, status, error = get_temporal_entity_parameters(args, context, app)
    if not status:
      return Response(error, status=400, )
    if data['timerel'] not in ['after','before', 'between']:
      return Response("Wrong timerel property", status=400, )
    if data['timerel'] == 'between' and (not data['endtime']):
      return Response("Wrong endtime value", status=400, )
    statement, params, status, error = build_sql_query_for_entity(data, entity_id, app)
    if not status:
      return Response(error, status=400, )
    cursor.execute(statement,params)
    record = cursor.fetchall()
    app.logger.info(len(record))
    if len(record):
      response_data , status, error= build_response_data_for_entity(record, context, data, app)
      if not status:
        return Response(error, status=400, )
    else:
      response_data = {}
    response = app.response_class(response=json.dumps(response_data, indent=2), status=200,mimetype='application/json')
    status, error = close_postgres_connection(cursor, conn, app)
    if not status:
      return Response(error, status=400, )
    return response
  except Exception as e:
    close_postgres_connection(cursor, conn, app)
    app.logger.error("Error: get_temporal_entity")
    app.logger.error(traceback.format_exc())
    abort(400)






load_context(default_context, app)

# if __name__ == '__main__':
  
  #app.run(debug=True, host='0.0.0.0')
