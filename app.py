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
    conn = create_postgres_connection(request, app)
    context = get_context(request, app)
    if not conn:
      return Response("Database connection failed.", status=400, )
    cursor = conn.cursor()
    data = get_temporal_entities_parameters(args, context)
    if data['timerel'] not in ['after','before', 'between']:
      return Response("Wrong timerel property", status=400, )
    if data['timerel'] == 'between' and (not data['endtime']):
      return Response("Wrong endtime value", status=400, )
    statement, params = build_sql_query_for_entities(data)
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

def build_sql_query_for_entities(data):
  statement = ''
  params = {}
  try:
    if data['timerel'] == 'after':
      statement = "SELECT * FROM entity_table WHERE observedat>%(time)s"
      params["time"] = data['time']
    elif data['timerel'] == 'before':
      statement = "SELECT * FROM entity_table WHERE observedat<%(time)s"
      params["time"] = data['time']
    else:
      statement = "SELECT * FROM entity_table WHERE observedat>=%(time)s AND observedat<%(endtime)s"
      params["time"] = data['time']
      params["endtime"] = data['endtime']
    if data['id_data'] and len(data['id_data']) > 0:
      statement += ' AND lower(id) in ('
      for index in range(0,len(data['id_data'])):
        if index == (len(data['id_data']) -1):
          statement += '%(id_data'+str(index)+')s'
        else:
          statement += '%(id_data'+str(index)+')s,'
        params['id_data'+str(index)] = data['id_data'][index]
      statement += ')'
    if data['type_data'] and len(data['type_data']) > 0:
      statement += ' AND lower(type) in ('
      for index in range(0,len(data['type_data'])):
        if index == (len(data['type_data']) -1):
          statement += '%(type_data'+str(index)+')s'
        else:
          statement += '%(type_data'+str(index)+')s,'
        params['type_data'+str(index)] = data['type_data'][index]
      statement += ')'
    if data['idPattern']:
      statement += " AND lower(id) like %(idPattern)s"
      params["idPattern"] = '%{}%'.format(data['idPattern'].lower())
    statement +=" order by observedat desc"
    if data['lastN']:
      statement += " limit %(lastN)s"
      params["lastN"] = data['lastN']  
    statement += "%s"%(";")
  except Exception as e:
    app.logger.error("Error: build_sql_query_for_entities")
    app.logger.error(traceback.format_exc())
  return statement, params

def get_temporal_entities_parameters(args, context):
  data = {'timerel': None, 'time': None, 'endtime': None, 'timeproperty': 'observedAt', 'attrs': None, 'lastN': None, 'id_data': '', 'type_data': '','idPattern': None, 'q':None, 'csf':None, 'georel': None, 'geometry': None, 'coordinates': None, 'geoproperty': None}
  try:
    if 'timerel' in args:
      data['timerel'] = args.get('timerel')
      data['time'] = args.get('time')
      data['endtime'] = args.get('endtime', None)
      data['timeproperty'] = args.get('timeproperty', 'observedAt')
      data['time'] = datetime.datetime.strptime(data['time'], '%Y-%m-%dT%H:%M:%SZ').strftime("%Y-%m-%d %H:%M:%S")
      if data['endtime']:
        data['endtime'] = datetime.datetime.strptime(data['endtime'], '%Y-%m-%dT%H:%M:%SZ').strftime("%Y-%m-%d %H:%M:%S")
    if 'attrs' in args and args.get('attrs'):
      data['attrs'] = args.get('attrs').split(',')
    if 'lastN' in args and args.get('lastN'):
      data['lastN'] = args.get('lastN')
    if 'idPattern' in args and args.get('idPattern'):
      data['idPattern'] = args.get('idPattern')
    if 'q' in args and args.get('q'):
      data['q'] = args.get('q')
      data = get_q_params(data)
    if 'csf' in args and args.get('csf'):
      data['csf'] = args.get('csf')
    if 'georel' in args and args.get('georel'):
      data['georel'] = args.get('georel')
    if 'geometry' in args and args.get('geometry'):
      data['geometry'] = args.get('geometry')
    if 'coordinates' in args and args.get('coordinates'):
      data['coordinates'] = args.get('coordinates')
    if 'geoproperty' in args and args.get('geoproperty'):
      data['geoproperty'] = args.get('geoproperty')
    if 'id' in args:
      ids = args.get('id').lower()
      if ids:
        data['id_data'] = ids.split(',') 
    if 'type' in args:
      types = args.get('type')
      if types:
        data['type_data'] = types.split(',')
    data = expand_entities_params(data, context)
  except Exception as e:
    app.logger.error("Error: get_temporal_entities_parameters")
    app.logger.error(traceback.format_exc())
  return data

def get_q_params(data):
  op_list = ['==', '!=', '>=','<=','>', '<','!~=','~=', '|']
  try:
    # if '(' in data['q']:
    #   return data = get_q_params_with_multiple_operations(data)
    params = data['q'].replace(' ', '').split(';')
    q = []
    for param in params:
      flag = 0
      for op in op_list:
        if op in param:
          flag = 1
          param = param.split(op)
          if '..' in param[1]:
            param[1] = param[1].split('..')
          if '.' in param[0]:
            attrs = param[0].split('.')
            q.append({'attribute': attrs[0], 'operation': op, 'value': param[1], 'sub-attribute': attrs[1]})
          elif '[' in param[0] and ']' in param[0]:
            attrs = param[0].replace(']', '').split('[')
            q.append({'attribute': attrs[0], 'operation': op, 'value': param[1], 'column': attrs[1]})
          else:
            q.append({'attribute': param[0], 'operation': op, 'value': param[1]})
          break
      if flag == 0:
        q.append({'attribute': param, 'operation': 'having', 'value': param})
    data['q'] = q
  except Exception as e:
    app.logger.error("Error: get_q_params")
    app.logger.error(traceback.format_exc())
  return data

def get_q_params_with_multiple_operations(data):
  op_list = ['==', '!=', '>=','<=','>', '<','!~=','~=', '..', '|']
  q = []
  try:
    q_params = data['q']
    start = 0
    end = None
    count = 0
    for ct in q_params:
      if '(' == q_params[ct]:
        start = ct
        count += 1
      if count == 0 and ';' == q_params[ct]:
        end = ct + 1
        q.append(q_params[start:end])
        start = ct + 1
  except Exception as e:
    app.logger.error("Error: get_q_params_with_multiple_operations")
    app.logger.error(traceback.format_exc())
  return data


def expand_entities_params(data, context):
  context_list = []
  try:
    if context:
      if context in app.context_dict.keys():
        context_list.append(app.context_dict[context])
      else:
        context_list.append(context)
    context_list.append(app.context_dict[default_context])
    if data['attrs']:
      for count in range(0, len(data['attrs'])):
        if not validators.url(data['attrs'][count]):
          com = {"@context": context_list, data['attrs'][count]: data['attrs'][count]}
          expanded = jsonld.expand(com)
          data['attrs'][count] = list(expanded[0].keys())[0]
    if data['type_data']:
      for count in range(0, len(data['type_data'])):
        if not validators.url(data['type_data'][count]):
          com = {"@context": context_list, "@type": data['type_data'][count]}
          expanded = jsonld.expand(com)
          data['type_data'][count] = expanded[0]['@type'][0]
    if data['q']:
      for count in range(0, len(data['q'])):
        if (not validators.url(data['q'][count]['attribute'])):
          com = {"@context": context_list, data['q'][count]['attribute']: data['q'][count]['attribute']} 
          expanded = jsonld.expand(com)
          data['q'][count]['attribute'] = list(expanded[0].keys())[0]
          if 'sub-attribute' in data['q'][count]:
            com = {"@context": context_list, data['q'][count]['sub-attribute']: data['q'][count]['sub-attribute']} 
            expanded = jsonld.expand(com)
            data['q'][count]['sub-attribute'] = list(expanded[0].keys())[0]
    app.logger.info(data)
  except Exception as e:
    app.logger.error("Error: expand_entities_params")
    app.logger.error(traceback.format_exc())
  return data


@app.route('/temporal/entities/<entity_id>/', methods=['GET'])
def get_temporal_entity(entity_id):
  response_data = []
  try:
    if request.method != 'GET':
      return Response("No get data", status=400, )
    args = request.args
    context = get_context(request, app)
    conn = create_postgres_connection(request, app)
    if not conn:
      return Response("Database connection failed.", status=400, )
    cursor = conn.cursor()
    data = get_temporal_entity_parameters(args, context, app)
    if data['timerel'] not in ['after','before', 'between']:
      return Response("Wrong timerel property", status=400, )
    if data['timerel'] == 'between' and (not data['endtime']):
      return Response("Wrong endtime value", status=400, )
    statement, params = build_sql_query_for_entity(data, entity_id, app)
    cursor.execute(statement,params)
    record = cursor.fetchall()
    app.logger.info(len(record))
    if len(record):
      response_data = build_response_data_for_entity(record, context, data, app)
    else:
      response_data = {}
    response = app.response_class(response=json.dumps(response_data, indent=2), status=200,mimetype='application/json')
    close_postgres_connection(cursor, conn, app)
    return response
  except Exception as e:
    close_postgres_connection(cursor, conn, app)
    app.logger.error("Error: get_temporal_entity")
    app.logger.error(traceback.format_exc())
    abort(400)






load_context(default_context, app)

# if __name__ == '__main__':
  
  #app.run(debug=True, host='0.0.0.0')
