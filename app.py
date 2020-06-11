from flask import Flask, request, Response, abort
import os
from psycopg2 import connect
import json
import datetime
import traceback
from pyld import jsonld
import validators
from flask_cors import CORS
import logging
import requests

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
    conn = create_postgres_connection(request)
    context = get_context(request)
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
    close_postgres_connection(cursor, conn)
    return response
  except Exception as e:
    close_postgres_connection(cursor, conn)
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
  op_list = ['==', '!=', '>=','<=','>', '<','!~=','~=', '..']
  try:
    params = data['q'].replace(' ', '').split(';')
    q = []
    for param in params:
      flag = 0
      for op in op_list:
        if op in param:
          flag = 1
          param = param.split(op)
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
    context = get_context(request)
    conn = create_postgres_connection(request)
    if not conn:
      return Response("Database connection failed.", status=400, )
    cursor = conn.cursor()
    data = get_temporal_entity_parameters(args, context)
    if data['timerel'] not in ['after','before', 'between']:
      return Response("Wrong timerel property", status=400, )
    if data['timerel'] == 'between' and (not data['endtime']):
      return Response("Wrong endtime value", status=400, )
    statement, params = build_sql_query_for_entity(data, entity_id)
    cursor.execute(statement,params)
    record = cursor.fetchall()
    app.logger.info(len(record))
    if len(record):
      response_data = build_response_data_for_entity(record, context, data)
    else:
      response_data = {}
    response = app.response_class(response=json.dumps(response_data, indent=2), status=200,mimetype='application/json')
    close_postgres_connection(cursor, conn)
    return response
  except Exception as e:
    close_postgres_connection(cursor, conn)
    app.logger.error("Error: get_temporal_entity")
    app.logger.error(traceback.format_exc())
    abort(400)

def build_response_data_for_entity(record, context, data):
  response_data = {}
  entity_val = {'id': 0, 'type':1, 'location':2 ,'createdAt':3, 'modifiedAt':4, 'observedAt':5}
  try:
    first = record[0]
    response_data['id'] = first[entity_val['id']]
    response_data['type'] = compact_entity_params(first[entity_val['type']], context)
    if first[entity_val['createdAt']]:
      response_data['createdAt'] = {"type": "Property",  "value": {"@type": "DateTime","@value":first[entity_val['createdAt']].replace(' ','')}}
    if first[entity_val['modifiedAt']]:
      response_data['modifiedAt'] = {"type": "Property",  "value": {"@type": "DateTime","@value":first[entity_val['modifiedAt']].replace(' ','')}}
    if first[entity_val['observedAt']]:
      response_data['observedAt'] = {"type": "Property",  "value": {"@type": "DateTime","@value":first[entity_val['observedAt']].replace(' ','')}}
    if first[entity_val['location']]:
      response_data['location'] = {"type": "GeoProperty", 'value': json.loads(first[entity_val['location']])}
    if 'options' in data and data['options'] == 'temporalValues':
      response_data = build_temporal_response_data_for_entity(record, response_data, context, data)
    else:
      response_data = build_normal_response_data_for_entity(record, response_data, context)
    if context:
      response_data['@context'] = [context, default_context]
    else:
      response_data['@context'] = default_context
  except Exception as e:
    app.logger.error("Error: build_response_data_for_entity")
    app.logger.error(traceback.format_exc())
  return response_data

def build_normal_response_data_for_entity(record_list, response_data, context):
  compacted_dict = {}
  attr_val = {'id': 7, 'value_type': 8, 'sub_property': 9, 'unit_code': 10, 'data_set_id': 11, 'value_string': 13,  'value_boolean': 14, 'value_number':15, 'value_relation': 16, 'value_object':17, 'location':18 ,'createdAt':19, 'modifiedAt':20, 'observedAt':21, 'value_datetime':32}
  subattr_val = {'id': 23, 'value_type': 24, 'value_string': 25,  'value_boolean': 26, 'value_number':27, 'value_relation': 28, 'location':29 , 'value_object':30, 'unit_code': 31, 'value_datetime':33}
  try:
    for record in record_list:
      attr = compact_entity_params(record[attr_val['id']], context, compacted_dict)
      if attr not in response_data.keys():
        response_data[attr] = []
      attr_dict = {}
      if record[attr_val['value_type']] == 'value_string':
        attr_dict['type'] = 'Property'
        attr_dict['value'] = record[attr_val['value_string']]
      elif record[attr_val['value_type']] == 'value_boolean':
        attr_dict['type'] = 'Property'
        if record[attr_val['value_boolean']]:
          attr_dict['value'] = 'true'
        else:
          attr_dict['value'] = 'false'
      elif record[attr_val['value_type']] == 'value_number':
        attr_dict['type'] = 'Property'
        attr_dict['value'] = record[attr_val['value_number']]
      elif record[attr_val['value_type']] == 'value_relation':
        attr_dict['type'] = 'Relationship'
        attr_dict['object'] = record[attr_val['value_relation']]
      elif record[attr_val['value_type']] == 'value_datetime':
        attr_dict['type'] = 'Property'
        attr_dict['object'] = {"@type": "DateTime","@value":record[attr_val['value_datetime']].replace(' ', '')}
      elif record[attr_val['value_type']] == 'value_object':
        attr_dict['type'] = 'Property'
        try:
          attr_dict['value'] = json.loads(record[attr_val['value_object']])
        except:
          attr_dict['value'] = record[attr_val['value_object']]
      if record[attr_val['unit_code']]:
        attr_dict['unitCode'] = record[attr_val['unit_code']]
      if record[attr_val['location']]:
        attr_dict['location'] = {"type": "GeoProperty", 'value': json.loads(record[attr_val['location']])}
      if record[attr_val['createdAt']]:
        attr_dict['createdAt'] = record[attr_val['createdAt']].replace(' ', '')
      if record[attr_val['modifiedAt']]:
        attr_dict['modifiedAt'] = record[attr_val['modifiedAt']].replace(' ', '')
      if record[attr_val['observedAt']]:
        attr_dict['observedAt'] = record[attr_val['observedAt']].replace(' ', '')
      if record[attr_val['sub_property']] and record[subattr_val['id']]:
        subattr = compact_entity_params(record[subattr_val['id']], context, compacted_dict)
        subattr_dict = {}
        if record[subattr_val['value_type']] == 'value_string':
          subattr_dict['type'] = 'Property'
          subattr_dict['value'] = record[subattr_val['value_string']]
        elif record[subattr_val['value_type']] == 'value_boolean':
          subattr_dict['type'] = 'Property'
          if record[subattr_val['value_boolean']]:
            subattr_dict['value'] = 'true'
          else:
            subattr_dict['value'] = 'false'
        elif record[subattr_val['value_type']] == 'value_number':
          subattr_dict['type'] = 'Property'
          subattr_dict['value'] = record[subattr_val['value_number']]
        elif record[subattr_val['value_type']] == 'value_datetime':
          subattr_dict['type'] = 'Property'
          subattr_dict['value'] = {"@type": "DateTime","@value":record[subattr_val['value_datetime']].replace(' ', '')}
        elif record[subattr_val['value_type']] == 'value_relation':
          subattr_dict['type'] = 'Relationship'
          subattr_dict['object'] = record[subattr_val['value_relation']]
        elif record[subattr_val['value_type']] == 'value_object':
          subattr_dict['type'] = 'Property'
          try:
            subattr_dict['value'] = json.loads(record[subattr_val['value_object']])
          except:
            subattr_dict['value'] = record[subattr_val['value_object']]
        if record[subattr_val['unit_code']]:
          subattr_dict['unitCode'] = record[subattr_val['unit_code']]
        if record[subattr_val['location']]:
          subattr_dict['location'] = {"type": "GeoProperty", 'value': json.loads(record[subattr_val['location']])}
        attr_dict[subattr] = subattr_dict
      response_data[attr].append(attr_dict)
  except Exception as e:
    app.logger.error("Error: build_normal_response_data_for_entity")
    app.logger.error(traceback.format_exc())
  return response_data

def build_temporal_response_data_for_entity(record_list, response_data, context, data):
  compacted_dict = {}
  attr_val = {'id': 7, 'value_type': 8, 'sub_property': 9, 'unit_code': 10, 'data_set_id': 11, 'value_string': 13,  'value_boolean': 14, 'value_number':15, 'value_relation': 16, 'value_object':17, 'location':18 ,'createdAt':19, 'modifiedAt':20, 'observedAt':21, 'value_datetime':32}
  subattr_val = {'id': 23, 'value_type': 24, 'value_string': 25,  'value_boolean': 26, 'value_number':27, 'value_relation': 28, 'location':29 , 'value_object':30, 'unit_code': 31, 'value_datetime':33}
  try:
    for record in record_list:
      attr = compact_entity_params(record[attr_val['id']], context, compacted_dict)
      if attr not in response_data.keys():
        response_data[attr] = {'type': '', 'value': [], 'unitCode': [], 'location': {'type': 'GeoProperty', 'value': []}}
      attr_list = [] 
      if record[attr_val['value_type']] == 'value_string':
        response_data[attr]['type'] = 'Property'
        attr_list.append(record[attr_val['value_string']])
      elif record[attr_val['value_type']] == 'value_boolean':
        response_data[attr]['type'] = 'Property'
        if record[attr_val['value_boolean']]:
          attr_list.append('true')
        else:
         attr_list.append('false')
      elif record[attr_val['value_type']] == 'value_number':
        response_data[attr]['type'] = 'Property'
        attr_list.append(record[attr_val['value_number']])
      elif record[attr_val['value_type']] == 'value_relation':
        response_data[attr]['type'] = 'Relationship'
        attr_list.append(record[attr_val['value_relation']])
      elif record[attr_val['value_type']] == 'value_datetime':
        response_data[attr]['type'] = 'Property'
        attr_list.append(record[attr_val['value_datetime']].replace(' ', ''))
      elif record[attr_val['value_type']] == 'value_object':
        response_data[attr]['type'] = 'Property'
        try:
         attr_list.append(json.loads(record[attr_val['value_object']]))
        except:
          attr_list.append(record[attr_val['value_object']])
      if data['timeproperty'] == 'created_at':
        if record[attr_val['createdAt']]:
          attr_list.append(record[attr_val['createdAt']].replace(' ', ''))
        else:
          attr_list.append('')
      elif data['timeproperty'] == 'observed_at':
        if record[attr_val['observedAt']]:
          attr_list.append(record[attr_val['observedAt']].replace(' ', ''))
        else:
          attr_list.append('')
      else:
        if record[attr_val['modifiedAt']]:
          attr_list.append(record[attr_val['modifiedAt']].replace(' ', ''))
        else:
          attr_list.append('')
      if record[attr_val['unit_code']]:
        response_data[attr]['unitCode'].append(record[attr_val['unit_code']])
      else:
        response_data[attr]['unitCode'].append('')
      if record[attr_val['location']]:
        response_data[attr]['location']['value'].append(json.loads(record[attr_val['location']]))
      else:
        response_data[attr]['location']['value'].append('')
      response_data[attr]['value'].append(attr_list)
      if record[attr_val['sub_property']] and record[subattr_val['id']]:
        subattr = compact_entity_params(record[subattr_val['id']], context, compacted_dict)
        if subattr not in response_data[attr].keys():
          response_data[attr][subattr] = {'type': '', 'value': [], 'unitCode': [], 'location': {'type': 'GeoProperty', 'value': []}}
        subattr_dict = {}
        if record[subattr_val['value_type']] == 'value_string':
          response_data[attr][subattr]['type'] = 'Property'
          response_data[attr][subattr]['value'].append(record[subattr_val['value_string']])
        elif record[subattr_val['value_type']] == 'value_boolean':
          response_data[attr][subattr]['type'] = 'Property'
          if record[subattr_val['value_boolean']]:
            response_data[attr][subattr]['value'].append('true')
          else:
            response_data[attr][subattr]['value'].append('false')
        elif record[subattr_val['value_type']] == 'value_number':
          response_data[attr][subattr]['type'] = 'Property'
          response_data[attr][subattr]['value'].append(record[subattr_val['value_number']])
        elif record[subattr_val['value_type']] == 'value_relation':
          response_data[attr][subattr]['type'] = 'Relationship'
          response_data[attr][subattr]['value'].append(record[subattr_val['value_relation']])
        elif record[subattr_val['value_type']] == 'value_datetime':
          response_data[attr][subattr]['type'] = 'Property'
          response_data[attr][subattr]['value'].append(record[subattr_val['value_datetime']].replace(' ', ''))
        elif record[subattr_val['value_type']] == 'value_object':
          response_data[attr][subattr]['type'] = 'Property'
          try:
            response_data[attr][subattr]['value'].append(json.loads(record[subattr_val['value_object']]))
          except:
            response_data[attr][subattr]['value'].append(record[subattr_val['value_object']])
        if record[subattr_val['unit_code']]:
          response_data[attr][subattr]['unitCode'].append(record[subattr_val['unit_code']])
        if record[subattr_val['location']]:
          response_data[attr][subattr]['location']['value'].append({"type": "GeoProperty", 'value': json.loads(record[subattr_val['location']])})
  except Exception as e:
    app.logger.error("Error: build_temporal_response_data_for_entity")
    app.logger.error(traceback.format_exc())
  return response_data

def compact_entity_params(attr, context, compacted_dict = {}):
  context_list = []
  attr_key = attr
  default_context_compact = 'https://uri.etsi.org/ngsi-ld/default-context/'
  try:
    if attr in compacted_dict:
      return compacted_dict[attr]
    if default_context_compact in attr:
      attr = attr.replace(default_context_compact, '')
      compacted_dict[attr_key] = attr
      return attr
    if context:
      if context in app.context_dict.keys():
        context_list.append(app.context_dict[context])
      else:
        context_list.append(context)
    context_list.append(app.context_dict[default_context])
    con = {"@context": context_list}
    com =  {attr: attr}
    compacted = jsonld.compact(com, con)
    attr = list(compacted.keys())[1]
    compacted_dict[attr_key] = attr
  except Exception as e:
    app.logger.error("Error: compact_entity_params")
    app.logger.error(traceback.format_exc())
  return attr

def build_sql_query_for_entity(data, entity_id):
  statement = start_statement
  params = {}
  try:
    if data['timerel'] == 'after':
      statement += " WHERE attributes_table."+ data['timeproperty']+">%(time)s"
      params["time"] = data['time']
    elif data['timerel'] == 'before':
      statement += " WHERE attributes_table."+ data['timeproperty']+"<%(time)s"
      params["time"] = data['time']
    else:
      statement += " WHERE attributes_table."+ data['timeproperty']+">=%(time)s AND attributes_table."+ data['timeproperty']+"<%(endtime)s"
      params["time"] = data['time']
      params["endtime"] = data['endtime']
    if data['attrs'] and len(data['attrs']) > 0:
      statement += ' AND attributes_table.id in ('
      for index in range(0,len(data['attrs'])):
        if index == (len(data['attrs']) -1):
          statement += '%(attrs'+str(index)+')s'
        else:
          statement += '%(attrs'+str(index)+')s,'
        params['attrs'+str(index)] = data['attrs'][index]
      statement += ')'
    statement += " AND attributes_table.entity_id = %(entity_id)s order by attributes_table."+ data['timeproperty']+" desc"
    params["entity_id"] = entity_id
    if data['lastN']:
      statement += " limit %(lastN)s"
      params["lastN"] = data['lastN'] 
    statement += "%s"%(';')
  except Exception as e:
    app.logger.error("Error: build_sql_query_for_entity")
    app.logger.error(traceback.format_exc())
  return statement, params

def get_temporal_entity_parameters(args, context):
  data = {'timerel': None, 'time': None, 'endtime': None, 'timeproperty': 'observedAt', 'attrs': None, 'lastN': None}
  timepropertyDict = {'modifiedAt':'modified_at', 'observedAt' :'observed_at', 'createdAt':'created_at'}
  try:
    if 'timerel' in args:
      data['timerel'] = args.get('timerel')
      data['time'] = args.get('time')
      data['endtime'] = args.get('endtime', None)
      if 'timeproperty' in args and args['timeproperty'] in timepropertyDict.keys():
        data['timeproperty'] = timepropertyDict[args['timeproperty']]
      else:
        data['timeproperty'] = 'modified_at'
      data['time'] = datetime.datetime.strptime(data['time'], '%Y-%m-%dT%H:%M:%SZ').strftime("%Y-%m-%d %H:%M:%S")
      if data['endtime']:
        data['endtime'] = datetime.datetime.strptime(data['endtime'], '%Y-%m-%dT%H:%M:%SZ').strftime("%Y-%m-%d %H:%M:%S")
    if 'attrs' in args and args.get('attrs'):
      data['attrs'] = args.get('attrs').split(',')
    if 'lastN' in args and args.get('lastN'):
      data['lastN'] = args.get('lastN')
    if data['timeproperty'] not in ['modified_at', 'observed_at', 'created_at']:
      data['timeproperty'] = 'modified_at'
    if 'options' in args and args.get('options'):
      data['options'] = args.get('options')
    data = expand_entity_params(data, context)
  except Exception as e:
    app.logger.error("Error: get_temporal_entity_parameters")
    app.logger.error(traceback.format_exc())
  return data

def expand_entity_params(data, context):
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
    app.logger.info(data)
  except Exception as e:
    app.logger.error("Error: expand_entities_params")
    app.logger.error(traceback.format_exc())
  return data

def create_postgres_connection(request):
  conn = None
  try:
    if 'NGSILD-Tenant' in request.headers and request.headers['NGSILD-Tenant']:
      dbName = request.headers['NGSILD-Tenant']
    else:
      dbName = os.getenv('POSTGRES_DB')
    user = os.getenv('POSTGRES_USER')
    host = os.getenv('POSTGRES_HOST')
    password = os.getenv('POSTGRES_PASSWORD')
    conn = connect(dbname = dbName, user = user,host = host,password = password)
  except Exception as e:
    app.logger.error("Error: create_postgres_connection")
    app.logger.error(traceback.format_exc())
  return conn

def close_postgres_connection(cursor, conn):
  try:
    cursor.close()
    conn.close()
  except Exception as e:
    app.logger.error("Error: close_postgres_connection")
    app.logger.error(traceback.format_exc())

def get_context(request):
  context = ''
  try:
    if 'Link' in request.headers and request.headers['Link']:
      context = request.headers['Link'].replace('<','').replace('>','').split(';')[0]
      if context not in app.context_dict.keys():
        load_context(context)
  except Exception as e:
    app.logger.error("Error: get_context")
    app.logger.error(traceback.format_exc())
  return context

def load_context(context):
  try:
    if context:
      headers = {'Accept': 'application/ld+json, application/json;q=0.5'} 
      headers['Accept'] = headers['Accept'] + ', text/html;q=0.8, application/xhtml+xml;q=0.8'
      response = requests.request('GET', context, headers = headers)
      response = json.loads(response.text)
      if '@context' in response:
        app.context_dict[context] = response['@context']
  except Exception as e:
    app.logger.error("Error: load_context")
    app.logger.error(traceback.format_exc())

start_statement = "SELECT entity_table.entity_id as entity_id, entity_table.entity_type as entity_type,ST_AsGeoJSON(entity_table.geo_property) as entity_geo_property,to_char(entity_table.created_at, 'YYYY-MM-DD T HH24:MI:SS.MSZ') as entity_created_at,to_char(entity_table.modified_at, 'YYYY-MM-DD T HH24:MI:SS.MSZ') as entity_modified_at, to_char(entity_table.observed_at, 'YYYY-MM-DD T HH24:MI:SS.MSZ') as entity_observed_at, attributes_table.name as attribute_name, attributes_table.id as attribute_id, attributes_table.value_type as attribute_value_type, attributes_table.sub_property as attribute_sub_property, attributes_table.unit_code as attribute_unit_code, attributes_table.data_set_id as attribute_data_set_id, attributes_table.instance_id as attribute_instance_id, attributes_table.value_string as attribute_value_string, attributes_table.value_boolean as attribute_value_boolean, attributes_table.value_number as attribute_value_number, attributes_table.value_relation as attribute_value_relation, attributes_table.value_object as attribute_value_object, ST_AsGeoJSON(attributes_table.geo_property) as attribute_geo_property, to_char(attributes_table.created_at, 'YYYY-MM-DD T HH24:MI:SS.MSZ') as attribute_created_at, to_char(attributes_table.modified_at, 'YYYY-MM-DD T HH24:MI:SS.MSZ') as attribute_modified_at, to_char(attributes_table.observed_at, 'YYYY-MM-DD T HH24:MI:SS.MSZ') as attribute_observed_at, attribute_sub_properties_table.name as subattribute_name, attribute_sub_properties_table.id as subattribute_id, attribute_sub_properties_table.value_type as subattribute_value_type, attribute_sub_properties_table.value_string as subattribute_value_string, attribute_sub_properties_table.value_boolean as subattribute_value_boolean, attribute_sub_properties_table.value_number as subattribute_value_number, attribute_sub_properties_table.value_relation as subattribute_value_relation, ST_AsGeoJSON(attribute_sub_properties_table.geo_property) as subattribute_geo_property, attribute_sub_properties_table.value_object as subattribute_value_object,attribute_sub_properties_table.unit_code as subattribute_unit_code, to_char(attributes_table.value_datetime, 'YYYY-MM-DD T HH24:MI:SS.MSZ') as attribute_value_datetime, to_char(attribute_sub_properties_table.value_datetime, 'YYYY-MM-DD T HH24:MI:SS.MSZ') as subattribute_value_datetime from attributes_table FULL OUTER JOIN entity_table ON entity_table.entity_id = attributes_table.entity_id FULL OUTER JOIN attribute_sub_properties_table ON attribute_sub_properties_table.attribute_instance_id = attributes_table.instance_id"

default_context = 'https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld'
load_context(default_context)

# if __name__ == '__main__':
  
  #app.run(debug=True, host='0.0.0.0')