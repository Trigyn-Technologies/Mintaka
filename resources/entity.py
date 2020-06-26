import json
import traceback
from resources.postgres import start_statement
from resources.context import default_context
from resources.records import *
import datetime
import validators
from pyld import jsonld

def build_response_data_for_entity(record, context, data, app):
  """Build response for entity api"""
  response_data = {}
  entity_val = {'id': 0, 'type':1, 'location':2 ,'createdAt':3, 'modifiedAt':4, 'observedAt':5}
  status = 0
  error = 'Error in building response data for entity'
  try:
    first = record[0]
    response_data['id'] = first[entity_val['id']]
    response_data['type'] = compact_entity_params(first[entity_val['type']], context, {} ,app)
    if 'options' in data and data['options'] == 'sysAttrs':
      if first[entity_val['createdAt']]:
        response_data['createdAt'] = first[entity_val['createdAt']].replace(' ','')
      if first[entity_val['modifiedAt']]:
        response_data['modifiedAt'] = first[entity_val['modifiedAt']].replace(' ','')
      if first[entity_val['observedAt']]:
        response_data['observedAt'] = first[entity_val['observedAt']].replace(' ','')
    else:
      if first[entity_val['observedAt']]:
        response_data['observedAt'] = first[entity_val['observedAt']].replace(' ','')
    if first[entity_val['location']]:
      response_data['location'] = {"type": "GeoProperty", 'value': json.loads(first[entity_val['location']])}
    if 'options' in data and data['options'] == 'temporalValues':
      response_data, status, error = build_temporal_response_data_for_entity(record, response_data, context, data, app)
    else:
      response_data, status, error = build_normal_response_data_for_entity(record, response_data, context, data, app)
    if context:
      response_data['@context'] = [context, default_context]
    else:
      response_data['@context'] = default_context
  except Exception as e:
    app.logger.error("Error: build_response_data_for_entity")
    app.logger.error(traceback.format_exc())
  return response_data, status, error

def build_normal_response_data_for_entity(record_list, response_data, context, data, app):
  """Build response if options = None"""
  get_attr_val_dict = {'value_string': get_normal_value_string,  'value_boolean': get_normal_value_boolean, 'value_number':get_normal_value_number, 'value_relation': get_normal_value_relation, 'value_object':get_normal_value_object,'value_datetime':get_normal_value_datetime}
  compacted_dict = {}
  status = 0
  error = 'Error in building normal response data for entity.'
  attrs_list = []
  attrs_index = []
  attr_val = {'id': 7, 'value_type': 8, 'sub_property': 9, 'unit_code': 10, 'data_set_id': 11, 'instance_id':12, 'value_string': 13,  'value_boolean': 14, 'value_number':15, 'value_relation': 16, 'value_object':17, 'location':18 ,'createdAt':19, 'modifiedAt':20, 'observedAt':21, 'value_datetime':32 }
  subattr_val = {'id': 23, 'value_type': 24, 'value_string': 25,  'value_boolean': 26, 'value_number':27, 'value_relation': 28, 'location':29 , 'value_object':30, 'unit_code': 31, 'value_datetime':33}
  try:
    for record in record_list:
      attr = compact_entity_params(record[attr_val['id']], context, compacted_dict, app)
      if attr not in response_data.keys():
        response_data[attr] = []
      elif 'lastN' in data and data['lastN'] and len(response_data[attr]) >= data['lastN']:
        continue
      attr_dict = {}
      if record[attr_val['value_type']] in ['value_string', 'value_boolean', 'value_number', 'value_relation', 'value_object','value_datetime']:
        attr_dict = get_attr_val_dict[record[attr_val['value_type']]](record, attr_val, attr_dict)
      if record[attr_val['unit_code']]:
        attr_dict['unitCode'] = record[attr_val['unit_code']]
      if record[attr_val['location']]:
        attr_dict['location'] = {"type": "GeoProperty", 'value': json.loads(record[attr_val['location']])}
      if 'options' in data and data['options'] == 'sysAttrs':
        if record[attr_val['createdAt']]:
          attr_dict['createdAt'] = record[attr_val['createdAt']].replace(' ', '')
        if record[attr_val['modifiedAt']]:
          attr_dict['modifiedAt'] = record[attr_val['modifiedAt']].replace(' ', '')
      if record[attr_val['observedAt']]:
        attr_dict['observedAt'] = record[attr_val['observedAt']].replace(' ', '')
      if record[attr_val['instance_id']] not in attrs_list:
        attrs_list.append(record[attr_val['instance_id']])
        response_data[attr].append(attr_dict)
        attrs_index.append(len(response_data[attr]) -1)
      if record[attr_val['sub_property']] and record[subattr_val['id']]:
        subattr = compact_entity_params(record[subattr_val['id']], context, compacted_dict, app)
        subattr_dict = {}
        if record[subattr_val['value_type']] in ['value_string', 'value_boolean', 'value_number', 'value_relation', 'value_object','value_datetime']:
          subattr_dict = get_attr_val_dict[record[subattr_val['value_type']]](record, subattr_val, subattr_dict)
        if record[subattr_val['unit_code']]:
          subattr_dict['unitCode'] = record[subattr_val['unit_code']]
        if record[subattr_val['location']]:
          subattr_dict['location'] = {"type": "GeoProperty", 'value': json.loads(record[subattr_val['location']])}
        index = attrs_list.index(record[attr_val['instance_id']])
        response_data[attr][attrs_index[index]][subattr] = subattr_dict
    status = 1
  except Exception as e:
    app.logger.error("Error: build_normal_response_data_for_entity")
    app.logger.error(traceback.format_exc())
  return response_data, status, error

def build_temporal_response_data_for_entity(record_list, response_data, context, data, app):
  """Building response if options = Temporalvalues"""
  get_attr_val_dict = {'value_string': get_temporal_value_string,  'value_boolean': get_temporal_value_boolean, 'value_number':get_temporal_value_number, 'value_relation': get_temporal_value_relation, 'value_object':get_temporal_value_object,'value_datetime':get_temporal_value_datetime}
  compacted_dict = {}
  status = 0
  error = 'Error in building temporal response data for entity'
  attr_val = {'id': 7, 'value_type': 8, 'sub_property': 9, 'unit_code': 10, 'data_set_id': 11, 'value_string': 13,  'value_boolean': 14, 'value_number':15, 'value_relation': 16, 'value_object':17, 'location':18 ,'createdAt':19, 'modifiedAt':20, 'observedAt':21, 'value_datetime':32}
  subattr_val = {'id': 23, 'value_type': 24, 'value_string': 25,  'value_boolean': 26, 'value_number':27, 'value_relation': 28, 'location':29 , 'value_object':30, 'unit_code': 31, 'value_datetime':33}
  try:
    for record in record_list:
      attr = compact_entity_params(record[attr_val['id']], context, compacted_dict, app)
      if attr not in response_data.keys():
        response_data[attr] = {'type': '', 'value': [], 'unitCode': [], 'location': {'type': 'GeoProperty', 'value': []}}
      elif 'lastN' in data and data['lastN'] and len(response_data[attr]['value']) >= data['lastN']:
        continue
      attr_list = []
      if record[attr_val['value_type']] in ['value_string', 'value_boolean', 'value_number', 'value_relation', 'value_object','value_datetime']:
        attr_list = get_attr_val_dict[record[attr_val['value_type']]](record, attr_val, attr_list, response_data, attr) 
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
      
      if attr_list not in response_data[attr]['value']:
        response_data[attr]['value'].append(attr_list)
        if record[attr_val['unit_code']]:
          response_data[attr]['unitCode'].append(record[attr_val['unit_code']])
        else:
          response_data[attr]['unitCode'].append('')
        if record[attr_val['location']]:
          response_data[attr]['location']['value'].append(json.loads(record[attr_val['location']]))
        else:
          response_data[attr]['location']['value'].append('')
      if record[attr_val['sub_property']] and record[subattr_val['id']]:
        subattr = compact_entity_params(record[subattr_val['id']], context, compacted_dict, app)
        if subattr not in response_data[attr].keys():
          response_data[attr][subattr] = {'type': '', 'value': [], 'unitCode': [], 'location': {'type': 'GeoProperty', 'value': []}}
        if record[subattr_val['value_type']] in ['value_string', 'value_boolean', 'value_number', 'value_relation', 'value_object','value_datetime']:
          subattr_list = get_attr_val_dict[record[subattr_val['value_type']]](record, subattr_val, response_data[attr][subattr]['value'], response_data[attr], subattr)
        if record[subattr_val['unit_code']]:
          response_data[attr][subattr]['unitCode'].append(record[subattr_val['unit_code']])
        if record[subattr_val['location']]:
          response_data[attr][subattr]['location']['value'].append({"type": "GeoProperty", 'value': json.loads(record[subattr_val['location']])})
    status = 1
  except Exception as e:
    app.logger.error("Error: build_temporal_response_data_for_entity")
    app.logger.error(traceback.format_exc())
  return response_data, status, error

def compact_entity_params(attr, context, compacted_dict, app):
  """Apply compaction output"""
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

def build_sql_query_for_entity(data, entity_id, app):
  """Build sql query"""
  statement = start_statement
  params = {}
  status = 0
  error = 'Error in build sql query for entity.'
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
    statement += "%s"%(';')
    status = 1
  except Exception as e:
    app.logger.error("Error: build_sql_query_for_entity")
    app.logger.error(traceback.format_exc())
  return statement, params, status, error

def get_temporal_entity_parameters(args, context, app):
  """Parse params"""
  data = {'timerel': None, 'time': None, 'endtime': None, 'timeproperty': 'observedAt', 'attrs': None, 'lastN': None}
  timepropertyDict = {'modifiedAt':'modified_at', 'observedAt' :'observed_at', 'createdAt':'created_at'}
  status = 0
  error = 'Error in getting temporal entity parameters.'
  try:
    if 'timerel' in args:
      data['timerel'] = args.get('timerel')
      data['time'] = args.get('time')
      data['endtime'] = args.get('endtime', None)
      if 'timeproperty' in args and args['timeproperty'] in timepropertyDict.keys():
        data['timeproperty'] = timepropertyDict[args['timeproperty']]
      else:
        data['timeproperty'] = 'observed_at'
      data['time'] = datetime.datetime.strptime(data['time'], '%Y-%m-%dT%H:%M:%SZ').strftime("%Y-%m-%d %H:%M:%S")
      if data['endtime']:
        data['endtime'] = datetime.datetime.strptime(data['endtime'], '%Y-%m-%dT%H:%M:%SZ').strftime("%Y-%m-%d %H:%M:%S")
    if 'attrs' in args and args.get('attrs'):
      data['attrs'] = args.get('attrs').split(',')
    if 'lastN' in args and args.get('lastN'):
      try:
        data['lastN'] = int(args.get('lastN'))
      except:
        data['lastN'] = None
    if data['timeproperty'] not in ['modified_at', 'observed_at', 'created_at']:
      data['timeproperty'] = 'modified_at'
    if 'options' in args and args.get('options'):
      data['options'] = args.get('options')
    data, status, error = expand_entity_params(data, context, app)
  except Exception as e:
    app.logger.error("Error: get_temporal_entity_parameters")
    app.logger.error(traceback.format_exc())
  return data, status, error

def expand_entity_params(data, context, app):
  """Expand entity params"""
  context_list = []
  status = 0
  error = 'Error in expand entity params.'
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
    status = 1
  except Exception as e:
    app.logger.error("Error: expand_entity_params")
    app.logger.error(traceback.format_exc())
  return data, status, error