import json
import traceback
from resources.postgres import start_statement
from resources.context import default_context
from resources.postgres import *
from resources.records import *
import datetime
import validators
import re
from pyld import jsonld


def build_response_data_for_entities(record, context, data, app):
  """Build response for entities api"""
  response_data = []
  response_dict = {}
  status = 0
  error = ''
  try:
    if 'options' in data and data['options'] == 'temporalValues':
      response_data, status, error = build_temporal_response_data_for_entities(record, response_dict, context, data, app)
    else:
      response_data, status, error = build_normal_response_data_for_entities(record, response_dict, context, data, app)
  except Exception as e:
    app.logger.error("Error: build_response_data_for_entities")
    app.logger.error(traceback.format_exc())
    error = 'Error in building response data for entities'
  return response_data, status, error

def build_normal_response_data_for_entities(record_list, response_dict, context, data, app):
  """Build response if options = None"""
  get_attr_val_dict = {'value_string': get_normal_value_string,  'value_boolean': get_normal_value_boolean, 'value_number':get_normal_value_number, 'value_relation': get_normal_value_relation, 'value_object':get_normal_value_object,'value_datetime':get_normal_value_datetime}
  compacted_dict = {}
  response_data = []
  status = 0
  error = ''
  attrs_list = []
  attrs_index = []
  entity_val = {'id': 0, 'type':1, 'location':2 ,'createdAt':3, 'modifiedAt':4, 'observedAt':5}
  attr_val = {'id': 7, 'value_type': 8, 'sub_property': 9, 'unit_code': 10, 'data_set_id': 11, 'instance_id':12, 'value_string': 13,  'value_boolean': 14, 'value_number':15, 'value_relation': 16, 'value_object':17, 'location':18 ,'createdAt':19, 'modifiedAt':20, 'observedAt':21, 'value_datetime':32 }
  subattr_val = {'id': 23, 'value_type': 24, 'value_string': 25,  'value_boolean': 26, 'value_number':27, 'value_relation': 28, 'location':29 , 'value_object':30, 'unit_code': 31, 'value_datetime':33}
  try:
    for record in record_list:
      if data['idPattern']:
        if not re.search(data['idPattern'], record[entity_val['id']]):
          continue
      response_keys = response_dict.keys()
      if record[entity_val['id']] not in response_keys:
        response_dict[record[entity_val['id']]] = {}
        response_dict[record[entity_val['id']]]['id'] = record[entity_val['id']]
        response_dict[record[entity_val['id']]]['type'] = compact_entities_params(record[entity_val['type']], context, {} ,app)
        if 'options' in data and data['options'] == 'sysAttrs':
          if record[entity_val['createdAt']]:
            response_dict[record[entity_val['id']]]['createdAt'] = record[entity_val['createdAt']].replace(' ','')
          if record[entity_val['modifiedAt']]:
            response_dict[record[entity_val['id']]]['modifiedAt'] = record[entity_val['modifiedAt']].replace(' ','')
          if record[entity_val['observedAt']]:
            response_dict[record[entity_val['id']]]['observedAt'] = record[entity_val['observedAt']].replace(' ','')
        else:
          if record[entity_val['observedAt']]:
            response_dict[record[entity_val['id']]]['observedAt'] = record[entity_val['observedAt']].replace(' ','')
        if record[entity_val['location']]:
          response_dict[record[entity_val['id']]]['location'] = {"type": "GeoProperty", 'value': json.loads(record[entity_val['location']])}
        if context:
          response_dict[record[entity_val['id']]]['@context'] = [context, default_context]
        else:
          response_dict[record[entity_val['id']]]['@context'] = default_context
      elif 'lastN' in data and data['lastN'] and len(response_keys) >= data['lastN']:
        continue
      if record[attr_val['id']] != None:
        attr = compact_entities_params(record[attr_val['id']], context, compacted_dict, app)
        if attr not in response_dict[record[entity_val['id']]].keys():
          response_dict[record[entity_val['id']]][attr] = []
        elif 'lastN' in data and data['lastN'] and len(response_dict[record[entity_val['id']]][attr]) >= data['lastN']:
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
          response_dict[record[entity_val['id']]][attr].append(attr_dict)
          attrs_index.append(len(response_dict[record[entity_val['id']]][attr]) -1)
        if record[attr_val['sub_property']] and record[subattr_val['id']]:
          subattr = compact_entities_params(record[subattr_val['id']], context, compacted_dict, app)
          subattr_dict = {}
          if record[subattr_val['value_type']] in ['value_string', 'value_boolean', 'value_number', 'value_relation', 'value_object','value_datetime']:
            subattr_dict = get_attr_val_dict[record[subattr_val['value_type']]](record, subattr_val, subattr_dict)
          if record[subattr_val['unit_code']]:
            subattr_dict['unitCode'] = record[subattr_val['unit_code']]
          if record[subattr_val['location']]:
            subattr_dict['location'] = {"type": "GeoProperty", 'value': json.loads(record[subattr_val['location']])}
          index = attrs_list.index(record[attr_val['instance_id']])
          response_dict[record[entity_val['id']]][attr][attrs_index[index]][subattr] = subattr_dict
    status = 1
    response_data = list(response_dict.values())
    app.logger.info(len(response_data))
  except Exception as e:
    app.logger.error("Error: build_normal_response_data_for_entities")
    app.logger.error(traceback.format_exc())
    error = 'Error in building normal response data for entities.'
  return response_data, status, error

def build_temporal_response_data_for_entities(record_list, response_dict, context, data, app):
  """Building response if options = Temporalvalues"""
  get_attr_val_dict = {'value_string': get_temporal_value_string,  'value_boolean': get_temporal_value_boolean, 'value_number':get_temporal_value_number, 'value_relation': get_temporal_value_relation, 'value_object':get_temporal_value_object,'value_datetime':get_temporal_value_datetime}
  compacted_dict = {}
  response_data = []
  status = 0
  error = ''
  entity_val = {'id': 0, 'type':1, 'location':2 ,'createdAt':3, 'modifiedAt':4, 'observedAt':5}
  attr_val = {'id': 7, 'value_type': 8, 'sub_property': 9, 'unit_code': 10, 'data_set_id': 11, 'value_string': 13,  'value_boolean': 14, 'value_number':15, 'value_relation': 16, 'value_object':17, 'location':18 ,'createdAt':19, 'modifiedAt':20, 'observedAt':21, 'value_datetime':32}
  subattr_val = {'id': 23, 'value_type': 24, 'value_string': 25,  'value_boolean': 26, 'value_number':27, 'value_relation': 28, 'location':29 , 'value_object':30, 'unit_code': 31, 'value_datetime':33}
  try:
    for record in record_list:
      if data['idPattern']:
        if not re.search(data['idPattern'], record[entity_val['id']]):
          continue
      response_keys = response_dict.keys()
      if record[entity_val['id']] not in response_keys:
        response_dict[record[entity_val['id']]] = {}
        response_dict[record[entity_val['id']]]['id'] = record[entity_val['id']]
        response_dict[record[entity_val['id']]]['type'] = compact_entities_params(record[entity_val['type']], context, {} ,app)
        if 'options' in data and data['options'] == 'sysAttrs':
          if record[entity_val['createdAt']]:
            response_dict[record[entity_val['id']]]['createdAt'] = record[entity_val['createdAt']].replace(' ','')
          if record[entity_val['modifiedAt']]:
            response_dict[record[entity_val['id']]]['modifiedAt'] = record[entity_val['modifiedAt']].replace(' ','')
          if record[entity_val['observedAt']]:
            response_dict[record[entity_val['id']]]['observedAt'] = record[entity_val['observedAt']].replace(' ','')
        else:
          if record[entity_val['observedAt']]:
            response_dict[record[entity_val['id']]]['observedAt'] = record[entity_val['observedAt']].replace(' ','')
        if record[entity_val['location']]:
          response_dict[record[entity_val['id']]]['location'] = {"type": "GeoProperty", 'value': json.loads(record[entity_val['location']])}
        if context:
          response_dict[record[entity_val['id']]]['@context'] = [context, default_context]
        else:
          response_dict[record[entity_val['id']]]['@context'] = default_context
      elif 'lastN' in data and data['lastN'] and len(response_keys) >= data['lastN']:
        continue
      if record[attr_val['id']] != None:
        attr = compact_entities_params(record[attr_val['id']], context, compacted_dict, app)
        if attr not in response_dict[record[entity_val['id']]].keys():
          response_dict[record[entity_val['id']]][attr] = {'type': '', 'value': [], 'unitCode': [], 'location': {'type': 'GeoProperty', 'value': []}}
        elif 'lastN' in data and data['lastN'] and len(response_dict[record[entity_val['id']]][attr]['value']) >= data['lastN']:
          continue
        attr_list = []
        if record[attr_val['value_type']] in ['value_string', 'value_boolean', 'value_number', 'value_relation', 'value_object','value_datetime']:
          attr_list = get_attr_val_dict[record[attr_val['value_type']]](record, attr_val, attr_list, response_dict[record[entity_val['id']]], attr) 
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
        if attr_list not in response_dict[record[entity_val['id']]][attr]['value']:
          response_dict[record[entity_val['id']]][attr]['value'].append(attr_list)
          if record[attr_val['unit_code']]:
            response_dict[record[entity_val['id']]][attr]['unitCode'].append(record[attr_val['unit_code']])
          else:
            response_dict[record[entity_val['id']]][attr]['unitCode'].append('')
          if record[attr_val['location']]:
            response_dict[record[entity_val['id']]][attr]['location']['value'].append(json.loads(record[attr_val['location']]))
          else:
            response_dict[record[entity_val['id']]][attr]['location']['value'].append('')
        if record[attr_val['sub_property']] and record[subattr_val['id']]:
          subattr = compact_entities_params(record[subattr_val['id']], context, compacted_dict, app)
          if subattr not in response_dict[record[entity_val['id']]][attr].keys():
            response_dict[record[entity_val['id']]][attr][subattr] = {'type': '', 'value': [], 'unitCode': [], 'location': {'type': 'GeoProperty', 'value': []}}
          if record[subattr_val['value_type']] in ['value_string', 'value_boolean', 'value_number', 'value_relation', 'value_object','value_datetime']:
            subattr_list = get_attr_val_dict[record[subattr_val['value_type']]](record, subattr_val, response_dict[record[entity_val['id']]][attr][subattr]['value'], response_dict[record[entity_val['id']]][attr], subattr)
          if record[subattr_val['unit_code']]:
            response_dict[record[entity_val['id']]][attr][subattr]['unitCode'].append(record[subattr_val['unit_code']])
          if record[subattr_val['location']]:
            response_dict[record[entity_val['id']]][attr][subattr]['location']['value'].append({"type": "GeoProperty", 'value': json.loads(record[subattr_val['location']])})
    status = 1
    response_data = list(response_dict.values())
  except Exception as e:
    app.logger.error("Error: build_temporal_response_data_for_entities")
    app.logger.error(traceback.format_exc())
    error = 'Error in building temporal response data for entities'
  return response_data, status, error

def compact_entities_params(attr, context, compacted_dict, app):
  """Apply compaction output"""
  context_list = []
  attr_key = attr
  default_context_compact = 'https://uri.etsi.org/ngsi-ld/default-context/'
  try:
    if attr:
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
    app.logger.error("Error: compact_entities_params")
    app.logger.error(traceback.format_exc())
  return attr


def build_sql_query_for_q_with_sub_attribute(statement, params, q_params, count, q_type):
  """Build sql query for q param"""
  attr_value = 'attr_value_' + q_type + str(count)
  sub_value = 'sub_value_' + q_type + str(count)
  operation = 'operation_' + q_type + str(count)
  if q_params['operation'] in ['like', 'not like']:
    q_params['value'] = '%{}%'.format(q_params['value'])
  if q_params['value'] in ['True', 'true', 'TRUE']:
    st = sub_attributes_true_statement.replace('operation', q_params['operation'])
  elif q_params['value'] in ['False', 'false', 'FALSE']:
    st = sub_attributes_false_statement.replace('operation', q_params['operation'])
  elif 'column' in q_params:
    col_value = 'col_value_' + q_type + str(count)
    opr_value = 'opr_value_' + q_type + str(count)
    st = sub_attributes_object_statement.replace('col_value',col_value).replace('opr_value',opr_value).replace('operation', q_params['operation'])
    params[col_value] = '%{}%'.format(q_params['column'])
    params[opr_value] = '%{}%'.format(q_params['value'])
  else:
    datetime_flag = 0
    number_flag = 0
    datetime_val = ''
    opr_value = 'opr_value_' + q_type + str(count)
    try:
      datetime_val = datetime.datetime.strptime(q_params['value'], '%Y-%m-%dT%H:%M:%SZ').strftime("%Y-%m-%d %H:%M:%S")
      datetime_flag = 1
    except:
      pass
    try:
      int(q_params['value'])
      number_flag = 1
    except:
      pass
    if datetime_val and datetime_flag:
      st = sub_attributes_datetime_statement.replace('opr_value',opr_value).replace('operation', q_params['operation'])
      params[opr_value] = datetime_val
    elif number_flag:
      st = sub_attributes_number_statement.replace('opr_value',opr_value).replace('operation', q_params['operation'])
      params[opr_value] = q_params['value']
    else:
      st = sub_attributes_statement.replace('opr_value',opr_value).replace('operation', q_params['operation'])
      params[opr_value] = q_params['value']
  st = st.replace('attr_value', attr_value).replace('sub_value', sub_value)
  params[attr_value] = q_params['attribute']
  params[sub_value] = q_params['sub-attribute']
  if count == 0:
    statement +=  st
  else:
    statement += ' OR ' + st   
  return statement, params

def build_sql_query_for_q_with_attribute(statement, params, q_params, count, q_type):
  """Build sql query for q param"""
  attr_value = 'attr_value_' + q_type + str(count)
  operation = 'operation_' + q_type + str(count)
  if q_params['operation'] in ['like', 'not like']:
    q_params['value'] = '%{}%'.format(q_params['value'])
  if q_params['value'] in ['True', 'true', 'TRUE']:
    st = attributes_true_statement.replace('operation', q_params['operation'])
  elif q_params['value'] in ['False', 'false', 'FALSE']:
    st = attributes_false_statement.replace('operation', q_params['operation'])
  elif 'column' in q_params:
    col_value = 'col_value_' + q_type + str(count)
    opr_value = 'opr_value_' + q_type + str(count)
    st = attributes_object_statement.replace('col_value',col_value).replace('opr_value',opr_value).replace('operation', q_params['operation'])
    params[col_value] = '%{}%'.format(q_params['column'])
    params[opr_value] = '%{}%'.format(q_params['value'])
  else:
    datetime_flag = 0
    number_flag = 0
    datetime_val = ''
    opr_value = 'opr_value_' + q_type + str(count)
    try:
      datetime_val = datetime.datetime.strptime(q_params['value'], '%Y-%m-%dT%H:%M:%SZ').strftime("%Y-%m-%d %H:%M:%S")
      datetime_flag = 1
    except:
      pass
    try:
      int(q_params['value'])
      number_flag = 1
    except:
      pass
    if datetime_val and datetime_flag:
      st = attributes_datetime_statement.replace('opr_value',opr_value).replace('operation', q_params['operation'])
      params[opr_value] = datetime_val
    elif number_flag:
      st = attributes_number_statement.replace('opr_value',opr_value).replace('operation', q_params['operation'])
      params[opr_value] = q_params['value']
    else:
      st = attributes_statement.replace('opr_value',opr_value).replace('operation', q_params['operation'])
      params[opr_value] = q_params['value']
  st = st.replace('attr_value', attr_value)
  params[attr_value] = q_params['attribute']
  if count == 0:
    statement += st
  else:
    statement += ' OR ' + st   
  return statement, params


def build_sql_query_for_q_for_dict_for_others(statement, params, q_params, count, q_type):
  """Build sql query for q param"""
  if q_params['operation'] == '==':
    q_params['operation'] = '='
  elif q_params['operation'] == '~=':
    q_params['operation'] = 'like'
  elif q_params['operation'] == '!~=':
    q_params['operation'] = 'not like'
  if 'sub-attribute' in q_params:
    statement, params = build_sql_query_for_q_with_sub_attribute(statement, params, q_params, count, q_type)
  else:
    statement, params = build_sql_query_for_q_with_attribute(statement, params, q_params, count, q_type)
  return statement, params

def build_sql_query_for_q_for_dict_for_range(statement, params, q_params, count, q_type):
  """Build sql query for q param"""
  attr_value = 'attr_value_' + q_type + str(count)
  low_range_value = 'low_range_value_' + q_type + str(count)
  high_range_value = 'high_range_value' + q_type + str(count)
  params[low_range_value] = q_params['value'][0]
  params[high_range_value] = q_params['value'][1]
  params[attr_value] = q_params['attribute']
  if 'sub-attribute' in q_params:
    sub_value = 'sub_value_' + q_type + str(count)
    st = sub_attributes_range_statement.replace('low_range_value', low_range_value).replace('high_range_value',high_range_value).replace('sub_value', sub_value)
    params[sub_value] = q_params['sub-attribute']
  else:
    st = attributes_range_statement.replace('low_range_value', low_range_value).replace('high_range_value',high_range_value)
  st = st.replace('attr_value', attr_value)
  if count == 0:
    statement +=  st
  else:
    statement += ' OR ' + st   
  return statement, params

def build_sql_query_for_q_for_dict_for_having(statement, params, q_params, count, q_type):
  """Build sql query for q param"""
  attr_value = 'attr_value_' + q_type + str(count)
  params[attr_value] = q_params['attribute']
  if 'sub-attribute' in q_params:
    sub_value = 'sub_value_' + q_type + str(count)
    st = sub_attributes_having_statement.replace('sub_value', sub_value)
    params[sub_value] = q_params['sub-attribute']
  else:
    st = attributes_having_statement
  st = st.replace('attr_value', attr_value)
  if count == 0:
    statement += st
  else:
    statement += ' OR ' + st      
  return statement, params

def build_sql_query_for_q_for_dict(statement, params, q_params, count, q_type, app):
  """Build sql query for q param"""
  status = 0
  error = ''
  try:
    if q_params['operation'] == 'having':
      statement, params = build_sql_query_for_q_for_dict_for_having(statement, params, q_params, count, q_type)
    elif q_params['operation'] == 'range':
      statement, params = build_sql_query_for_q_for_dict_for_range(statement, params, q_params, count, q_type)
    else:
      statement, params = build_sql_query_for_q_for_dict_for_others(statement, params, q_params, count, q_type)
    status = 1
  except Exception as e:
    app.logger.error("Error: build_sql_query_for_q_for_dict")
    app.logger.error(traceback.format_exc())
    error = 'Error in building sql query for entities for q param.'
  return statement, params, status, error

def build_sql_query_for_q_list_with_sub_attribute(statement, params, q_params, count, q_type):
  """Build sql query for q param"""
  attr_value = 'attr_value_' + q_type + str(count)
  sub_value = 'sub_value_' + q_type + str(count)
  operation = 'operation_' + q_type + str(count)
  if q_params['operation'] in ['like', 'not like']:
    q_params['value'] = '%{}%'.format(q_params['value'])
  if q_params['value'] in ['True', 'true', 'TRUE']:
    st = sub_attributes_true_statement.replace('operation', q_params['operation'])
  elif q_params['value'] in ['False', 'false', 'FALSE']:
    st = sub_attributes_false_statement.replace('operation', q_params['operation'])
  elif 'column' in q_params:
    col_value = 'col_value_' + q_type + str(count)
    opr_value = 'opr_value_' + q_type + str(count)
    st = sub_attributes_object_statement.replace('col_value',col_value).replace('opr_value',opr_value).replace('operation', q_params['operation'])
    params[col_value] = '%{}%'.format(q_params['column'])
    params[opr_value] = '%{}%'.format(q_params['value'])
  else:
    datetime_flag = 0
    number_flag = 0
    datetime_val = ''
    opr_value = 'opr_value_' + q_type + str(count)
    try:
      datetime_val = datetime.datetime.strptime(q_params['value'], '%Y-%m-%dT%H:%M:%SZ').strftime("%Y-%m-%d %H:%M:%S")
      datetime_flag = 1
    except:
      pass
    try:
      int(q_params['value'])
      number_flag = 1
    except:
      pass
    if datetime_val and datetime_flag:
      st = sub_attributes_datetime_statement.replace('opr_value',opr_value).replace('operation', q_params['operation'])
      params[opr_value] = datetime_val
    elif number_flag:
      st = sub_attributes_number_statement.replace('opr_value',opr_value).replace('operation', q_params['operation'])
      params[opr_value] = q_params['value']
    else:
      st = sub_attributes_statement.replace('opr_value',opr_value).replace('operation', q_params['operation'])
      params[opr_value] = q_params['value']
  st = st.replace('attr_value', attr_value).replace('sub_value', sub_value)
  params[attr_value] = q_params['attribute']
  params[sub_value] = q_params['sub-attribute']
  statement += ' ' + st   
  return statement, params

def build_sql_query_for_q_list_with_attribute(statement, params, q_params, count, q_type):
  """Build sql query for q param"""
  attr_value = 'attr_value_' + q_type + str(count)
  operation = 'operation_' + q_type + str(count)
  if q_params['operation'] in ['like', 'not like']:
    q_params['value'] = '%{}%'.format(q_params['value'])
  if q_params['value'] in ['True', 'true', 'TRUE']:
    st = attributes_true_statement.replace('operation', q_params['operation'])
  elif q_params['value'] in ['False', 'false', 'FALSE']:
    st = attributes_false_statement.replace('operation', q_params['operation'])
  elif 'column' in q_params:
    col_value = 'col_value_' + q_type + str(count)
    opr_value = 'opr_value_' + q_type + str(count)
    st = attributes_object_statement.replace('col_value',col_value).replace('opr_value',opr_value).replace('operation', q_params['operation'])
    params[col_value] = '%{}%'.format(q_params['column'])
    params[opr_value] = '%{}%'.format(q_params['value'])
  else:
    datetime_flag = 0
    number_flag = 0
    datetime_val = ''
    opr_value = 'opr_value_' + q_type + str(count)
    try:
      datetime_val = datetime.datetime.strptime(q_params['value'], '%Y-%m-%dT%H:%M:%SZ').strftime("%Y-%m-%d %H:%M:%S")
      datetime_flag = 1
    except:
      pass
    try:
      int(q_params['value'])
      number_flag = 1
    except:
      pass
    if datetime_val and datetime_flag:
      st = attributes_datetime_statement.replace('opr_value',opr_value).replace('operation', q_params['operation'])
      params[opr_value] = datetime_val
    elif number_flag:
      st = attributes_number_statement.replace('opr_value',opr_value).replace('operation', q_params['operation'])
      params[opr_value] = q_params['value']
    else:
      st = attributes_statement.replace('opr_value',opr_value).replace('operation', q_params['operation'])
      params[opr_value] = q_params['value']
  st = st.replace('attr_value', attr_value)
  params[attr_value] = q_params['attribute']
  statement += ' ' + st   
  return statement, params


def build_sql_query_for_q_for_list_for_others(statement, params, q_params, count, q_type):
  """Build sql query for q param"""
  if q_params['operation'] == '==':
    q_params['operation'] = '='
  elif q_params['operation'] == '~=':
    q_params['operation'] = 'like'
  elif q_params['operation'] == '!~=':
    q_params['operation'] = 'not like'
  if 'sub-attribute' in q_params:
    statement, params = build_sql_query_for_q_list_with_sub_attribute(statement, params, q_params, count, q_type)
  else:
    statement, params = build_sql_query_for_q_list_with_attribute(statement, params, q_params, count, q_type)
  return statement, params

def build_sql_query_for_q_for_list_for_range(statement, params, q_params, count, q_type):
  """Build sql query for q param"""
  attr_value = 'attr_value_' + q_type + str(count)
  low_range_value = 'low_range_value_' + q_type + str(count)
  high_range_value = 'high_range_value' + q_type + str(count)
  params[low_range_value] = q_params['value'][0]
  params[high_range_value] = q_params['value'][1]
  params[attr_value] = q_params['attribute']
  if 'sub-attribute' in q_params:
    sub_value = 'sub_value_' + q_type + str(count)
    st = sub_attributes_range_statement.replace('low_range_value', low_range_value).replace('high_range_value',high_range_value).replace('sub_value', sub_value)
    params[sub_value] = q_params['sub-attribute']
  else:
    st = attributes_range_statement.replace('low_range_value', low_range_value).replace('high_range_value',high_range_value)
  st = st.replace('attr_value', attr_value)
  statement += ' ' + st   
  return statement, params

def build_sql_query_for_q_for_list_for_having(statement, params, q_params, count, q_type):
  """Build sql query for q param"""
  attr_value = 'attr_value_' + q_type + str(count)
  params[attr_value] = q_params['attribute']
  if 'sub-attribute' in q_params:
    sub_value = 'sub_value_' + q_type + str(count)
    st = sub_attributes_having_statement.replace('sub_value', sub_value)
    params[sub_value] = q_params['sub-attribute']
  else:
    st = attributes_having_statement
  st = st.replace('attr_value', attr_value)
  statement += ' ' + st      
  return statement, params

def build_sql_query_for_q_for_list(statement, params, q_params, count, q_type, app):
  """Build sql query for q param"""
  status = 0
  error = ''
  try:
    if q_params['operation'] == 'having':
      statement, params = build_sql_query_for_q_for_list_for_having(statement, params, q_params, count, q_type)
    elif q_params['operation'] == 'range':
      statement, params = build_sql_query_for_q_for_list_for_range(statement, params, q_params, count, q_type)
    else:
      statement, params = build_sql_query_for_q_for_list_for_others(statement, params, q_params, count, q_type)
    status = 1
  except Exception as e:
    app.logger.error("Error: build_sql_query_for_q_for_list")
    app.logger.error(traceback.format_exc())
    error = 'Error in building sql query for entities for q param.'
  return statement, params, status, error


def build_sql_query_for_q(statement, params, q_params, data, attributes, app):
  """Build sql query for q param"""
  status = 0
  error = ''
  q_dict = 'q_dict'
  q_list = 'q_list'
  attr_statement = attributes_having_statement
  sub_statement = sub_attributes_having_statement
  sub_attribute_flag = 0 
  try:
    q_len = len(q_params)
    for count in range(0, q_len):
      if type(q_params[count]) is dict:
        if count == 0:
          statement += ' AND ('
        statement, params, status, error = build_sql_query_for_q_for_dict(statement, params, q_params[count], count, q_dict, app)
        attributes.append(q_params[count]['attribute'])
        if not status:
          return statement, params, attributes, status, error
      else:
        if count == 0:
          statement += ' AND (('
        else:
          statement += ' OR ('
        for index in range(0, len(q_params[count])):
          ids = '%s_%s' %(count,index)
          if type(q_params[count][index]) is dict:
            statement, params, status, error = build_sql_query_for_q_for_list(statement, params, q_params[count][index], ids, q_list, app)
            attributes.append(q_params[count][index]['attribute'])
            if not status:
              return statement, params, attributes, status, error
          else:
            statement += ' ' + q_params[count][index] + ' '
        statement += ' )'
    if data['geoproperty'] != 'geo_property':
      statement, params, attributes, status, error = build_sql_query_for_geoproperty_for_attributes(statement, params, data, attributes, app)
    statement += ' )'
    status = 1
  except Exception as e:
    app.logger.error("Error: build_sql_query_for_q")
    app.logger.error(traceback.format_exc())
    error = 'Error in building sql query for entities for q param.'
  return statement, params, attributes, status, error

def build_sql_query_for_geoproperty_for_attributes(statement, params, data, attributes, app):
  """Build sql query for geoproperty"""
  status = 0
  error = ''
  geo_property_dict = {'near_maxDistance': attrubutes_geo_dwithin, 'near_minDistance': attrubutes_geo_not_dwithin, 'within': attrubutes_geo_within, 'contains': attrubutes_geo_contains, 'intersects': attrubutes_geo_intersects, 'equals': attrubutes_geo_equals, 'disjoint': attrubutes_geo_disjoint, 'overlaps': attrubutes_geo_overlaps}
  try:
    statement += ' OR ' + geo_property_dict[data['georel']]
    if data['georel'] in ['near_maxDistance', 'near_minDistance']:
      params["geo_distance"] = data['near_distance']
    params['geo_property'] = str({"type": data['geometry'], "coordinates": data['coordinates']})
    params['geo_attr_value'] = data['geoproperty']
    attributes.append(data['geoproperty'])
    status = 1
  except Exception as e:
    app.logger.error("Error: build_sql_query_for_geoproperty_for_attributes")
    app.logger.error(traceback.format_exc())
    error = 'Error in building sql query for geoproperty.'
  return statement, params, attributes, status, error

def build_sql_query_for_geoproperty_for_entity(statement, params, data, app):
  """Build sql query for geoproperty"""
  status = 0
  error = ''
  geo_property_dict = {'near_maxDistance': entity_geo_dwithin, 'near_minDistance': entity_geo_not_dwithin, 'within': entity_geo_within, 'contains': entity_geo_contains, 'intersects': entity_geo_intersects, 'equals': entity_geo_equals, 'disjoint': entity_geo_disjoint, 'overlaps': entity_geo_overlaps}
  try:
    statement += ' AND ' + geo_property_dict[data['georel']]
    if data['georel'] in ['near_maxDistance', 'near_minDistance']:
      params["geo_distance"] = data['near_distance']
    params['geo_property'] = str({"type": data['geometry'], "coordinates": data['coordinates']})
    status = 1
  except Exception as e:
    app.logger.error("Error: build_sql_query_for_geoproperty_for_entity")
    app.logger.error(traceback.format_exc())
    error = 'Error in building sql query for geoproperty.'
  return statement, params, status, error


def get_entities_ids_from_records(data, records, attributes):
  """Get entities ids from records"""
  entity_ids = []
  records_dict = {}
  records_data = []
  attr_val = {'entity_id': 0, 'attr_id': 7}
  if data['attrs'] and len(data['attrs']) > 0:
    for record in records:
      if record[attr_val['entity_id']] not in records_dict.keys():
        records_dict[record[attr_val['entity_id']]] = {'value': [], 'attrs': {record[attr_val['attr_id']]}}
      else:
        records_dict[record[attr_val['entity_id']]]['attrs'].add(record[attr_val['attr_id']])
      if record[attr_val['attr_id']] in data['attrs']: 
        records_dict[record[attr_val['entity_id']]]['value'].append(record)
  else:
    for record in records:
      if record[attr_val['entity_id']] not in records_dict.keys():
        records_dict[record[attr_val['entity_id']]] = {'value': [record], 'attrs': {record[attr_val['attr_id']]}}
      else:
        records_dict[record[attr_val['entity_id']]]['value'].append(record)
        records_dict[record[attr_val['entity_id']]]['attrs'].add(record[attr_val['attr_id']])
  attributes = list(set(attributes))
  attrs_len = len(attributes)
  if data['id_data'] and len(data['id_data']) > 0:
    for key, value in records_dict.items():
      if len(value['attrs']) == attrs_len and key in data['id_data']:
        records_data.extend(value['value'])
        entity_ids.append(key)
  else:
    for key, value in records_dict.items():
      if len(value['attrs']) == attrs_len:
        records_data.extend(value['value'])
        entity_ids.append(key)
  return records_data, attributes, entity_ids


def build_sql_query_for_entities_after_attributes(data, records, attributes, app):
  """Build sql query"""
  status = 0
  error = ''
  try:
    records_data, attributes, entity_ids = get_entities_ids_from_records(data, records, attributes)
    statement, params, status, error = sql_query_for_entities(data, app)
    if not status:
      return statement, params, records_data, status, error
    if data['type_data'] and len(data['type_data']) > 0:
      statement += ' AND entity_table.entity_type in ('
      for index in range(0,len(data['type_data'])):
        if index == (len(data['type_data']) -1):
          statement += '%(type_data'+str(index)+')s'
        else:
          statement += '%(type_data'+str(index)+')s,'
        params['type_data'+str(index)] = data['type_data'][index]
      statement += ')'
    if data['attrs'] and len(data['attrs']) > 0:
      statement += ' AND attributes_table.id in ('
      for index in range(0,len(data['attrs'])):
        if index == (len(data['attrs']) -1):
          statement += '%(attrs'+str(index)+')s'
        else:
          statement += '%(attrs'+str(index)+')s,'
        params['attrs'+str(index)] = data['attrs'][index]
      statement += ')'
    statement += ' AND attributes_table.entity_id in ('
    for index in range(0,len(entity_ids)):
      if index == (len(entity_ids) -1):
        statement += '%(id_data'+str(index)+')s'
      else:
        statement += '%(id_data'+str(index)+')s,'
      params['id_data'+str(index)] = entity_ids[index]
    statement += ')'
    statement += ' AND attributes_table.id NOT in ('
    for index in range(0,len(attributes)):
      if index == (len(attributes) -1):
        statement += '%(attrs_not'+str(index)+')s'
      else:
        statement += '%(attrs_not'+str(index)+')s,'
      params['attrs_not'+str(index)] = attributes[index]
    statement += ')'
    statement +=" order by entity_table."+ data['timeproperty']+" desc" 
    statement += "%s"%(";")
    status = 1
  except Exception as e:
    app.logger.error("Error: build_sql_query_for_entities_after_attributes")
    app.logger.error(traceback.format_exc())
    error = 'Error in building sql query for entities.'
  return statement, params, records_data, status, error


def build_sql_query_for_entities_with_attributes(data, cursor, run_sql, app):
  """Build sql query"""
  status = 0
  run_sql = 0
  error = ''
  records_data = []
  attributes = []
  try:
    statement, params, status, error = sql_query_for_entities(data, app)
    if not status:
      return statement, params, records_data, run_sql, status, error
    if data['type_data'] and len(data['type_data']) > 0:
      statement += ' AND entity_table.entity_type in ('
      for index in range(0,len(data['type_data'])):
        if index == (len(data['type_data']) -1):
          statement += '%(type_data'+str(index)+')s'
        else:
          statement += '%(type_data'+str(index)+')s,'
        params['type_data'+str(index)] = data['type_data'][index]
      statement += ')'
    if data['q']:
      statement, params, attributes, status, error = build_sql_query_for_q(statement, params, data['q'], data, attributes, app)
      if not status:
        return statement, params, records, run_sql, status, error
    statement +=" order by entity_table."+ data['timeproperty']+" desc" 
    statement += "%s"%(";")
    cursor.execute(statement, params)
    records = cursor.fetchall()
    if len(records) == 0:
      status = 1
      return statement, params, records_data, run_sql, status, error
    else:
      run_sql = 1
      statement, params, records_data, status, error = build_sql_query_for_entities_after_attributes(data, records, attributes, app)
    status = 1
  except Exception as e:
    app.logger.error("Error: build_sql_query_for_entities_with_attributes")
    app.logger.error(traceback.format_exc())
    error = 'Error in building sql query for entities.'
  return statement, params, records_data, run_sql, status, error

def sql_query_for_entities(data, app):
  """Build sql query"""
  statement = start_statement
  params = {}
  status = 0
  error = ''
  try:
    if data['timerel'] == 'after':
      statement += " WHERE entity_table."+ data['timeproperty']+">%(time)s"
      params["time"] = data['time']
    elif data['timerel'] == 'before':
      statement += " WHERE entity_table."+ data['timeproperty']+"<%(time)s"
      params["time"] = data['time']
    else:
      statement += " WHERE entity_table."+ data['timeproperty']+">=%(time)s AND entity_table."+ data['timeproperty']+"<%(endtime)s"
      params["time"] = data['time']
      params["endtime"] = data['endtime']
    if data['coordinates'] and data['geoproperty'] == 'geo_property':
      statement, params,status, error = build_sql_query_for_geoproperty_for_entity(statement, params, data, app)
      if not status:
        return statement, params,status, error
    status = 1
  except Exception as e:
    app.logger.error("Error: build_sql_query_for_entities_without_attributes")
    app.logger.error(traceback.format_exc())
    error = 'Error in building sql query for entities without attributes.'
  return statement, params, status, error

def build_sql_query_for_entities_without_attributes(data, cursor, app):
  """Build sql query"""
  status = 0
  error = ''
  try:
    statement, params, status, error = sql_query_for_entities(data, app)
    if not status:
      return statement, params,status, error
    if data['id_data'] and len(data['id_data']) > 0:
      statement += ' AND attributes_table.entity_id in ('
      for index in range(0,len(data['id_data'])):
        if index == (len(data['id_data']) -1):
          statement += '%(id_data'+str(index)+')s'
        else:
          statement += '%(id_data'+str(index)+')s,'
        params['id_data'+str(index)] = data['id_data'][index]
      statement += ')'
    if data['type_data'] and len(data['type_data']) > 0:
      statement += ' AND entity_table.entity_type in ('
      for index in range(0,len(data['type_data'])):
        if index == (len(data['type_data']) -1):
          statement += '%(type_data'+str(index)+')s'
        else:
          statement += '%(type_data'+str(index)+')s,'
        params['type_data'+str(index)] = data['type_data'][index]
      statement += ')'
    if data['attrs'] and len(data['attrs']) > 0:
      statement += ' AND attributes_table.id in ('
      for index in range(0,len(data['attrs'])):
        if index == (len(data['attrs']) -1):
          statement += '%(attrs'+str(index)+')s'
        else:
          statement += '%(attrs'+str(index)+')s,'
        params['attrs'+str(index)] = data['attrs'][index]
      statement += ')'
    statement +=" order by entity_table."+ data['timeproperty']+" desc" 
    statement += "%s"%(";")
    status = 1
  except Exception as e:
    app.logger.error("Error: build_sql_query_for_entities_without_attributes")
    app.logger.error(traceback.format_exc())
    error = 'Error in building sql query for entities without attributes.'
  return statement, params, status, error

def build_sql_query_for_entities(data, cursor, app):
  """Build sql query"""
  statement = start_statement
  params = {}
  attributes = []
  status = 0
  error = ''
  run_sql = 1
  records = []
  try:
    if (data['q']) or (data['coordinates'] and  data['geoproperty'] != 'geo_property'):
      statement, params, records, run_sql, status, error = build_sql_query_for_entities_with_attributes(data, cursor, run_sql, app)
    else:
      statement, params, status, error = build_sql_query_for_entities_without_attributes(data, cursor, app)
  except Exception as e:
    app.logger.error("Error: build_sql_query_for_entities")
    app.logger.error(traceback.format_exc())
    error = 'Error in building sql query for entities.'
  return statement, params, records, run_sql, status, error

def parse_geo_properties(data, args, app):
  """Parse geo params"""
  status = 0
  error = ''
  try:
    if 'georel' in args and args.get('georel'):
      georel = args.get('georel')
      if 'near' in georel:
        data['georel'] = 'near'
        if 'maxDistance' in georel:
          data['georel'] += '_maxDistance'
        elif 'minDistance' in georel:
          data['georel'] += '_minDistance'
        else:
          error = 'Error in parsing geo params: maxDistance or minDistance'
          return data, status, error
        data['near_distance'] = georel.split('==')[1]
      else:
        data['georel'] = georel
    if 'coordinates' in args and args.get('coordinates'):
      data['coordinates'] = eval(args.get('coordinates'))
    if 'geometry' in args and args.get('geometry'):
      data['geometry'] = args.get('geometry')
    if (data['georel'] != None and (data['geometry'] == None or data['coordinates'] == None)) or (data['geometry'] != None and (data['georel'] == None or data['coordinates'] == None)) or (data['coordinates'] != None and (data['geometry'] == None or data['georel'] == None)):
      error = 'Error in parsing geo params: params not correct'
      return data, status, error
    else:
      if 'geoproperty' in args and args.get('geoproperty'):
        data['geoproperty'] = args.get('geoproperty')
        if data['geoproperty'] == 'location':
          data['geoproperty'] = 'geo_property'
      else:
        data['geoproperty'] = 'geo_property'
    status = 1
  except Exception as e:
    app.logger.error("Error: parse_geo_properties")
    app.logger.error(traceback.format_exc())
    error = 'Error in parsing geo params'
  return data, status, error

def get_temporal_entities_parameters(args, context, app):
  """Parse params"""
  data = {'timerel': None, 'time': None, 'endtime': None, 'timeproperty': 'observedAt', 'attrs': None, 'lastN': None, 'id_data': '', 'type_data': '','idPattern': None, 'q':None, 'csf':None, 'georel': None, 'geometry': None, 'coordinates': None, 'geoproperty': None}
  timepropertyDict = {'modifiedAt':'modified_at', 'observedAt' :'observed_at', 'createdAt':'created_at'}
  status = 0
  error = ''
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
    if 'idPattern' in args and args.get('idPattern'):
      data['idPattern'] = args.get('idPattern')
    if 'q' in args and args.get('q'):
      data['q'] = args.get('q')
      data, status, error = get_q_params(data, app)
      if not status:
        return data, status, error
    if 'csf' in args and args.get('csf'):
      data['csf'] = args.get('csf')
    data, status, error = parse_geo_properties(data, args, app)
    if not status:
      return data, status, error
    if 'id' in args:
      ids = args.get('id')
      if ids:
        data['id_data'] = ids.split(',') 
    if 'type' in args:
      types = args.get('type')
      if types:
        data['type_data'] = types.split(',')
    if 'options' in args and args.get('options'):
      data['options'] = args.get('options')
    data, status, error = expand_entities_params(data, context, app)
  except Exception as e:
    app.logger.error("Error: get_temporal_entities_parameters")
    app.logger.error(traceback.format_exc())
    error = 'Error in getting temporal entities parameters.'
  return data, status, error

def get_q_params_in_list(start, count, param, q, app):
  """Covert q params to list"""
  status = 0
  error = ''
  try:
    end = count
    if end != 0 and start != (end -1):
      dt, status, error = parse_q_single(param[start:end], app)
      if not status:
        return q, status, error
      q.append(dt)
    start = count + 1
    end = 0
  except Exception as e:
    app.logger.error("Error: get_q_params_in_list")
    app.logger.error(traceback.format_exc())
    error = 'Error in get q params in list'
  return start, end, status, error

def parse_q_multiple(param, app):
  """Parse q params"""
  status = 0
  q = []
  q_len = len(param)
  count = 0
  start = 0
  end = 0
  try:
    while True:
      if param[count] == '(':
        q.append(param[count])
        start = count + 1
      elif param[count] == ')':
        start, end, status, error = get_q_params_in_list(start, count, param, q, app)
        start = count
        q.append(param[count])
      elif param[count] in ['|',';']:
        start, end, status, error = get_q_params_in_list(start, count, param, q, app)
        q.append('OR')
      elif count == (q_len -1):
        end = count + 1
        if end != 0:
          dt, status, error = parse_q_single(param[start:end], app)
          if not status:
            return q, status, error
          q.append(dt)
        start = count + 1
      count += 1
      if count == q_len:
        break
    status = 1
  except Exception as e:
    app.logger.error("Error: parse_q_multiple")
    app.logger.error(traceback.format_exc())
    error = 'Error in parsing q miltiple params'
  return q, status, error

def parse_q_single(param, app):
  """Parse q params"""
  op_list = ['==', '!=', '>=','<=','>', '<','!~=','~=']
  status = 0
  error = ''
  q = {}
  try:
    flag = 0
    for op in op_list:
      if op in param:
        flag = 1
        param = param.split(op)
        if '.' in param[0]:
          attrs = param[0].split('.')
          if '..' in param[1]:
            param[1] = param[1].split('..')
            q = {'attribute': attrs[0], 'operation': 'range', 'value': param[1], 'sub-attribute': attrs[1]}
          else:
            q = {'attribute': attrs[0], 'operation': op, 'value': param[1], 'sub-attribute': attrs[1]}
        elif '[' in param[0] and ']' in param[0]:
          attrs = param[0].replace(']', '').split('[')
          q = {'attribute': attrs[0], 'operation': op, 'value': param[1], 'column': attrs[1]}
        else:
          if '..' in param[1]:
            param[1] = param[1].split('..')
            q = {'attribute': param[0], 'operation': 'range', 'value': param[1]}
          else:
            q = {'attribute': param[0], 'operation': op, 'value': param[1]}
        break
    if flag == 0:
      if '..' in param:
        param = param.split('=')
        param[1] = param[1].split('..')
        if '.' in param[0]:
          attrs = param[0].split('.')
          q = {'attribute': attrs[0], 'operation': 'range', 'value': param[1], 'sub-attribute': attrs[1]}
        else:
          q = {'attribute': param[0], 'operation': 'range', 'value': param[1]}
      else:
        if '.' in param:
          attrs = param.split('.')
          q = {'attribute': attrs[0], 'operation': 'having', 'value': param, 'sub-attribute': attrs[1]}
        else:
          q = {'attribute': param, 'operation': 'having', 'value': param}
    status = 1
  except Exception as e:
    app.logger.error("Error: parse_q_single")
    app.logger.error(traceback.format_exc())
    error = 'Error in parsing q single params.'
  return q, status, error

def get_q_params(data, app):
  """Parse q params"""
  op_list = ['==', '!=', '>=','<=','>', '<','!~=','~=']
  status = 0
  error = ''
  try:
    if '(' in data['q']:
      params, status, error = split_q_params(data['q'].replace(' ', ''), app)
      if not status:
        return data, status, error
    else:
      params = data['q'].replace(' ', '').split(';')
    q = []
    for param in params:
      if '(' in param or '|' in param:
        dt, status, error = parse_q_multiple(param, app)
        if not status:
          return data, status, error
        q.append(dt)
      else: 
        dt, status, error = parse_q_single(param, app)
        if not status:
          return data, status, error
        q.append(dt)
    data['q'] = q
    status = 1
  except Exception as e:
    app.logger.error("Error: get_q_params")
    app.logger.error(traceback.format_exc())
    error = 'Error in parsing q params'
  return data, status, error

def split_q_params(q_params, app):
  """Parse q params"""
  q = []
  status = 0
  error = ''
  try:
    q_params = q_params.replace(' ','')
    start = 0
    end = None
    count = 0
    countend = 0
    q_len = len(q_params)
    flagend = 0
    for ct in range(0,q_len):
      flag = 0
      if flagend:
        flagend = 0
        continue
      if '(' == q_params[ct]:
        if count == 0:
          start = ct
        count += 1
      if ')' == q_params[ct]:
        countend += 1
      if count != 0 and countend == count and ((ct != (q_len -1) and q_params[ct+1] == ';') or ct == (q_len -1)):
        end = ct
        q.append(q_params[start:end+1])
        flag = 1
        count = countend = 0
        if ct != (q_len -1) and q_params[ct+1] == ';':
          start = ct + 2
          flagend = 1
      if flag == 0 and count == 0 and ';' == q_params[ct]:
        end = ct
        q.append(q_params[start:end])
        flag = 1
        start = ct + 1
      if flag == 0 and ct == (q_len -1):
        end = ct + 1
        q.append(q_params[start:end])
        flag = 1
    status = 1
  except Exception as e:
    app.logger.error("Error: split_q_params")
    app.logger.error(traceback.format_exc())
    error = 'Error in split q params'
  return q, status, error


def expand_entities_params(data, context, app):
  """Expand entities params"""
  context_list = []
  status = 0
  error = ''
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
    if data['geoproperty'] and data['geoproperty'] !='geo_property':
      com = {"@context": context_list, data['geoproperty']: data['geoproperty']}
      expanded = jsonld.expand(com)
      data['geoproperty'] = list(expanded[0].keys())[0]
    if data['type_data']:
      for count in range(0, len(data['type_data'])):
        if not validators.url(data['type_data'][count]):
          com = {"@context": context_list, "@type": data['type_data'][count]}
          expanded = jsonld.expand(com)
          data['type_data'][count] = expanded[0]['@type'][0]
    if data['q']:
      for count in range(0, len(data['q'])):
        if type(data['q'][count]) is dict: 
          if (not validators.url(data['q'][count]['attribute'])):
            com = {"@context": context_list, data['q'][count]['attribute']: data['q'][count]['attribute']} 
            expanded = jsonld.expand(com)
            data['q'][count]['attribute'] = list(expanded[0].keys())[0]
            if 'sub-attribute' in data['q'][count]:
              com = {"@context": context_list, data['q'][count]['sub-attribute']: data['q'][count]['sub-attribute']} 
              expanded = jsonld.expand(com)
              data['q'][count]['sub-attribute'] = list(expanded[0].keys())[0]
        elif type(data['q'][count]) is list:
          for ct in range(0, len(data['q'][count])):
            if type(data['q'][count][ct]) is dict: 
              if (not validators.url(data['q'][count][ct]['attribute'])):
                com = {"@context": context_list, data['q'][count][ct]['attribute']: data['q'][count][ct]['attribute']} 
                expanded = jsonld.expand(com)
                data['q'][count][ct]['attribute'] = list(expanded[0].keys())[0]
                if 'sub-attribute' in data['q'][count][ct]:
                  com = {"@context": context_list, data['q'][count][ct]['sub-attribute']: data['q'][count][ct]['sub-attribute']} 
                  expanded = jsonld.expand(com)
                  data['q'][count][ct]['sub-attribute'] = list(expanded[0].keys())[0]
    status = 1
  except Exception as e:
    app.logger.error("Error: expand_entities_params")
    app.logger.error(traceback.format_exc())
    error = 'Error in expanding entities params.'
  return data, status, error
