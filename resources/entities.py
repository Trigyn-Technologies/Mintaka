import json
import traceback
from resources.postgres import start_statement
from resources.context import default_context
from resources.postgres import *
import datetime
import validators
from pyld import jsonld

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
    statement += ' AND ' + st
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
    statement += ' AND ' + st
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
    statement += ' AND ' + st
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
    statement += ' AND ' + st
  else:
    statement += ' OR ' + st      
  return statement, params

def build_sql_query_for_q_for_dict(statement, params, q_params, count, q_type, app):
  """Build sql query for q param"""
  status = 0
  error = 'Error in building sql query for entities for q param.'
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
  error = 'Error in building sql query for entities for q param.'
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
  return statement, params, status, error


def build_sql_query_for_q(statement, params, q_params, attributes, app):
  """Build sql query for q param"""
  status = 0
  q_dict = 'q_dict'
  q_list = 'q_list'
  error = 'Error in building sql query for entities for q param.'
  attr_statement = attributes_having_statement
  sub_statement = sub_attributes_having_statement
  sub_attribute_flag = 0 
  try:
    q_len = len(q_params)
    for count in range(0, q_len):
      if type(q_params[count]) is dict:
        statement, params, status, error = build_sql_query_for_q_for_dict(statement, params, q_params[count], count, q_dict, app)
        attributes.append(q_params[count]['attribute'])
        if not status:
          return statement, params, attributes, status, error
      else:
        if count == 0:
          statement += ' AND ('
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
    status = 1
  except Exception as e:
    app.logger.error("Error: build_sql_query_for_q")
    app.logger.error(traceback.format_exc())
  return statement, params, attributes, status, error

def build_sql_query_for_entities(data, app):
  """Build sql query"""
  statement = start_statement
  params = {}
  status = 0
  error = 'Error in building sql query for entities.'
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
    if data['id_data'] and len(data['id_data']) > 0:
      statement += ' AND entity_table.entity_id in ('
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
    if data['idPattern']:
      statement += " AND entity_table.entity_id like %(idPattern)s"
      params["idPattern"] = '%{}%'.format(data['idPattern'])
    attributes = []
    if data['q']:
      statement, params, attributes, status, error = build_sql_query_for_q(statement, params, data['q'], attributes, app)
      if not status:
        return statement, params, status, error
    run_attr = 1
    if len(attributes) > 0:
      attr_flag = 0
      run_attr = 0
      run_not_attr = 1
      if data['attrs'] and len(data['attrs']) > 0:
        run_not_attr = 0
        for attr in data['attrs']:
          if attr not in attributes:
            attr_flag = 1
            break
      if attr_flag:
        statement += ' OR (attributes_table.id in ('
        for index in range(0,len(data['attrs'])):
          if index == (len(data['attrs']) -1):
            statement += '%(attrs'+str(index)+')s'
          else:
            statement += '%(attrs'+str(index)+')s,'
          params['attrs'+str(index)] = data['attrs'][index]
        statement += ') AND '
        statement += 'attributes_table.id not in ('
        for index in range(0,len(attributes)):
          if index == (len(attributes) -1):
            statement += '%(attributes'+str(index)+')s'
          else:
            statement += '%(attributes'+str(index)+')s,'
          params['attributes'+str(index)] = attributes[index]
        statement += '))'
      elif run_not_attr:
        statement += ' OR (attributes_table.id not in ('
        for index in range(0,len(attributes)):
          if index == (len(attributes) -1):
            statement += '%(attributes'+str(index)+')s'
          else:
            statement += '%(attributes'+str(index)+')s,'
          params['attributes'+str(index)] = attributes[index]
        statement += '))'
    if run_attr and data['attrs'] and len(data['attrs']) > 0:
      statement += ' AND attributes_table.id in ('
      for index in range(0,len(data['attrs'])):
        if index == (len(data['attrs']) -1):
          statement += '%(attrs'+str(index)+')s'
        else:
          statement += '%(attrs'+str(index)+')s,'
        params['attrs'+str(index)] = data['attrs'][index]
      statement += ')'
    statement +=" order by entity_table."+ data['timeproperty']+" desc"
    if data['lastN']:
      statement += " limit %(lastN)s"
      params["lastN"] = data['lastN']  
    statement += "%s"%(";")
    status = 1
    app.logger.info(statement)
    app.logger.info(params)
  except Exception as e:
    app.logger.error("Error: build_sql_query_for_entities")
    app.logger.error(traceback.format_exc())
  return statement, params, status, error

def get_temporal_entities_parameters(args, context, app):
  """Parse params"""
  data = {'timerel': None, 'time': None, 'endtime': None, 'timeproperty': 'observedAt', 'attrs': None, 'lastN': None, 'id_data': '', 'type_data': '','idPattern': None, 'q':None, 'csf':None, 'georel': None, 'geometry': None, 'coordinates': None, 'geoproperty': None}
  timepropertyDict = {'modifiedAt':'modified_at', 'observedAt' :'observed_at', 'createdAt':'created_at'}
  status = 0
  error = 'Error in getting temporal entities parameters.'
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
      data['lastN'] = args.get('lastN')
    if 'idPattern' in args and args.get('idPattern'):
      data['idPattern'] = args.get('idPattern')
    if 'q' in args and args.get('q'):
      data['q'] = args.get('q')
      data, status, error = get_q_params(data, app)
      if not status:
        return data, status, error
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
      ids = args.get('id')
      if ids:
        data['id_data'] = ids.split(',') 
    if 'type' in args:
      types = args.get('type')
      if types:
        data['type_data'] = types.split(',')
    data, status, error = expand_entities_params(data, context, app)
  except Exception as e:
    app.logger.error("Error: get_temporal_entities_parameters")
    app.logger.error(traceback.format_exc())
  return data, status, error

def get_q_params_in_list(start, count, param, q, app):
  """Covert q params to list"""
  status = 0
  error = 'Error in get q params in list'
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
  return start, end, status, error

def parse_q_multiple(param, app):
  """Parse q params"""
  status = 0
  error = 'Error in parsing q miltiple params'
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
  return q, status, error

def parse_q_single(param, app):
  """Parse q params"""
  op_list = ['==', '!=', '>=','<=','>', '<','!~=','~=']
  status = 0
  q = {}
  error = 'Error in parsing q single params.'
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
  return q, status, error

def get_q_params(data, app):
  """Parse q params"""
  op_list = ['==', '!=', '>=','<=','>', '<','!~=','~=']
  status = 0
  error = 'Error in parsing q params'
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
  return data, status, error

def split_q_params(q_params, app):
  """Parse q params"""
  q = []
  status = 0
  error = 'Error in split q params'
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
  return q, status, error


def expand_entities_params(data, context, app):
  """Expand entities params"""
  context_list = []
  status = 0
  error = 'Error in expanding entities params.'
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
  return data, status, error
