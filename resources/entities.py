import json
import traceback
from resources.postgres import start_statement
from resources.context import default_context
from resources.postgres import *
import datetime
import validators
from pyld import jsonld

def build_sql_query_for_q_for_dict_for_having(statement, params, q_params, count, q_type):
  """Build sql query for q param"""
  if 'sub-attribute' in q_params:
    attr_value = 'attr_value_' + q_type + str(count)
    sub_value = 'sub_value_'  + q_type + str(count)
    st = sub_attributes_having_statement.replace('sub_value', sub_value).replace('attr_value', attr_value)
    statement += ' AND ' + st
    params[attr_value] = q_params['attribute']
    params[sub_value] = q_params['sub-attribute']
  else:
    attr_value = 'attr_value_' + q_type + str(count)
    st = attributes_having_statement.replace('attr_value', attr_value)
    statement += ' AND ' + st
    params[attr_value] = q_params['attribute']
  return statement, params

def build_sql_query_for_q_for_dict_for_range(statement, params, q_params, count, q_type):
  """Build sql query for q param"""
  if 'sub-attribute' in q_params:
    attr_value = 'attr_value_' + q_type + str(count)
    sub_value = 'sub_value_'  + q_type + str(count)
    low_range_value = 'low_range_value_' + q_type + str(count)
    high_range_value = 'high_range_value' + q_type + str(count)
    st = sub_attributes_range_statement.replace('sub_value', sub_value).replace('attr_value', attr_value).replace('low_range_value', low_range_value).replace('high_range_value',high_range_value)
    statement += ' AND ' + st
    params[attr_value] = q_params['attribute']
    params[sub_value] = q_params['sub-attribute']
    params[low_range_value] = q_params['value'][0]
    params[high_range_value] = q_params['value'][1]
  else:
    attr_value = 'attr_value_' + q_type + str(count)
    low_range_value = 'low_range_value_' + q_type + str(count)
    high_range_value = 'high_range_value' + q_type + str(count)
    st = attributes_range_statement.replace('attr_value', attr_value).replace('low_range_value', low_range_value).replace('high_range_value',high_range_value)
    statement += ' AND ' + st
    params[attr_value] = q_params['attribute']
    params[low_range_value] = q_params['value'][0]
    params[high_range_value] = q_params['value'][1]
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
    status = 1
  except Exception as e:
    app.logger.error("Error: build_sql_query_for_q_for_dict")
    app.logger.error(traceback.format_exc())
  return statement, params, status, error

def build_sql_query_for_q(statement, params, q_params, app):
  """Build sql query for q param"""
  status = 0
  q_dict = 'q_dict'
  error = 'Error in building sql query for entities for q param.'
  try:
    for count in range(0, len(q_params)):
      if type(q_params[count]) is dict:
        statement, params, status, error = build_sql_query_for_q_for_dict(statement, params, q_params[count], count, q_dict, app)
        if not status:
          return statement, params, status, error
    status = 1
  except Exception as e:
    app.logger.error("Error: build_sql_query_for_q")
    app.logger.error(traceback.format_exc())
  return statement, params, status, error

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
    if data['attrs'] and len(data['attrs']) > 0:
      statement += ' AND attributes_table.id in ('
      for index in range(0,len(data['attrs'])):
        if index == (len(data['attrs']) -1):
          statement += '%(attrs'+str(index)+')s'
        else:
          statement += '%(attrs'+str(index)+')s,'
        params['attrs'+str(index)] = data['attrs'][index]
      statement += ')'
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
    if data['q']:
      statement, params, status, error = build_sql_query_for_q(statement, params, data['q'], app)
      if not status:
        return statement, params, status, error
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
      elif param[count] == '|':
        start, end, status, error = get_q_params_in_list(start, count, param, q, app)
        q.append('OR')
      elif param[count] == ';':
        start, end, status, error = get_q_params_in_list(start, count, param, q, app)
        q.append('AND')
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
          q = {'attribute': attrs[0], 'operation': op, 'value': param[1], 'sub-attribute': attrs[1]}
        elif '[' in param[0] and ']' in param[0]:
          attrs = param[0].replace(']', '').split('[')
          q = {'attribute': attrs[0], 'operation': op, 'value': param[1], 'column': attrs[1]}
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
    app.logger.info(data['q'])
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
    app.logger.info(data['q'])
  except Exception as e:
    app.logger.error("Error: expand_entities_params")
    app.logger.error(traceback.format_exc())
  return data, status, error
