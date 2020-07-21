import json

def get_normal_value_string(record, attr_val, attr_dict):
  attr_dict['type'] = 'Property'
  attr_dict['value'] = record[attr_val['value_string']]
  return attr_dict

def get_normal_value_boolean(record, attr_val, attr_dict):
  attr_dict['type'] = 'Property'
  if record[attr_val['value_boolean']]:
    attr_dict['value'] = 'true'
  else:
    attr_dict['value'] = 'false'
  return attr_dict

def get_normal_value_relation(record, attr_val, attr_dict):
  attr_dict['type'] = 'Relationship'
  attr_dict['object'] = record[attr_val['value_relation']]
  return attr_dict

def get_normal_value_number(record, attr_val, attr_dict):
  attr_dict['type'] = 'Property'
  attr_dict['value'] = record[attr_val['value_number']]
  return attr_dict

def get_normal_value_object(record, attr_val, attr_dict):
  attr_dict['type'] = 'Property'
  try:
    attr_dict['value'] = json.loads(record[attr_val['value_object']])
  except:
    attr_dict['value'] = record[attr_val['value_object']]
  return attr_dict

def get_normal_value_datetime(record, attr_val, attr_dict):
  attr_dict['type'] = 'Property'
  attr_dict['object'] = {"@type": "DateTime","@value":record[attr_val['value_datetime']].replace(' ', '')}
  return attr_dict

def get_normal_value_geo(record, attr_val, attr_dict):
  attr_dict['type'] = 'GeoProperty'
  attr_dict['value'] = json.loads(record[attr_val['location']])
  return attr_dict

def get_temporal_value_string(record, attr_val, attr_list, response_data, attr):
  response_data[attr]['type'] = 'Property'
  attr_list.append(record[attr_val['value_string']])
  return attr_list

def get_temporal_value_boolean(record, attr_val, attr_list, response_data, attr):
  response_data[attr]['type'] = 'Property'
  if record[attr_val['value_boolean']]:
    attr_list.append('true')
  else:
    attr_list.append('false')
  return attr_list

def get_temporal_value_number(record, attr_val, attr_list, response_data, attr):
  response_data[attr]['type'] = 'Property'
  attr_list.append(record[attr_val['value_number']])
  return attr_list

def get_temporal_value_object(record, attr_val, attr_list, response_data, attr):
  response_data[attr]['type'] = 'Property'
  try:
    attr_list.append(json.loads(record[attr_val['value_object']]))
  except:
    attr_list.append(record[attr_val['value_object']])
  return attr_list

def get_temporal_value_relation(record, attr_val, attr_list, response_data, attr):
  response_data[attr]['type'] = 'Relationship'
  attr_list.append(record[attr_val['value_relation']])
  return attr_list

def get_temporal_value_datetime(record, attr_val, attr_list, response_data, attr):
  response_data[attr]['type'] = 'Property'
  attr_list.append(record[attr_val['value_datetime']].replace(' ', ''))
  return attr_list

def get_temporal_value_geo(record, attr_val, attr_list, response_data, attr):
  response_data[attr]['type'] = 'GeoProperty'
  attr_list.append(json.loads(record[attr_val['location']]))
  return attr_list
