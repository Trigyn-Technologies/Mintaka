import os
from psycopg2 import connect
import traceback

def create_postgres_connection(request, app):
  """Create postgres connection"""
  conn = None
  status = 0
  error = 'Error in creating postgres connection.'
  try:
    if 'NGSILD-Tenant' in request.headers and request.headers['NGSILD-Tenant']:
      dbName = request.headers['NGSILD-Tenant']
    else:
      dbName = os.getenv('POSTGRES_DB')
    user = os.getenv('POSTGRES_USER')
    host = os.getenv('POSTGRES_HOST')
    password = os.getenv('POSTGRES_PASSWORD')
    conn = connect(dbname = dbName, user = user,host = host,password = password)
    status = 1
  except Exception as e:
    app.logger.error("Error: create_postgres_connection")
    app.logger.error(traceback.format_exc())
  return conn, status, error

def close_postgres_connection(cursor, conn, app):
  """Close postgres connection"""
  status = 0
  error = 'Error in closing postgres connection.'
  try:
    cursor.close()
    conn.close()
    status = 1
  except Exception as e:
    app.logger.error("Error: close_postgres_connection")
    app.logger.error(traceback.format_exc())
  return status, error

start_statement = "SELECT entity_table.entity_id as entity_id, entity_table.entity_type as entity_type,ST_AsGeoJSON(entity_table.geo_property) as entity_geo_property,to_char(entity_table.created_at, 'YYYY-MM-DD T HH24:MI:SS.MSZ') as entity_created_at,to_char(entity_table.modified_at, 'YYYY-MM-DD T HH24:MI:SS.MSZ') as entity_modified_at, to_char(entity_table.observed_at, 'YYYY-MM-DD T HH24:MI:SS.MSZ') as entity_observed_at, attributes_table.name as attribute_name, attributes_table.id as attribute_id, attributes_table.value_type as attribute_value_type, attributes_table.sub_property as attribute_sub_property, attributes_table.unit_code as attribute_unit_code, attributes_table.data_set_id as attribute_data_set_id, attributes_table.instance_id as attribute_instance_id, attributes_table.value_string as attribute_value_string, attributes_table.value_boolean as attribute_value_boolean, attributes_table.value_number as attribute_value_number, attributes_table.value_relation as attribute_value_relation, attributes_table.value_object as attribute_value_object, ST_AsGeoJSON(attributes_table.geo_property) as attribute_geo_property, to_char(attributes_table.created_at, 'YYYY-MM-DD T HH24:MI:SS.MSZ') as attribute_created_at, to_char(attributes_table.modified_at, 'YYYY-MM-DD T HH24:MI:SS.MSZ') as attribute_modified_at, to_char(attributes_table.observed_at, 'YYYY-MM-DD T HH24:MI:SS.MSZ') as attribute_observed_at, attribute_sub_properties_table.name as subattribute_name, attribute_sub_properties_table.id as subattribute_id, attribute_sub_properties_table.value_type as subattribute_value_type, attribute_sub_properties_table.value_string as subattribute_value_string, attribute_sub_properties_table.value_boolean as subattribute_value_boolean, attribute_sub_properties_table.value_number as subattribute_value_number, attribute_sub_properties_table.value_relation as subattribute_value_relation, ST_AsGeoJSON(attribute_sub_properties_table.geo_property) as subattribute_geo_property, attribute_sub_properties_table.value_object as subattribute_value_object,attribute_sub_properties_table.unit_code as subattribute_unit_code, to_char(attributes_table.value_datetime, 'YYYY-MM-DD T HH24:MI:SS.MSZ') as attribute_value_datetime, to_char(attribute_sub_properties_table.value_datetime, 'YYYY-MM-DD T HH24:MI:SS.MSZ') as subattribute_value_datetime from attributes_table FULL OUTER JOIN entity_table ON entity_table.entity_id = attributes_table.entity_id FULL OUTER JOIN attribute_sub_properties_table ON attribute_sub_properties_table.attribute_instance_id = attributes_table.instance_id"
