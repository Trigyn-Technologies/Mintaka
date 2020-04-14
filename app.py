from flask import Flask, request, Response, abort
import os
from psycopg2 import connect
import json
import datetime
import traceback

app = Flask(__name__)
app.debug = True

dbName = os.getenv('POSTGRES_DB')
user = os.getenv('POSTGRES_USER')
host = os.getenv('POSTGRES_HOST')
password = os.getenv('POSTGRES_PASSWORD')
conn = connect(dbname = dbName, user = user,host = host,password = password)
cursor = conn.cursor()

@app.route('/temporal/entities/', methods=['GET'])
def get_temporal_entities():
	response_data = []
	try:
		if request.method != 'GET':
			return Response("No get data", status=400, )
		args = request.args
		data = get_temporal_entities_parameters(args)
		if data['timerel'] not in ['after','before', 'between']:
			return Response("Wrong timerel property", status=400, )
		if data['timerel'] == 'between' and (not data['endtime']):
			return Response("Wrong endtime value", status=400, )
		statement = build_sql_query_for_entities(data)
		print(statement)
		cursor.execute(statement)
		for i, record in enumerate(cursor):
			record = list(record)
			record[0] = record[0].strftime("%Y-%m-%dT%H:%M:%SZ")
			response_data.append(record)
		response = app.response_class(response=json.dumps(response_data, indent=2), status=200,mimetype='application/json')
		return response
	except Exception as e:
		print("Error: get_temporal_entity")
		print(e)
		print(traceback.format_exc())
		abort(400)

def build_sql_query_for_entities(data):
	statement = ''
	try:
		if data['timerel'] == 'after':
			statement = "SELECT * FROM entity WHERE observedat>'"+data['time']+"'"
		elif data['timerel'] == 'before':
			statement = "SELECT * FROM entity WHERE observedat<'"+data['time']+"'"
		else:
			statement = "SELECT * FROM entity WHERE observedat>='"+data['time']+"' AND observedat<'"+data['endtime']+"'"
		if data['id_data']:
			statement += ' AND id in ' + data['id_data']
		if data['type_data']:
			statement += ' AND type in' + data['type_data']
		if data['idPattern']:
			statement += " AND lower(id) like '%" + data['idPattern'] + "%'"
		statement +=" order by observedat desc"
		if data['lastN']:
			statement += " limit " + data['lastN']	
		statement += ";"
	except Exception as e:
		print("Error: build_sql_query_for_entities")
		print(traceback.format_exc())
	return statement

def get_temporal_entities_parameters(args):
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
				ids = ids.split(',') 
			if len(ids) == 1:
				data['id_data'] += "('" + ids[0] + "')"
			else:
				for index in range(0,len(ids)):
					if index == 0:
						data['id_data'] += "('" + ids[index] + "',"
					elif index == (len(ids) -1):
						data['id_data'] += "'"+ ids[index] + "')"
					else:
						data['id_data'] += "'"+ ids[index] + "',"
		if 'type' in args:
			types = args.get('type')
			if types:
				types = types.split(',')
			if len(types) == 1:
				data['type_data'] += "('" + types[0] + "')"
			else:
				for index in range(0,len(types)):
					if index == 0:
						data['type_data'] += "('" + types[index] + "',"
					elif index == (len(types) -1):
						data['type_data'] +=  "'"+types[index] + "')"
					else:
						data['type_data'] +=  "'"+types[index] + "',"
	except Exception as e:
		print("Error: get_temporal_entities_parameters")
		print(traceback.format_exc())
	return data

@app.route('/temporal/entities/<entity_id>/', methods=['GET'])
def get_temporal_entity(entity_id):
	response_data = []
	try:
		if request.method != 'GET':
			return Response("No get data", status=400, )
		args = request.args
		data = get_temporal_entity_parameters(args)
		if data['timerel'] not in ['after','before', 'between']:
			return Response("Wrong timerel property", status=400, )
		if data['timerel'] == 'between' and (not data['endtime']):
			return Response("Wrong endtime value", status=400, )
		statement = build_sql_query_for_entity(data, entity_id)
		cursor.execute(statement)
		for i, record in enumerate(cursor):
			record = list(record)
			record[0] = record[0].strftime("%Y-%m-%dT%H:%M:%SZ")
			response_data.append(record)
		response = app.response_class(response=json.dumps(response_data, indent=2), status=200,mimetype='application/json')
		return response
	except Exception as e:
		print("Error: get_temporal_entity")
		print(traceback.format_exc())
		abort(400)

def build_sql_query_for_entity(data, entity_id):
	statement = ''
	try:
		if data['timerel'] == 'after':
			statement = "SELECT * FROM entity WHERE observedat>'"+data['time']+"'"
		elif data['timerel'] == 'before':
			statement = "SELECT * FROM entity WHERE observedat<'"+data['time']+"'"
		else:
			statement = "SELECT * FROM entity WHERE observedat>='"+data['time']+"' AND observedat<'"+data['endtime']+"'"
		statement += " AND id = '" + entity_id + "' order by observedat desc"
		if data['lastN']:
			statement += " limit " + data['lastN']	
		statement += ";"
	except Exception as e:
		print("Error: build_sql_query_for_entity")
		print(traceback.format_exc())
	return statement

def get_temporal_entity_parameters(args):
	data = {'timerel': None, 'time': None, 'endtime': None, 'timeproperty': 'observedAt', 'attrs': None, 'lastN': None}
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
	except Exception as e:
		print("Error: get_temporal_entity_parameters")
		print(traceback.format_exc())
	return data

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')