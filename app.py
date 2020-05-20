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
			statement = "SELECT * FROM entity WHERE observedat>%(time)s"
			params["time"] = data['time']
		elif data['timerel'] == 'before':
			statement = "SELECT * FROM entity WHERE observedat<%(time)s"
			params["time"] = data['time']
		else:
			statement = "SELECT * FROM entity WHERE observedat>=%(time)s AND observedat<%(endtime)s"
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
		data = get_temporal_entity_parameters(args)
		if data['timerel'] not in ['after','before', 'between']:
			return Response("Wrong timerel property", status=400, )
		if data['timerel'] == 'between' and (not data['endtime']):
			return Response("Wrong endtime value", status=400, )
		statement, params = build_sql_query_for_entity(data, entity_id)
		cursor.execute(statement,params)
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
		app.logger.error(traceback.format_exc())
		abort(400)

def build_sql_query_for_entity(data, entity_id):
	statement = ''
	params = {}
	try:
		if data['timerel'] == 'after':
			statement = "SELECT * FROM entity WHERE observedat>%(time)s"
			params["time"] = data['time']
		elif data['timerel'] == 'before':
			statement = "SELECT * FROM entity WHERE observedat<%(time)s"
			params["time"] = data['time']
		else:
			statement = "SELECT * FROM entity WHERE observedat>=%(time)s AND observedat<%(endtime)s"
			params["time"] = data['time']
			params["endtime"] = data['endtime']
		statement += " AND id = %(entity_id)s order by observedat desc"
		params["entity_id"] = entity_id
		if data['lastN']:
			statement += " limit %(lastN)s"
			params["lastN"] = data['lastN'] 
		statement += "%s"%(';')
	except Exception as e:
		app.logger.error("Error: build_sql_query_for_entity")
		app.logger.error(traceback.format_exc())
	return statement, params

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
		app.logger.error("Error: get_temporal_entity_parameters")
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


default_context = 'https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld'
load_context(default_context)

# if __name__ == '__main__':
	
	#app.run(debug=True, host='0.0.0.0')