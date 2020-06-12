import json
import traceback
import requests

default_context = 'https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld'

def get_context(request, app):
  """Get context from reauest and load in app dict"""
  context = ''
  try:
    if 'Link' in request.headers and request.headers['Link']:
      context = request.headers['Link'].replace('<','').replace('>','').split(';')[0]
      if context not in app.context_dict.keys():
        load_context(context, app)
  except Exception as e:
    app.logger.error("Error: get_context")
    app.logger.error(traceback.format_exc())
  return context

def load_context(context, app):
  """Load context in app dict"""
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
