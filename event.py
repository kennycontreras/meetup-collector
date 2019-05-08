import requests
import json


class Event:

    def __init__(self,
                 params='',
                 url_request=''
                 ):
        self.params = params
        self.url_request = url_request

    def get_events(self, *args, **kwargs):

        r = requests.get(self.url_request, params=self.params)
        print(r.url)
        response_string = r.read()

        return json.loads(response_string)
