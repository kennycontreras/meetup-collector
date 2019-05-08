import os
import pandas as pd
from event import Event


os.environ['MEETUP_API_KEY'] = '1e742615172342564c18644174823'

default_args = dict(country='United Stated',
                    key=os.environ['MEETUP_API_KEY'], zip='10012', format='json')
url_path = "https://api.meetup.com/2/open_events"

event = Event(params=default_args, url_request=url_path)

json_response = event.get_events()

df = pd.read_json(json_response)

print(json_response)
