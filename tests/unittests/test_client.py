from datetime import datetime, timedelta
import unittest

from graccreq import Client



class TestClient(unittest.TestCase):
    def setUp(self):

        #self.client = Client.Client("gracc.osg.requests")
        pass
        
    def test_raw(self):
        # Set the timerange
        start_time = datetime(2016, 6, 1)
        end_time = start_time + timedelta(days=32)
        status = {'num_messages': 0}

        def getMessage(msg):
            status['num_messages'] += 1
            
        client = Client("gracc.osg.requests", "gracc.osg.requests")
        client.query(start_time, end_time, 'raw', destination_exchange="gracc.osg.raw", destination_key="gracc.osg.raw")
               
        
