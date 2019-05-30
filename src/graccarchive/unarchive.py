
# Copyright 2017 Derek Weitzel
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import tarfile
import argparse
import pika
import json


class UnArchiver(object):
    
    def __init__(self, url, exchange):
        self.url = url
        self.exchange = exchange
        pass
    
    def createConnection(self):
        self.parameters = pika.URLParameters(self.url)
        self._conn = pika.adapters.blocking_connection.BlockingConnection(self.parameters)

        self._chan = self._conn.channel()
        
    def sendRecord(self, record):
        self._chan.basic_publish(exchange=self.exchange, routing_key='', body=record)
        
    def parseTarFile(self, tar_file, start=0):
        tf = tarfile.open(tar_file, mode='r')

        counter = 0
        # For each file in the tar file:
        for member in tf:
            if counter < start:
                counter += 1
                if (counter % 10000) == 0:
                    print("Skipping {} records".format(counter))
                    tf.members = []
                continue
            f = tf.extractfile(member)
            self.sendRecord(f.read())
            counter += 1
            if (counter % 10000) == 0:
                print("Unpacked and sent {} records".format(counter))
                tf.members = []
        
        tf.close()
        
        
class PerfSonarUnArchiver(UnArchiver):
    """
    Subclass of the UnArchiver in order to send PS data
    """
    def __init__(self, url, exchange):
        super(PerfSonarUnArchiver, self).__init__(url, exchange)

    def sendRecord(self, record):
        # Parse the json record, looking for the "event-type"
        json_record = json.loads(record)
        event_type = json_record['meta']['event-type']

        # Prepend the "perfsonar.raw." to the event-type
        routing_key = "perfsonar.raw." + event_type

        self._chan.basic_publish(exchange=self.exchange, routing_key=routing_key, body=record)


def main():
    # Parse arguments
    parser = argparse.ArgumentParser(description="GRACC UnArchiver")
    
    parser.add_argument("rabbiturl", help="Rabbit URL Parameters")
    parser.add_argument("exchange", help="Exchange to send records")
    parser.add_argument("tarfile", nargs='+', help="Tar Files to parse and send")
    parser.add_argument("-p", "--psdata", help="Unarchive perfsonar data", action='store_true')
    parser.add_argument("-s", "--start", help="Record number to start sending", type=int, default=0)
    
    args = parser.parse_args()
    print args
    
    if args.psdata:
        unarchive = PerfSonarUnArchiver(args.rabbiturl, args.exchange)
    else:
        unarchive = UnArchiver(args.rabbiturl, args.exchange)
    unarchive.createConnection()
    
    for tar_file in args.tarfile:
        print "Parsing %s" % tar_file
        unarchive.parseTarFile(tar_file, start=args.start)

    


if __name__ == '__main__':
    main()


