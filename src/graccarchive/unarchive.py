
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
        
    def parseTarFile(self, tar_file):
        tf = tarfile.open(tar_file, mode='r')
        
        # For each file in the tar file:
        for member in tf:
            f = tf.extractfile(member)
            self.sendRecord(f.read())
        
        tf.close()
        
        



def main():
    # Parse arguments
    parser = argparse.ArgumentParser(description="GRACC UnArchiver")
    
    parser.add_argument("rabbiturl", help="Rabbit URL Parameters")
    parser.add_argument("exchange", help="Exchange to send records")
    parser.add_argument("tarfile", nargs='+', help="Tar Files to parse and send")
    
    args = parser.parse_args()
    print args
    
    unarchive = UnArchiver(args.rabbiturl, args.exchange)
    unarchive.createConnection()
    
    for tar_file in args.tarfile:
        print "Parsing %s" % tar_file
        unarchive.parseTarFile(tar_file)

    


if __name__ == '__main__':
    main()


