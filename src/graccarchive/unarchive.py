
import logging
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
        tf = tarfile.TarFile(tar_file, mode='r')
        
        # For each file in the tar file:
        for member in tf:
            f = tf.extracfile(member)
            print "Sending file %s" % member
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
    
    unarchive = UnArchiver()
    unarchive.createConnection(args['rabbiturl'], args['exchange'])
    
    for tar_file in args['tarfile']:
        parseTarFile(tar_file)

    


if __name__ == '__main__':
    main()


