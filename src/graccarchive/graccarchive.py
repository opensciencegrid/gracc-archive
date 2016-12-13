
import pika
import toml
import argparse
import logging
import threading
import Queue

class ArchiverAgent(object):
    
    def __init__(self, config):
        self._config = config
        self.queue = Queue.queue()
        
    def run(self):
        self.createConnection()
        self._chan.basic_consume(self._receiveMsg, self._config["AMQP"]['queue'])
        
        # Start the receiving thread
        threading.Thread(target=self.startReceiving)
        
        # Start the writer
        self.tarWriter()
        
        
    def createConnection(self):
        credentials = pika.PlainCredentials(self._config['AMQP']['username'], self._config['AMQP']['password'])
        self.parameters = pika.ConnectionParameters(self._config['AMQP']['host'],
                                                5672, self._config['AMQP']['vhost'], credentials)
        self._conn = pika.adapters.blocking_connection.BlockingConnection(self.parameters)
        
        self._chan = self._conn.channel()
        
        # TODO: capture exit codes on all these call
        self._chan.queue_declare(queue=self._config["AMQP"]['queue'], durable=True, auto_delete=False, exclusive=True)
        self._chan.queue_bind(self._config["AMQP"]['queue'], self._config["AMQP"]['exchange'])


    def startReceiving(self):
        
        # The library gives us an event loop built-in, so lets use it!
        # This program only responds to messages on the rabbitmq, so no
        # reason to listen to anything else.
        try:
            self._chan.start_consuming()
        except KeyboardInterrupt:
            self._chan.stop_consuming()
            

    def receiveMsg(self, channel, method_frame, header_frame, body):
        
        self.queue.put(body)
        pass
    
    
    def tarWriter(self):
        job = self.queue.get()
        



def main():
    # Parse arguments
    parser = argparse.ArgumentParser(description="GRACC Archiver Agent")
    parser.add_argument("-c", "--configuration", help="Configuration file location",
                        default="/etc/graccarchive/config.toml", dest='config')
    args = parser.parse_args()
    
    
    # Create and run the OverMind
    archiver_agent = ArchiverAgent(args.config)
    archiver_agent.run()



