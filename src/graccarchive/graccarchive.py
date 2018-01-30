
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


import os
import pwd
import time
import errno
import shutil
import hashlib
import tarfile
import argparse
import datetime
import cStringIO
import signal
import sys
import gzip
import socket

import pika
import pika.exceptions
import toml

def move_without_overwrite(src, orig_dest):
    counter = 0
    while True:
        dest_dir, dest_fname = os.path.split(orig_dest)
        if counter:
            parts = dest_fname.split(".")
            parts = [parts[0], "%d" % counter] + parts[1:]
            dest_fname = ".".join(parts)
        dest = os.path.join(dest_dir, dest_fname)
        try:
            fd = os.open(dest, os.O_CREAT|os.O_EXCL)
            break
        except OSError as oe:
            if (oe.errno != errno.EEXIST):
                raise
        counter += 1
    try:
        shutil.move(src, dest)
    finally:
        os.close(fd)


class ArchiverAgent(object):
    
    def __init__(self, config):
        self._config = config
        self.message_counter = 0
        self.output_file = None
        self.pw = pwd.getpwnam("nobody")
        self.delivery_tag = None
        self._conn = None
        self._chan = None
        self.parameters = None
        self.timer_id = None
        self._closing = False
        
        # Initialize the output file
        now = time.time()
        dt = datetime.datetime.utcfromtimestamp(now)
        output_fname = self.genFilename(dt)
        self.gzfile = gzip.GzipFile(output_fname, 'a')
        self.output_file = output_fname
        self.tf = tarfile.open(fileobj=self.gzfile, mode="w|")
        
        
    def run(self):
        self.createConnection()
        self._conn.ioloop.start()


    def createConnection(self):
        try:
            print "Creating connection"
            self.parameters = pika.URLParameters(self._config['AMQP']['url'])
            #self._conn = pika.adapters.blocking_connection.BlockingConnection(self.parameters)
            self._conn = pika.SelectConnection(self.parameters,
                                     self.on_connection_open,
                                     stop_ioloop_on_close=False)
            return self._conn
            
        except pika.exceptions.ConnectionClosed:
            print "Connection was closed while setting up connection... exiting"
            sys.exit(1)
            
    def on_connection_closed(self, connection, reply_code, reply_text):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param int reply_code: The server provided reply_code if given
        :param str reply_text: The server provided reply_text if given

        """
        self._chan = None
        if self._closing:
            self._con.ioloop.stop()
        else:
            print('Connection closed, reopening in 5 seconds: (%s) %s',
                           reply_code, reply_text)
            self._conn.add_timeout(5, self.reconnect)
    
    def reconnect(self):
        """Will be invoked by the IOLoop timer if the connection is
        closed. See the on_connection_closed method.

        """
        # This is the old connection IOLoop instance, stop its ioloop
        self._conn.ioloop.stop()

        if not self._closing:

            # Create a new connection
            self._conn = self.createConnection()

            # There is now a new connection, needs a new ioloop to run
            self._conn.ioloop.start()

    def on_connection_open(self, unused_connection):
        print("Conneciton opened")
        self._timeoutFunc()
        self._conn.add_on_close_callback(self.on_connection_closed)
        self._chan = self._conn.channel(self.on_channel_open)
        

    def on_channel_open(self, channel):
        print("Channel opened")
        self._chan = channel
        self._chan.basic_qos(prefetch_count=2000)
        self._chan.add_on_close_callback(self.on_channel_closed)
        self.setup_queue()

    def on_channel_closed(self, channel, reply_code, reply_text):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel: The closed channel
        :param int reply_code: The numeric reason the channel was closed
        :param str reply_text: The text reason the channel was closed

        """
        print('Channel %i was closed: (%s) %s',
                       channel, reply_code, reply_text)
        self._conn.close()

    def setup_queue(self):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.

        :param str|unicode queue_name: The name of the queue to declare.

        """
        self._chan.queue_declare(self.on_queue_declareok, queue=self._config["AMQP"]['queue'], durable=True, auto_delete=self._config['AMQP'].get('auto_delete', False))

    def on_queue_declareok(self, method_frame):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.

        :param pika.frame.Method method_frame: The Queue.DeclareOk frame

        """
        print("Queue declare ok")
        self._chan.queue_bind(self.on_bindok, self._config["AMQP"]['queue'], self._config["AMQP"]['exchange'])

    def on_bindok(self, unused_frame):
        """Invoked by pika when the Queue.Bind method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.

        :param pika.frame.Method unused_frame: The Queue.BindOk response frame

        """
        print("Bind ok")
        self._chan.basic_recover(requeue=True)
        self.start_consuming()
        
        
    def start_consuming(self):
        """This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. The on_message method is passed in as a callback pika
        will invoke when a message is fully received.

        """
        print("Starting consuming")
        self._chan.add_on_cancel_callback(self.on_consumer_cancelled)
        self._consumer_tag = self._chan.basic_consume(self.receiveMsg, self._config["AMQP"]['queue'])

    def on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.

        :param pika.frame.Method method_frame: The Basic.Cancel frame

        """
        print('Consumer was cancelled remotely, shutting down: %r',
                    method_frame)
        if self._chan:
            self._chan.close()

    def receiveMsg(self, channel, method_frame, header_frame, body):
        self.tarWriter(body, method_frame.delivery_tag)

    def genFilename(self, dt):
        return os.path.join(self._config['Directories']['sandbox'], "gracc-{0}-{1}.tar.gz".format(socket.gethostname(), dt.strftime("%Y-%m-%d")))

    def genTarFile(self, dt):
        output_fname = self.genFilename(dt)
        if self.output_file == output_fname:
            return self.tf

        # Move the previous output file
        fname = os.path.split(self.output_file)[-1]
        final_output_fname = os.path.join(self._config['Directories']['output'], fname)
        # Close all the things
        self.tf.close()
        self.gzfile.close()
        print "Copying final archive file from %s to %s" % (self.output_file, final_output_fname)
        move_without_overwrite(self.output_file, final_output_fname)
        self.gzfile = gzip.GzipFile(output_fname, 'a')
        self.output_file = output_fname
        self.tf = tarfile.open(fileobj=self.gzfile, mode="w|")
        return self.tf

    def tarWriter(self, record, delivery_tag):
        now = time.time()
        dt = datetime.datetime.utcfromtimestamp(now)
        tf = self.genTarFile(dt)
        hobj = hashlib.sha256()
        hobj.update(record)
        formatted_time = dt.strftime("gracc/%Y/%m/%d/%H")
        fname = "%s/record-%d-%s" % (formatted_time, self.message_counter, hobj.hexdigest())
        ti = tarfile.TarInfo(fname)
        sio = cStringIO.StringIO()
        sio.write(record)
        ti.size = sio.tell()
        sio.seek(0)
        ti.uid = self.pw.pw_uid
        ti.gid = self.pw.pw_gid
        ti.mtime = now
        ti.mode = 0600
        tf.addfile(ti, sio)
        self.recordTag(delivery_tag)

    def recordTag(self, delivery_tag):
        self.delivery_tag = delivery_tag
        self.message_counter += 1
        if self.message_counter % 1000 == 0:
            print "Syncing file to disk (count=%d)" % self.message_counter
            self.flushFile()
            self.tf.members = []
        
    def _timeoutFunc(self):
        self.flushFile()
        self.timer_id = self._conn.add_timeout(10, self._timeoutFunc)

    def flushFile(self):
        self.gzfile.flush()
        with open(self.output_file, "a") as fp:
            os.fsync(fp.fileno())
        print "Cleared queue; ack'ing: %i" % self.message_counter
        if self.delivery_tag:
            self._chan.basic_ack(self.delivery_tag, multiple=True)
            self.delivery_tag = None
        
    def closeFile(self):
        if self.tf:
            self.tf.close()
        if self.gzfile:
            self.gzfile.close()


def main():
    # Parse arguments
    parser = argparse.ArgumentParser(description="GRACC Archiver Agent")
    parser.add_argument("-c", "--configuration", help="Configuration file location",
                        default=[], dest='config', action="append")
    parser.add_argument("--development", help="Run the archiver in development mode",
                        action="store_true", dest="dev")
    args = parser.parse_args()
    config = {}
    if not args.config:
        args.config = ["/etc/graccarchive/config.toml"]
    for conffile in args.config:
        with open(conffile) as fp:
            config.update(toml.load(fp))

    if args.dev:
        config.setdefault('AMQP', {})['auto_delete'] = 'true'

    # Move any previous file to the output directory; we cannot append currently.
    for fname in os.listdir(config['Directories']['sandbox']):
        in_fname = os.path.join(config['Directories']['sandbox'], fname)
        out_fname = os.path.join(config['Directories']['output'], fname)
        move_without_overwrite(in_fname, out_fname)

    # Create and run the OverMind
    print "Starting the archiver agent."
    archiver_agent = ArchiverAgent(config)
    # Capture and call sys.exit for SIGTERM commands
    def exit_gracefully(signum, frame):
        archiver_agent.flushFile()
        archiver_agent.closeFile()
        sys.exit(0)
    signal.signal(signal.SIGTERM, exit_gracefully)
    
    archiver_agent.run()


if __name__ == '__main__':
    main()

