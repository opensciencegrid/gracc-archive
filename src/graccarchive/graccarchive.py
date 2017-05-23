
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
import Queue
import shutil
import hashlib
import logging
import tarfile
import argparse
import datetime
import cStringIO
import threading
import signal
import sys
import gzip

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
        self.queue = Queue.Queue(maxsize=1000)
        self.message_counter = 0
        
    def run(self):
        self.createConnection()
        self._chan.basic_consume(self.receiveMsg, self._config["AMQP"]['queue'])
        
        # Start the receiving thread
        self.pika_thread = threading.Thread(target=self.startReceiving)
        self.pika_thread.daemon = True
        self.pika_thread.name = 'RabbitMQ Listener Thread'
        self.pika_thread.start()

        # tarWriter is a generator that generates complete files.
        for source_file in self.tarWriter():
            fname = os.path.split(source_file)[-1]
            output_file = os.path.join(self._config['Directories']['output'], fname)
            print "Copying final archive file from %s to %s" % (source_file, output_file)
            move_without_overwrite(source_file, output_file)


    def createConnection(self):
        try:
            self.parameters = pika.URLParameters(self._config['AMQP']['url'])
            self._conn = pika.adapters.blocking_connection.BlockingConnection(self.parameters)

            self._chan = self._conn.channel()
            # TODO: capture exit codes on all these call
            self._chan.queue_declare(queue=self._config["AMQP"]['queue'], durable=True, auto_delete=self._config['AMQP'].get('auto_delete', False))
            self._chan.queue_bind(self._config["AMQP"]['queue'], self._config["AMQP"]['exchange'])
            self._chan.basic_recover(requeue=True)
        except pika.exceptions.ConnectionClosed:
            print "Connection was closed while setting up connection... exiting"
            sys.exit(1)


    def startReceiving(self):
        
        # The library gives us an event loop built-in, so lets use it!
        # This program only responds to messages on the rabbitmq, so no
        # reason to listen to anything else.
        while True:
            try:
                print "Starting to consume data from queue", self._config['AMQP']['queue']
                self._chan.start_consuming()
            except KeyboardInterrupt:
                self._chan.stop_consuming()
                break
            except pika.exceptions.ConnectionClosed:
                print "Connection was closed, re-opening"
                self.createConnection()
                self._chan.basic_consume(self.receiveMsg, self._config["AMQP"]['queue'])
                continue
            
            
            
    
    def receiveMsg(self, channel, method_frame, header_frame, body):
        
        
        self.message_counter += 1
        self.queue.put(body)
        
        # Every 1000 messages, clear the queue and make sure everything is written
        if self.message_counter % 1000 == 0:
            self.queue.join()
            print "Cleared queue"
            self._chan.basic_ack(method_frame.delivery_tag, multiple=True)
            


    def genFilename(self, dt):
        return os.path.join(self._config['Directories']['sandbox'], dt.strftime("gracc-%Y-%m-%d.tar.gz"))


    def tarWriter(self):
        pw = pwd.getpwnam("nobody")
        counter = 0
        dt = datetime.datetime.utcnow()
        output_fname = self.genFilename(dt)
        gzfile = gzip.GzipFile(output_fname, 'a')
        tf = tarfile.open(fileobj=gzfile, mode="w|")
        record = None
        try:
            while True:
                try:
                    record = self.queue.get(block=True, timeout=10)
                except Queue.Empty as qe:
                    if record is None:
                        continue
                    # Timed out waiting for new updates; let's ACK outstanding requests.
                    print "No updates in the last 10s; syncing file to disk (count=%d)" % counter
                    with open(output_fname, "a") as fp:
                        os.fsync(fp.fileno())
                    record = None
                    continue
                hobj = hashlib.sha256()
                hobj.update(record)
                now = time.time()
                dt = datetime.datetime.utcfromtimestamp(now)
                formatted_time = dt.strftime("gracc/%Y/%m/%d/%H")
                next_output_fname = self.genFilename(dt)
                if next_output_fname != output_fname:
                    tf.close()
                    gzfile.close()
                    print "Switching from %s to %s" % (output_fname, next_output_fname)
                    yield output_fname
                    gzfile = gzip.GzipFile(next_output_fname, 'a')
                    tf = tarfile.open(fileobj=gzfile, mode="w|")
                    counter = 0
                    output_fname = next_output_fname
                fname = "%s/record-%d-%s" % (formatted_time, counter, hobj.hexdigest())
                ti = tarfile.TarInfo(fname)
                sio = cStringIO.StringIO()
                sio.write(record)
                ti.size = sio.tell()
                sio.seek(0)
                ti.uid = pw.pw_uid
                ti.gid = pw.pw_gid
                ti.mtime = now
                ti.mode = 0600
                tf.addfile(ti, sio)
                self.queue.task_done()
                counter += 1
                if counter % 1000 == 0:
                    print "Syncing file to disk (count=%d)" % counter
                    gzfile.flush()
                    with open(output_fname, "a") as fp:
                        os.fsync(fp.fileno())
                    tf.members = []
            gzfile.flush()
            with open(output_fname, "a") as fp:
                os.fsync(fp.fileno())
            print "Finalized last output file: %s" %  output_fname
            yield output_fname
        except SystemExit as se:
            print "Cleaning up after systemexit"
            tf.close()
            gzfile.close()
            yield output_fname


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

    # Capture and call sys.exit for SIGTERM commands
    def exit_gracefully(signum, frame):
        sys.exit(0)
    signal.signal(signal.SIGTERM, exit_gracefully)

    # Move any previous file to the output directory; we cannot append currently.
    for fname in os.listdir(config['Directories']['sandbox']):
        in_fname = os.path.join(config['Directories']['sandbox'], fname)
        out_fname = os.path.join(config['Directories']['output'], fname)
        move_without_overwrite(in_fname, out_fname)

    # Create and run the OverMind
    print "Starting the archiver agent."
    archiver_agent = ArchiverAgent(config)
    archiver_agent.run()


if __name__ == '__main__':
    main()

