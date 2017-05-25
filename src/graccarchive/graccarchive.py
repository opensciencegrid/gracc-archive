
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
        
        # Initialize the output file
        now = time.time()
        dt = datetime.datetime.utcfromtimestamp(now)
        output_fname = self.genFilename(dt)
        self.gzfile = gzip.GzipFile(output_fname, 'a')
        self.output_file = output_fname
        self.tf = tarfile.open(fileobj=self.gzfile, mode="w|")
        
        
    def run(self):
        self.createConnection()
        self._chan.basic_consume(self.receiveMsg, self._config["AMQP"]['queue'])
        self._timeoutFunc()

        self.startReceiving()

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
        self.tarWriter(body, method_frame.delivery_tag)

    def genFilename(self, dt):
        return os.path.join(self._config['Directories']['sandbox'], dt.strftime("gracc-%Y-%m-%d.tar.gz"))

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
            self.tf.members = []
            self.flushFile()
        
    def _timeoutFunc(self):
        self.flushFile()
        self.timer_id = self._conn.add_timeout(10, self._timeoutFunc)

    def flushFile(self):
        self.gzfile.flush()
        with open(self.output_file, "a") as fp:
            os.fsync(fp.fileno())
        print "Cleared queue; ack'ing"
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

