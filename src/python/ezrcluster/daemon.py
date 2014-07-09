import argparse
import logging
import socket
from threading import Thread
import pika
import simplejson as json
import time
from ezrcluster.core import *

class Daemon(Thread):
    def __init__(self, output_dir, instance_id):
        Thread.__init__(self)
        self.job=None
        self.broken=False
        self.logger = logging.getLogger('daemon')
        self.logger.setLevel(logging.DEBUG)

        self.logger.debug('Initializing daemon...')
        self.instance_id=instance_id
        log_file = os.path.join(output_dir, 'ezrcluster-daemon.%s.log' % self.instance_id)
        lh = logging.FileHandler(log_file, mode='w')
        self.logger.addHandler(lh)

    def init_connection(self):
        #connect to MQ
        self.conn = pika.BlockingConnection(pika.ConnectionParameters(host=config.get('mq', 'host'), socket_timeout=1200))
        self.channel = self.conn.channel()
        self.channel.queue_declare(queue=config.get('mq', 'job_queue'), durable=True)
        self.channel.basic_qos(prefetch_count=1)

    def poll_for_job(self):
        for (method, properties, body) in self.channel.consume(config.get('mq', 'job_queue')):
            self.run_job(method, properties, body)
            break
        self.channel.basic_cancel(self.channel._generator)
        if self.channel._generator_messages:
            # Get the last item
            (method, properties, body) = self.channel._generator_messages.pop()
            messages = len(self.channel._generator_messages)
            self.channel.basic_nack(method.delivery_tag, multiple=True, requeue=True)
        self.channel._generator = None

    def run(self):
        self.init_connection()

        self.logger.info('Daemon initialized and started')
        self.logger.info('job queue name: %s' % config.get('mq','job_queue'))

        self.logger.debug('Starting daemon...')

        while not self.broken:
            try:
                if self.job is None:
                    self.poll_for_job()
                else:
                    self.monitor_job()
                self.channel.connection.process_data_events()
            except SystemExit:
                self.channel.stop_consuming()
                break
            except pika.exceptions.AMQPConnectionError:
                self.logger.error('Server went away, reconnecting..')
                self.init_connection()
            except pika.exceptions.ChannelClosed:
                self.logger.error('Server went away, reconnecting..')
                self.init_connection()
            except socket.error:
                self.logger.error('Server went away, reconnecting..')
                self.init_connection()
            except Exception:
                self.logger.exception("Caught unexpected exception")
                self.init_connection()
            time.sleep(10)
        if self.broken:
            self.channel.cancel()

    def run_job(self, method, properties, body):
        """ Run an individual job from the SQS queue. """

        job_info = json.loads(body)
        self.job = job_from_dict(job_info)
        self.job.batch_id = job_info['batch_id']
        self.logger.debug('Starting job from batch %s with id %s' % (self.job.batch_id, self.job.id))
        self.job.method=method
        self.job.run(output_dir)

        self.logger.debug('Job log file: %s' % self.job.log_file)
        self.logger.debug('Job command: %s' % ' '.join(self.job.cmds))

    def monitor_job(self):
        if self.job.process is not None:
            self.job.process.poll()
            if self.job.process.returncode is not None:
                self.finalize_job()

    def finalize_job(self):
        self.logger.debug('Process finished')

        if os.path.exists(self.job.output_file):
            #copy logfile to data
            (rootdir, log_filename) = os.path.split(self.job.log_file)
            dataserver=config.get('ssh', 'data_server')
            port=config.get('ssh','port')
            dest_dir=config.get('ssh','log_dir')
            user=config.get('ssh','user')
            dest_file=os.path.join(dest_dir,log_filename)

            remote_cmd_str = '(echo cd %s; echo put %s; echo quit)' % (dest_dir, self.job.log_file)
            cmds = ['%s | sftp -P %s -b - %s@%s' % (remote_cmd_str, port, user, dataserver)]
            ret_code=subprocess.call(cmds, shell=True)
            if ret_code != 0:
                if ret_code < 0:
                    self.logger.debug('Log transfer of %s killed by signal %d' % (self.job.log_file,ret_code))
                else:
                    self.logger.debug('Log transfer of %s failed with return code %d' % (self.job.log_file,
                                                                                         ret_code))
            else:
                self.logger.debug('Copied log file from %s to sftp://%s@%s:%s/%s' % (self.job.log_file, user,
                                                                                     dataserver, port,
                                                                                     dest_file))

            # remove log file from local machine
            os.remove(self.job.log_file)

            # copy output file to data
            if self.job.output_file:
                (rootdir, output_filename) = os.path.split(self.job.output_file)
                dest_dir=config.get('ssh','output_dir')
                dest_file=os.path.join(dest_dir,output_filename)

                remote_cmd_str = '(echo cd %s; echo put %s; echo quit)' % (dest_dir, self.job.output_file)
                cmds = ['%s | sftp -P %s -b - %s@%s' % (remote_cmd_str, port, user, dataserver)]
                ret_code=subprocess.call(cmds, shell=True)
                if ret_code != 0:
                    if ret_code < 0:
                        self.logger.debug('Output transfer of %s killed by signal %d' % (self.job.output_file,ret_code))
                    else:
                        self.logger.debug('Output transfer of %s failed with return code %d' % (self.job.output_file,
                                                                                             ret_code))
                else:
                    self.logger.debug('Copied output file from %s to sftp://%s@%s:%s/%s' % (self.job.output_file,
                                                                                            user, dataserver,
                                                                                            port, dest_file))

                # remove output file from local machine
                os.remove(self.job.output_file)
            else:
                self.logger.debug('** Job had no output file set: %s' % self.job.log_file)

            self.channel.basic_ack(delivery_tag = self.job.method.delivery_tag)
        else:
            self.logger.debug('** Output file not found: %s' % self.job.output_file)
            self.broken=True
        self.job=None

if __name__=='__main__':
    ap = argparse.ArgumentParser(description='Run the daemon')
    ap.add_argument('--num_instances', type=int, default=1, help='Number of instances to start')

    argvals = ap.parse_args()

    # The daemon runs on a working instance. It collects jobs from the message queue and runs them.
    output_dir = '/tmp'

    for i in range(argvals.num_instances):
        daemon=Daemon(output_dir, i)
        daemon.start()



