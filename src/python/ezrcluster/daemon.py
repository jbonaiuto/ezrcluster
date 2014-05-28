import argparse
import logging
import socket
from threading import Thread
import pika
import simplejson as json
from ezrcluster.core import *

class Daemon(Thread):
    def __init__(self, output_dir, instance_id):
        Thread.__init__(self)
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
        self.conn = pika.BlockingConnection(pika.ConnectionParameters(host=config.get('mq', 'host')))
        self.channel = self.conn.channel()
        self.channel.queue_declare(queue=config.get('mq', 'job_queue'), durable=True)

    def run(self):
        self.init_connection()

        self.logger.info('Daemon initialized and started')
        self.logger.info('SQS job queue name: %s' % config.get('mq','job_queue'))

        self.logger.debug('Starting daemon...')

        while not self.broken:
            try:
                self.channel.basic_qos(prefetch_count=1)
                self.channel.basic_consume(self.run_job, queue=config.get('mq','job_queue'))
                self.channel.start_consuming()
            except SystemExit:
                self.channel.stop_consuming()
                break
            except pika.exceptions.AMQPConnectionError:
                self.logger.error('Server went away, reconnecting..')
                self.init_connection()
            except socket.error:
                self.logger.error('Server went away, reconnecting..')
                self.init_connection()
            except Exception:
                self.logger.exception("Caught unexpected exception")
                self.init_connection()
        if self.broken:
            self.channel.stop_consuming()

    def run_job(self, ch, method, properties, body):
        """ Run an individual job from the SQS queue. """

        job_info = json.loads(body)
        j = job_from_dict(job_info)
        j.batch_id = job_info['batch_id']
        self.logger.debug('Starting job from batch %s with id %s' % (j.batch_id, j.id))

        log_file = os.path.join(output_dir, j.log_file_template)
        print 'Opening log file: %s' % log_file
        fd = open(log_file, 'w')
        j.fd = fd
        j.log_file = log_file
        self.logger.debug('Job log file: %s' % j.log_file)

        subprocess.call(j.cmds, shell=False, stdout=fd, stderr=fd)
        self.logger.debug('Job command: %s' % ' '.join(j.cmds))
        self.logger.debug('Process finished')

        if os.path.exists(j.output_file):
            #copy logfile to data
            (rootdir, log_filename) = os.path.split(j.log_file)
            dataserver=config.get('ssh', 'data_server')
            dest_dir=config.get('ssh','log_dir')
            dest_file=os.path.join(dest_dir,log_filename)

            remote_cmd_str = '(echo cd %s; echo put %s; echo quit)' % (dest_dir, j.log_file)
            cmds = ['%s | sftp -b - %s@%s' % (remote_cmd_str, config.get('ssh','user'), config.get('ssh', 'data_server'))]
            subprocess.call(cmds, shell=True)
            self.logger.debug('Copied log file from %s to sftp://%s/%s' % (j.log_file, dataserver, dest_file))

            # remove log file from local machine
            os.remove(j.log_file)

            # copy output file to data
            if j.output_file:
                (rootdir, output_filename) = os.path.split(j.output_file)
                dest_dir=config.get('ssh','output_dir')
                dest_file=os.path.join(dest_dir,output_filename)

                remote_cmd_str = '(echo cd %s; echo put %s; echo quit)' % (dest_dir, j.output_file)
                cmds = ['%s | sftp -b - %s@%s' % (remote_cmd_str, config.get('ssh','user'), config.get('ssh', 'data_server'))]
                subprocess.call(cmds, shell=True)
                self.logger.debug('Copied output file from %s to sftp://%s/%s' % (j.output_file, dataserver, dest_file))

                # remove output file from local machine
                os.remove(j.output_file)

            ch.basic_ack(delivery_tag = method.delivery_tag)
        else:
            self.broken=True

if __name__=='__main__':
    ap = argparse.ArgumentParser(description='Run the daemon')
    ap.add_argument('--num_instances', type=int, default=1, help='Number of instances to start')

    argvals = ap.parse_args()

    # The daemon runs on a working instance. It collects jobs from the message queue and runs them.
    output_dir = '/tmp'

    for i in range(argvals.num_instances):
        daemon=Daemon(output_dir, i)
        daemon.start()



