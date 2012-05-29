import argparse
import logging
import pika
import time
import simplejson as json
from ezrcluster.core import *

logger = logging.getLogger('daemon')
logger.setLevel(logging.DEBUG)

def run_job(ch, method, properties, body):
    """ Run an individual job from the SQS queue. """

    job_info = json.loads(body)
    j = job_from_dict(job_info)
    j.batch_id = job_info['batch_id']
    logger.debug('Starting job from batch %s with id %s' % (j.batch_id, j.id))

    log_file = os.path.join(output_dir, j.log_file_template)
    print 'Opening log file: %s' % log_file
    fd = open(log_file, 'w')
    j.fd = fd
    j.log_file = log_file
    logger.debug('Job log file: %s' % j.log_file)

    subprocess.call(j.cmds, shell=False, stdout=fd, stderr=fd)
    logger.debug('Job command: %s' % ' '.join(j.cmds))
    logger.debug('Process finished')

    #copy logfile to data
    (rootdir, log_filename) = os.path.split(j.log_file)
    dataserver=config.get('ssh', 'data_server')
    dest_dir=config.get('ssh','log_dir')
    dest_file=os.path.join(dest_dir,log_filename)

    remote_cmd_str = '(echo cd %s; echo put %s; echo quit)' % (dest_dir, j.log_file)
    cmds = ['%s | sftp -b - %s@%s' % (remote_cmd_str, config.get('ssh','user'), config.get('ssh', 'data_server'))]
    subprocess.call(cmds, shell=True)
    logger.debug('Copied log file from %s to sftp://%s/%s' % (j.log_file, dataserver, dest_file))

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
        logger.debug('Copied output file from %s to sftp://%s/%s' % (j.output_file, dataserver, dest_file))

        # remove output file from local machine
        os.remove(j.output_file)

    ch.basic_ack(delivery_tag = method.delivery_tag)

if __name__=='__main__':
    ap = argparse.ArgumentParser(description='Run the daemon')
    ap.add_argument('--instance_id', type=str, default='0', help='Instance ID')

    argvals = ap.parse_args()

    # The daemon runs on a working instance. It collects jobs from the message queue and runs them.
    output_dir = '/tmp'
    log_file = os.path.join(output_dir, 'ezrcluster-daemon.%s.log' % argvals.instance_id)
    lh = logging.FileHandler(log_file, mode='w')
    logger.addHandler(lh)

    logger.debug('Initializing daemon...')

    #connect to MQ
    conn = pika.BlockingConnection(pika.ConnectionParameters(host=config.get('mq','host')))
    channel = conn.channel()

    channel.queue_declare(queue=config.get('mq','job_queue'), durable=True)

    logger.info('Daemon initialized and started')
    logger.info('SQS job queue name: %s' % config.get('mq','job_queue'))

    logger.debug('Starting daemon...')
    start_time = time.time()

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(run_job, queue=config.get('mq','job_queue'))
    channel.start_consuming()




