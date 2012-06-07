import hashlib
import simplejson as json
import pika
import time
from ezrcluster.config import SH_DIR, INSTANCES
from ezrcluster.core import *

class Launcher():
    def __init__(self, instances=None):
        if instances is None:
            instances=INSTANCES
        self.conn = pika.BlockingConnection(pika.ConnectionParameters(host=config.get('mq', 'host')))
        self.channel = self.conn.channel()
        self.channel.queue_declare(queue=config.get('mq','job_queue'), durable=True)

        self.instances=instances
        self.jobs = []
        self.application_script_file = None

    def is_ssh_running(self, instance):
        host_str = '%s@%s' % (config.get('ssh', 'user'), instance)
        ret_code = subprocess.call(['ssh', '-o', 'ConnectTimeout=15',
                                    host_str, 'exit'], shell=False)
        return ret_code == 0

    def scp(self, instance, src_file, dest_file):
        dest_str = '%s@%s:%s' % (config.get('ssh', 'user'), instance, dest_file)
        cmds = ['scp', src_file, dest_str]
        subprocess.call(cmds, shell=False)

    def scmd(self, instance, cmd, use_shell=False, remote_output_file='/dev/null'):
        host_str = '%s@%s' % (config.get('ssh', 'user'), instance)
        cstr = '""nohup %s >> %s 2>> %s < /dev/null &""' %\
               (cmd, remote_output_file, remote_output_file)
        cmds = ['ssh', host_str, cstr]
        #print ' '.join(cmds)
        proc = subprocess.Popen(cmds, shell=use_shell)
        proc.communicate()
        ret_code = proc.poll()
        return ret_code

    def generate_job_id(self):
        s = '%0.6f' % time.time()
        md5 = hashlib.md5()
        md5.update(s)
        return md5.hexdigest()

    def add_batch_job(self, cmds, num_cpus=1, expected_runtime=-1, log_file_template=None, output_file=None):
        """ Adds a job to the local queue, job will be posted to SQS queue with call to post_jobs """
        j = Job(cmds, num_cpus=num_cpus, expected_runtime=expected_runtime, log_file_template=log_file_template,
            output_file=output_file)
        self.jobs.append(j)

    def add_job(self, cmds, num_cpus=1, expected_runtime=-1, log_file_template=None, output_file=None, batch_id=None):
        """ Skips the local queue and posts job directly to SQS queue """
        j = Job(cmds, num_cpus=num_cpus, expected_runtime=expected_runtime, log_file_template=log_file_template,
            output_file=output_file)
        j.batch_id = batch_id
        self.post_job(j)

    def post_jobs(self, batch_id=None):
        """ Takes jobs in local queue and posts them to SQS queue """
        if batch_id is None:
            batch_id = random_string(10)
        for k,j in enumerate(self.jobs):
            j.batch_id = batch_id
            self.post_job(j, id=self.generate_job_id())

    def post_job(self, j, id=None):
        """ Posts a single job to SQS queue """
        if id is None:
            id = self.generate_job_id()
        j.id = id
        ji = j.to_dict()
        ji['batch_id'] = str(j.batch_id)
        self.channel.basic_publish(exchange='', routing_key=config.get('mq','job_queue'), body=json.dumps(ji),
            properties=pika.BasicProperties(
                delivery_mode = 2, # make message persistent
            ))

    def close(self):
        self.conn.close()

    def set_application_script(self, file_name):
        self.application_script_file = file_name

    def start_instances(self, timeout_after=1800):
        """ Starts the number of instances specified in the constructor.

            The steps it takes to do this are as follows:
            1) Start each instance with the specified security group, keypair, and insatnce type.
            2) Wait for each instance to be in a running state with a working SSH connection
            3) Initialize each instance by copying over some files and running a script (see initialize_instance)
        """

        instances_pending = zip(range(len(self.instances)),copy(self.instances))
        print 'Starting %d instances...' % len(self.instances)
        start_time = time.time()
        timed_out = False
        while len(instances_pending) > 0 and not timed_out:
            for k,(inst_id,inst) in enumerate(instances_pending):
                if self.is_ssh_running(inst):
                    self.initialize_instance(inst, inst_id)
                    del instances_pending[k]
                    break
            time.sleep(5.0)
            timed_out = (time.time() - start_time) > timeout_after

        if timed_out:
            print 'Timed out! Only %d instances were started...' %\
                  (len(self.instances) - len(instances_pending))

    def fill_template_and_scp(self, instance, params, template_file, dest_file):
        tpl = ScriptTemplate(template_file)
        str = tpl.fill(params)
        tfile = tempfile.NamedTemporaryFile('w', delete=False)
        tfile.write(str)
        tfile.close()
        self.scp(instance, tfile.name, dest_file)
        os.remove(tfile.name)

    def initialize_instance(self, instance, instance_id):
        """ Initialize a started instance. T

            The instance is assumed to have python
            installed, as well as a running SSH daemon. All uploaded files are
            stored in /tmp. The following steps are taken:

            3) Send over a shell script to /tmp/start-daemon.sh that sets the right
               environment variables, sends ezrcluster, and then starts
               a daemon.
            4) Start the shell script.
        """

        print 'Initializing instance: %s' % instance

        self.scmd(instance, 'rm /tmp/ezrcluster-daemon.%d.log' % instance_id)
        self.scmd(instance, 'rm /tmp/ezrcluster-daemon-startup.%d.log' % instance_id)
        self.scmd(instance, 'rm /tmp/start-daemon.%d.sh' % instance_id)
        send_self_tgz_to_instance(instance)

        params = {'USER': config.get('ssh','user'), 'HOST': config.get('ssh','host'),
                  'INSTANCE_ID': '%d' % instance_id}

        self.fill_template_and_scp(instance, params,
            os.path.join(SH_DIR, 'start-daemon.sh'),
            '/tmp/start-daemon.%d.sh' % instance_id)
        self.scmd(instance, 'chmod 777 /tmp/start-daemon.%d.sh' % instance_id)

        if self.application_script_file is not None:
            self.fill_template_and_scp(instance, params,
                self.application_script_file,
                '/tmp/application-script.sh')
            self.scmd(instance, 'chmod 777 /tmp/application-script.sh')

        self.scmd(instance, '/tmp/start-daemon.%d.sh' % instance_id, remote_output_file='/tmp/ezrcluster-daemon-startup.%s.log' % instance_id)

    def num_instances_running(self, host):
        host_str = '%s@%s' % (config.get('ssh', 'user'), host)
        cstr = 'ps -ewwo pid,args | grep "[p]ython /tmp/ezrcluster" | wc -l'
        cmds = ['ssh', host_str, cstr]
        p = subprocess.Popen(cmds, stdout=subprocess.PIPE)
        out, err = p.communicate()
        return int(out)

    def launch(self):
        self.post_jobs()
        self.start_instances()
        self.close()

