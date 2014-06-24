import hashlib
import simplejson as json
import pika
import time
from ezrcluster.config import SH_DIR, NODES
from ezrcluster.core import *

class Launcher():
    def __init__(self, nodes=None):
        if nodes is None:
            nodes=NODES
        print('connecting to %s' % config.get('mq', 'host'))
        self.conn = pika.BlockingConnection(pika.ConnectionParameters(host=config.get('mq', 'host')))
        self.channel = self.conn.channel()
        self.channel.queue_declare(queue=config.get('mq','job_queue'), durable=True)

        self.nodes=nodes
        self.jobs = []
        self.application_script_file = None

    def is_ssh_running(self, host):
        host_str = '%s@%s' % (config.get('ssh', 'user'), host)
        ret_code = subprocess.call(['ssh', '-o', 'ConnectTimeout=15',
                                    host_str, 'exit'], shell=False)
        return ret_code == 0

    def scp(self, host, src_file, dest_file):
        dest_str = '%s@%s:%s' % (config.get('ssh', 'user'), host, dest_file)
        cmds = ['scp', src_file, dest_str]
        subprocess.call(cmds, shell=False)

    def scmd(self, host, cmd, use_shell=False, remote_output_file='/dev/null'):
        host_str = '%s@%s' % (config.get('ssh', 'user'), host)
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

    def start_nodes(self, timeout_after=1800):
        """ Starts the number of instances specified in the constructor.

            The steps it takes to do this are as follows:
            1) Start each instance with the specified security group, keypair, and insatnce type.
            2) Wait for each instance to be in a running state with a working SSH connection
            3) Initialize each instance by copying over some files and running a script (see initialize_instance)
        """

        nodes_pending = copy(self.nodes)
        print 'Starting %d nodes...' % len(self.nodes)
        start_time = time.time()
        timed_out = False
        while len(nodes_pending) > 0 and not timed_out:
            for host,num_instances in nodes_pending.iteritems():
                if self.is_ssh_running(host):
                    self.initialize_node(host, num_instances=num_instances)
                    del nodes_pending[host]
                    break
            time.sleep(5.0)
            timed_out = (time.time() - start_time) > timeout_after

        if timed_out:
            print 'Timed out! Only %d nodes were started...' %\
                  (len(self.nodes) - len(nodes_pending))

    def fill_template_and_scp(self, host, params, template_file, dest_file):
        tpl = ScriptTemplate(template_file)
        str = tpl.fill(params)
        tfile = tempfile.NamedTemporaryFile('w', delete=False)
        tfile.write(str)
        tfile.close()
        self.scp(host, tfile.name, dest_file)
        os.remove(tfile.name)

    def initialize_node(self, host, num_instances=1):
        """ Initialize a started node.

            The host is assumed to have python
            installed, as well as a running SSH daemon. All uploaded files are
            stored in /tmp. The following steps are taken:

            3) Send over a shell script to /tmp/start-daemon.sh that sets the right
               environment variables, sends ezrcluster, and then starts
               a daemon.
            4) Start the shell script.
        """

        print 'Initializing node: %s' % host

        self.scmd(host, 'rm /tmp/ezrcluster-daemon.*.log')
        self.scmd(host, 'rm /tmp/ezrcluster-host-startup.log')
        self.scmd(host, 'rm /tmp/start-daemon.sh')
        send_self_tgz_to_instance(host)

        params = {'USER': config.get('ssh','user'), 'HOST': config.get('ssh','host'), 'PORT': config.get('ssh','port'),
                  'NUM_INSTANCES': num_instances}

        self.fill_template_and_scp(host, params, os.path.join(SH_DIR, 'start-daemon.sh'), '/tmp/start-daemon.sh')
        self.scmd(host, 'chmod 777 /tmp/start-daemon.sh')

        if self.application_script_file is not None:
            self.fill_template_and_scp(host, params, self.application_script_file, '/tmp/application-script.sh')
            self.scmd(host, 'chmod 777 /tmp/application-script.sh')

        self.scmd(host, '/tmp/start-daemon.sh', remote_output_file='/tmp/ezrcluster-host-startup.log')

    def num_instances_running(self, host):
        host_str = '%s@%s' % (config.get('ssh', 'user'), host)
        cstr = 'pgrep -f "python /tmp/ezrcluster" | wc -l'
        cmds = ['ssh', host_str, cstr]
        p = subprocess.Popen(cmds, stdout=subprocess.PIPE)
        out, err = p.communicate()
        return int(out)

    def kill_all_nodes(self):
        killed=[]
        for host in self.nodes.iterkeys():
            if not host in killed:
                self.kill_node(host)
                killed.append(host)

    def kill_node(self, host):
        host_str = '%s@%s' % (config.get('ssh', 'user'), host)
        cstr = '\'pkill -f "python /tmp/ezrcluster"\''
        cmds = ['ssh', host_str, cstr]
        p = subprocess.Popen(cmds, stdout=subprocess.PIPE)

    def launch(self):
        self.post_jobs()
        self.start_nodes()
        self.close()

