import os
import string
import random
import tempfile
import subprocess
from copy import copy

from ezrcluster.config import config, ROOT_DIR

def random_string(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for x in range(size))

def create_self_tgz():
    base_dir = os.path.abspath(os.path.join(ROOT_DIR, '..'))
    (tfd, temp_name) = tempfile.mkstemp(suffix='.tgz', prefix='ezrcluster-')
    ret_code = subprocess.call(['tar', 'cvzf', temp_name,
                                '-C', base_dir,
                                '--exclude=.git',
                                'ezrcluster'])
    if ret_code != 0:
        print 'create_self_tgz failed for some reason, ret_code=%d' % ret_code
    return temp_name

def send_self_tgz_to_instance(instance):
    tgz_file = create_self_tgz()
    dest_str = '%s@%s:/tmp/ezrcluster.tgz' % (config.get('ssh', 'user'), instance)
    cmds = ['scp', tgz_file, dest_str]
    subprocess.call(cmds, shell=False)
    os.remove(tgz_file)

class Job():
    def __init__(self, cmds, num_cpus, expected_runtime, log_file_template='job_%d.log', output_file=None):
        self.id = None
        self.cmds = cmds
        self.num_cpus = num_cpus
        self.expected_runtime = expected_runtime
        self.log_file_template = log_file_template
        self.output_file = output_file
        self.process = None
        self.method=None

    def to_dict(self):
        return {'type':'job',
                'id':self.id,
                'command':self.cmds,
                'num_cpus':self.num_cpus,
                'expected_runtime':self.expected_runtime,
                'log_file_template':self.log_file_template,
                'output_file':self.output_file}

    def run(self, output_dir):
        log_file = os.path.join(output_dir, self.log_file_template)
        print 'Opening log file: %s' % log_file
        fd = open(log_file, 'w')
        self.fd = fd
        self.log_file = log_file
        self.process=subprocess.Popen(self.cmds, shell=False, stdout=fd, stderr=fd)


def job_from_dict(ji):
    id = ji['id']
    command = ji['command']
    num_cpus = int(ji['num_cpus'])
    expected_runtime = ji['expected_runtime']
    log_file_template = ji['log_file_template']
    output_file = ji['output_file']
    batch_id = 'None'
    if 'batch_id' in ji:
        batch_id = ji['batch_id']
    log_file = 'None'
    if 'local_log_file' in ji:
        log_file = ji['local_log_file']
    j = Job(command, num_cpus, expected_runtime, log_file_template=log_file_template, output_file=output_file)
    j.id = id
    j.batch_id = batch_id
    j.log_file = log_file
    return j

class ScriptTemplate:

    def __init__(self, fname):

        if not os.path.isfile(fname):
            print 'Template file does not exist: %s' % fname
            return
        f = open(fname, 'r')
        self.template = f.read()
        f.close()

    def fill(self, params):

        script_str = self.template
        for name,val in params.iteritems():
            nStr = '#%s#' % name
            script_str = string.replace(script_str, nStr, str(val))

        return script_str
