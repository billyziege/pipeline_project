import re
import sys
import subprocess
def check_if_single_job_running_on_system(job_id):
    command = ["qstat", "-j", job_id]
    proc = subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    out = proc.stdout.read()
    if re.search('Following jobs do not exist', out):
        return False
    return True

if __name__ == '__main__':
    print check_if_single_job_running_on_system(sys.argv[1])
