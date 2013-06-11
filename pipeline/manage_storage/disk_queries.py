import sys
import subprocess

def disk_available(path):
    df = subprocess.Popen(["df", path], stdout=subprocess.PIPE)
    output = df.stdout.read()
    report = output.split("\n")[2].split()
    return report[0]

def disk_usage(path):
    du = subprocess.Popen(["du", "-cs", path], stdout=subprocess.PIPE)
    output = du.stdout.read().split("\n")
    result = output[-2].split()
    return result[0]

if __name__ == '__main__':
    print disk_usage(sys.argv[1])
