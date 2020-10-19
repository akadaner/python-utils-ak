import psutil

import subprocess
from utils_ak.str import cast_unicode

import re

import pandas as pd


def execute(cmd, run_async=False):
    if run_async:
        process = subprocess.Popen(cmd, shell=True)
    else:
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        output, error = process.communicate()
        return ' '.join([cast_unicode(x) for x in [output, error] if x]), process.returncode


def get_system_usage_stats():
    # todo: add cpu stats
    stats = {}

    # svmem(total=17058336768, available=6641549312, percent=61.1, used=10416787456, free=6641549312)
    memory = psutil.virtual_memory()
    stats['ram'] = {'total': memory.total, 'available': memory.available, 'free': memory.free,
                    'percent': memory.percent}

    # sdiskusage(total=254277836800, used=212004134912, free=42273701888, percent=83.4)
    disk = psutil.disk_usage('/')
    stats['disk'] = {'total': disk.total, 'used': disk.used, 'free': disk.free, 'percent': disk.percent}
    return stats


def get_dfh():
    output = execute("df -h | grep -v 'map '")  # grep -v 'map ' for mac book
    s = output[0]
    s = s.replace('Mounted on', 'Mounted')
    s = s.replace('%iused', 'Use%')  # for mac
    lines = s.strip().split('\n')
    lines = [re.sub('\s+', ' ', line) for line in lines]
    values = [line.split(' ') for line in lines]
    df = pd.DataFrame(values[1:], columns=values[0])
    return df
