
import time
import delorean


def get_cur_time():
    return time.time()


get_cur_ts = get_cur_time


def midnight(datetime):
    return delorean.Delorean(datetime, timezone="UTC").midnight.replace(tzinfo=None)
