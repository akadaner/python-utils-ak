import calendar
from datetime import datetime, timedelta


def add_months(dt, n):
    month = dt.month - 1 + n
    year = dt.year + month // 12
    month = month % 12 + 1
    day = min(dt.day, calendar.monthrange(year, month)[1])
    return datetime(year, month, day)


def iter_range_by_months(beg, end):
    while True:
        next_end = add_months(datetime(beg.year, beg.month, 1), 1)  # first day of next month
        next_end = min(next_end, end)
        yield beg, next_end
        beg = next_end

        if next_end == end:
            return


def iter_range(beg, end, period):
    while beg < end:
        next_beg = beg + period
        next_beg = min(next_beg, end)
        yield beg, next_beg
        beg = next_beg


if __name__ == '__main__':
    from utils_ak.time import cast_datetime, cast_timedelta

    # add months
    dt = cast_datetime('2020.01.30')
    for i in range(12):
        print(add_months(dt, i))

    # iter range by months
    print(list(iter_range_by_months(cast_datetime('2020.01.15'),cast_datetime('2020.02.16'))))
    print(list(iter_range_by_months(cast_datetime('2020.01.01'),cast_datetime('2020.06.01'))))

    # iter range
    print(list(iter_range(cast_datetime('2020.01.01 12:00:00'), cast_datetime('2020.01.01 12:16:00'), cast_timedelta('5m'))))