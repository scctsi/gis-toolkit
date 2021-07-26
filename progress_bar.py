# REF: https://gist.github.com/vladignatyev/06860ec2040cb497f0f3
import sys


def progress(count, total, status=''):
    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)

    print('[%s] %s%s ...%s\r' % (bar, percents, '%', status))
