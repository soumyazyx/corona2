import datetime


def print_info(msg):
    print(f'{datetime.datetime.now()}: [I] {msg}')


def print_error(msg):
    print(f'{datetime.datetime.now()}: [E] {msg}')


def print_warn(msg):
    print(f'{datetime.datetime.now()}: [W] {msg}')
