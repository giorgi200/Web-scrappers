HEADER = '\033[95m'
VALUE = '\033[94m'
SUCCESS = '\033[92m'
WARNING = '\033[93m'
FAIL = '\033[91m'
ENDC = '\033[0m'
UNDERLINE = '\033[4m'


def ok(text):
    print(SUCCESS+str(text)+ENDC)

def value(text):
    print(VALUE+str(text)+ENDC)

def fail(text):
        print(FAIL+str(text)+ENDC)

def underline(text):
        print(UNDERLINE+str(text)+ENDC)

def warning(text):
    print(WARNING+str(text)+ENDC)


