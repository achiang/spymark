import os

def get_fixture(filename):
    ''' Load a file from the fixtures directory. '''
    path = 'fixtures/' + filename
    if ('tests' in os.listdir('.')):
        path = 'tests/' + path
    return path
