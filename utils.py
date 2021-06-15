def is_yaml(name):
    if name.strip().split('.')[-1] == 'yml' or name.strip().split('.')[-1] == 'yaml':
        return True
    else:
        return False