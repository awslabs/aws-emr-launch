def str2bool(v):
    return v.lower() in ("yes", "true", "t", "1")


def return_message(code=0, step_ids=None, message='', cluster_id=''):
    return {'Code': code, 'StepIds': step_ids, 'Message': message, 'ClusterId': cluster_id}
