import pickle

def encode_obj(obj):
    return pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)

def decode_obj(raw):
    return pickle.loads(raw)
