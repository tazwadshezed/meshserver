import hashlib


def crypt(salt='', digest='sha512'):
    """
    Returns a function that will encrypt
    data using salt and digest.
    """
    def _crypt(data):
        alg = hashlib.new(digest.lower())
        alg.update(str(salt)+str(data))
        return alg.hexdigest()
    return _crypt
