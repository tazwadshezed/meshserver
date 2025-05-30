def list_pformatter(l):
    """
    A pretty formatter for lists that skips large
    elements in the list from being printed.
    """
    pp = ""
    for i in range(len(l)):
        if isinstance(l[i], (list, tuple)):
            pp += list_pformatter(l[i])
        elif isinstance(l[i], str) and len(l[i]) < 50:
            pp += '[%s]' % (l[i])
        elif isinstance(l[i], str):
            pp += '[...%s...]' % (len(l[i]))
        else:
            pp += '[%s]' % (l[i])
    return pp

def dict_pformatter(d, lvl=1):
    """
    A pretty formatter for dicts that skips large
    elements in the dict from being printed
    """
    pp = ""

    for k,v in sorted(d.items()):
        line = '    '*lvl
        if isinstance(v, dict):
            line += '%s: =>' % (k)
            pp += line + '\n'
            pp += dict_pformatter(v, lvl+1)
        else:
            line += '%s: %s' % (k, v)
            pp += line + '\n'

    return pp
