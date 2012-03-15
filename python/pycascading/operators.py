
import cascading.tuple.Fields

from pycascading.pipe import Apply
from pycascading.decorators import map


def Map(*args):
    input_selector = cascading.tuple.Fields.ALL
    if len(args) == 2:
        (function, output_field) = args
    elif len(args) == 3:
        (input_selector, function, output_field) = args
    else:
        raise Exception('Map needs to be called with 2 or 3 parameters')
    df = map(produces=output_field)(function)
    return Apply(input_selector, df, cascading.tuple.Fields.ALL)


#def Project():
