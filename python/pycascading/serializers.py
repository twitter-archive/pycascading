
import inspect

import pipe
from com.twitter.pycascading import PythonFunctionWrapper


def _remove_indents_from_function(code):
    """Remove leading indents from the function's source code.

    Otherwise an exec later when running the function would complain about
    the indents.
    """
    i = 0
    indent = 0
    blanks = -1
    result = ''
    while i < len(code):
        if blanks < 0:
            if code[i] == ' ':
                indent += 1
            elif code[i] == '\t':
                indent += 8
            else:
                result = code[i]
                blanks = indent
        else:
            if blanks >= indent:
                # This is to substitute indenting tabs if necessary
                result += ' ' * (blanks - indent) + code[i]
                blanks = indent
            else:
                if code[i] == ' ':
                    blanks += 1
                elif code[i] == '\t':
                    blanks += 8
                else:
                    # This happens when in one line we have less indent
                    # than in the first, but this should have been caught by
                    # the compiler, so we shouldn't get here ever.
                    raise Exception('Indents mismatch')
            if code[i] == '\n':
                blanks = 0
        i += 1
    return result


def replace_object(obj):
    if inspect.isfunction(obj):
#        print '****** replace_object', obj, inspect.getsource(obj)
        source = _remove_indents_from_function(inspect.getsource(obj))
        wrapped_func = PythonFunctionWrapper(obj, source)
#        print '*** run mode', pipe.config['pycascading.running_mode']
        if pipe.config['pycascading.running_mode'] == 'local':
            wrapped_func.setRunningMode(PythonFunctionWrapper.RunningMode.LOCAL)
        else:
            wrapped_func.setRunningMode(PythonFunctionWrapper.RunningMode.HADOOP)
        return wrapped_func
    else:
        return None


def resolve_object(obj):
    return None
