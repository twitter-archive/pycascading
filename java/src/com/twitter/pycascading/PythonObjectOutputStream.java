package com.twitter.pycascading;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import org.python.core.Py;
import org.python.core.PyFunction;
import org.python.core.PyNone;
import org.python.core.PyObject;
import org.python.core.PyString;
import org.python.core.PySystemState;
import org.python.util.PythonInterpreter;

import cascading.pipe.Each;
import cascading.pipe.Pipe;

public class PythonObjectOutputStream extends ObjectOutputStream {

  private OutputStream outStream;
  private PyFunction callBack;

  public PythonObjectOutputStream(OutputStream out, PyFunction callBack) throws IOException {
    super(out);
    outStream = out;
    this.callBack = callBack;
    enableReplaceObject(true);
  }

  // @Override
  // protected void annotateClass(Class<?> cl) throws IOException {
  // super.annotateClass(cl);
  // System.out.println("*** annotate " + cl);
  // }
  //
  // @Override
  // protected void annotateProxyClass(Class<?> cl) throws IOException {
  // super.annotateProxyClass(cl);
  // System.out.println("*** annotatep " + cl);
  // }

  @Override
  protected Object replaceObject(Object obj) throws IOException {
    // System.out.println("*** replace " + obj + "/" + obj.getClass());
    if (obj instanceof PyObject) {
      PyObject replaced = callBack.__call__((PyObject) obj);
      if (!(replaced instanceof PyNone)) {
        System.out.println("######## replaced " + obj + "/" + obj.getClass() + "->" + replaced
                + " " + replaced.getClass());
        // outStream.writeObject(replaced);
        return Py.tojava(replaced, PythonFunctionWrapper.class);
      }
    } else if (obj instanceof Each) {
      System.out.println("******* PIPE FOUND " + obj);
    }
    // outStream.writeObject(obj);
    return obj;
    //
    // String function_source_code = Py.tojava(callBack.__call__(func),
    // String.class);
    //
    // if (obj instanceof PyFunction) {
    // PyFunction func = (PyFunction) obj;
    // String function_source_code = Py.tojava(callBack.__call__(func),
    // String.class);
    // System.out.println("****** GOT IT " + function_source_code.getClass() +
    // function_source_code);
    // System.out.println("*** NO REPLACE " + obj + " " + obj.getClass());
    // // System.out.println("*** eval: " + Py.getSystemState());
    // PythonInterpreter interpreter = Main.getInterpreter();
    // System.out.println("*** INT: " + interpreter);
    // System.out.println("*** _python_function_to_java: " + func.func_code);
    // // System.out.println("*** 1 "
    // // +
    // //
    // interpreter.getSystemState().__getitem__(Py.newString("random_pipe_name")));
    // interpreter.exec("print 'hi " + func.__name__ + "'");
    // return "ok";
    // } else
    // return super.replaceObject(obj);
  }

  // @Override
  // protected void writeObjectOverride(Object object) throws IOException {
  // super.writeObjectOverride(object);
  // System.out.println("*** writing object: " + object);
  // }

}
