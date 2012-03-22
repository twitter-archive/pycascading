package com.twitter.pycascading;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import org.python.core.Py;
import org.python.core.PyFunction;
import org.python.core.PyObject;
import org.python.core.PySystemState;
import org.python.util.PythonInterpreter;

public class PythonObjectOutputStream extends ObjectOutputStream {

  private OutputStream outStream;

  public PythonObjectOutputStream(OutputStream out) throws IOException {
    super(out);
    outStream = out;
    super.enableReplaceObject(true);
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
    System.out.println("*** replace " + obj + " " + obj.getClass());
    if (obj instanceof PyFunction) {
      PyFunction func = (PyFunction) obj;
      System.out.println("*** NO REPLACE " + obj + " " + obj.getClass());
      // System.out.println("*** eval: " + Py.getSystemState());
      PythonInterpreter interpreter = Main.getInterpreter();
      System.out.println("*** INT: " + interpreter);
      System.out.println("*** _python_function_to_java: " + func.func_code);
      // System.out.println("*** 1 "
      // +
      // interpreter.getSystemState().__getitem__(Py.newString("random_pipe_name")));
      interpreter.exec("print 'hi', " + func.func_code);
      return "ok";
    } else
      return super.replaceObject(obj);
  }
  // @Override
  // protected void writeObjectOverride(Object object) throws IOException {
  // super.writeObjectOverride(object);
  // System.out.println("*** writing object: " + object);
  // }

}
