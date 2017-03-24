package com.yahoo.ycsb.generator.soe;


public class MemcachedGenerator extends Generator {

  private int _counter = 0;

  public void MemcachedGenerator(int offset, String memHost, String memPort) {
    _counter = offset;
    _counter ++;
    _initMemched(memHost, memPort);
  }

  private void _initMemched(String memHost, String memPort) {

  }

  @Override
  protected  String setVal(String key) {
    return null;
  }

  @Override
  protected String getVal(String key) {
    return null;
  }

  @Override
  protected int incremetInsertCounter() {
    _counter ++;
    return _counter;
  }

}
