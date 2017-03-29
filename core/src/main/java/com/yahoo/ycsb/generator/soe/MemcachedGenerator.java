package com.yahoo.ycsb.generator.soe;


import net.spy.memcached.FailureMode;
import net.spy.memcached.internal.OperationFuture;

//import java.net.InetSocketAddress;



/**
 * Created by oleksandr.gyryk on 3/20/17.
 *
 * The storage-based generator is fetching pre-generated values/documents from an internal in-memory database instead
 * of generating new random values on the fly.
 * This approach allows YCSB to operate with real (or real-looking) JSON documents rather then synthetic.
 *
 * It also provides the ability to query rich JSON documents by splitting JSON documents into query predicates
 * (field, value, type, field-value relation, logical operation)
 */
public class MemcachedGenerator extends Generator {

  private net.spy.memcached.MemcachedClient client;

  public MemcachedGenerator(String memHost, String memPort, String totalDocs) throws Exception {
    try {
      String prefix = SOE_DOCUMENT_PREFIX_CUSTOMER + SOE_SYSTEMFIELD_DELIMITER;
      client = createMemcachedClient(memHost, Integer.parseInt(memPort));

      if (client.get(prefix + SOE_SYSTEMFIELD_TOTALDOCS_COUNT) == null){
        client.add(prefix + SOE_SYSTEMFIELD_TOTALDOCS_COUNT, 0, totalDocs);
      }

      if (client.get(prefix + SOE_SYSTEMFIELD_INSERTDOC_COUNTER) == null) {
        client.add(prefix + SOE_SYSTEMFIELD_INSERTDOC_COUNTER, 0, totalDocs);
        client.incr(prefix + SOE_SYSTEMFIELD_INSERTDOC_COUNTER, 1);
      }

      if (client.get(prefix + SOE_SYSTEMFIELD_STORAGEDOCS_COUNT) == null) {
        client.add(prefix + SOE_SYSTEMFIELD_STORAGEDOCS_COUNT, 0, "0");
      }

    } catch (Exception e) {
      System.err.println("Memcached init error" + e.getMessage());
      throw e;
    }

  }

  protected net.spy.memcached.MemcachedClient createMemcachedClient(String memHost, int memPort)
      throws Exception {
    String address = memHost + ":" + memPort;
    return new net.spy.memcached.MemcachedClient(
        new net.spy.memcached.ConnectionFactoryBuilder().setDaemon(true).setFailureMode(FailureMode.Retry).build(),
        net.spy.memcached.AddrUtil.getAddresses(address));
    //return new net.spy.memcached.MemcachedClient(new InetSocketAddress(memHost, memPort));
  }





  @Override
  protected void setVal(String key, String value) {
    try {
      OperationFuture<Boolean> future = client.add(key, 0, value);
    } catch (Exception e) {
      System.err.println("error inserting value to memcached" + e.getMessage());
      throw e;
    }
  }

  @Override
  protected String getVal(String key) {
    try {
      return client.get(key).toString();
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  protected int increment(String key, int step) {
    try {
      return (int) client.incr(key, step);
    } catch (Exception e) {
      System.err.println("Error incrementing a counter in memcached" + e.getMessage());
      throw e;
    }
  }

}
