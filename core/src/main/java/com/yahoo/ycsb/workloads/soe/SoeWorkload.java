package com.yahoo.ycsb.workloads.soe;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.workloads.CoreWorkload;
import com.yahoo.ycsb.generator.soe.MemcachedGenerator;
import com.yahoo.ycsb.WorkloadException;

import java.util.Properties;

/**
 * Created by oleksandr.gyryk on 3/20/17.
 */
public class SoeWorkload extends CoreWorkload {



  public static final String STORAGE_HOST = "soe_storage_host";
  public static final String STORAGE_HOST_DEFAULT = "localhost";
  public static final String STORAGE_PORT = "soe_storage_port";
  public static final String STORAGE_PORT_DEFAULT = "8000";
  public static final String TOTAL_DOCS = "soe_total_docs";
  public static final String TOTAL_DOCS_DEFAULT = "1000000";



  @Override
  public Object initThread(Properties p, int mythreadid, int threadcount) throws WorkloadException {

    String memHost = p.getProperty(STORAGE_HOST, STORAGE_HOST_DEFAULT);
    String memPort = p.getProperty(STORAGE_PORT, STORAGE_PORT_DEFAULT);
    String totalDocs = p.getProperty(TOTAL_DOCS, TOTAL_DOCS_DEFAULT);
    try {
      return new MemcachedGenerator(memHost, memPort, totalDocs);
    } catch (Exception e) {
      System.err.println("Memcached generator init failed " + e.getMessage());
      throw new WorkloadException();
    }
  }


  @Override
  public void init(Properties p) throws WorkloadException {
    super.init(p);
    operationchooser = createOperationGenerator(p);
  }


  @Override
  public boolean doInsert(DB db, Object threadstate) {
    Status status;
    status = db.soeLoad((MemcachedGenerator) threadstate);
    return null != status && status.isOk();
  }

}
