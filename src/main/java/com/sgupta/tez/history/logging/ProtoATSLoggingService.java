package com.sgupta.tez.history.logging;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.logging.ats.ATSHistoryLoggingService;
import org.apache.tez.dag.history.logging.proto.ProtoHistoryLoggingService;

/**
 * The logging service for Tez framework to log events to both in ATS and protobuf.
 * Add the following to tez-site.xml to enable this service.
 * <property>
 *   <name>tez.history.logging.service.class</name>
 *   <value>com.sgupta.tez.history.logging.ProtoATSLoggingService</value>
 *  </property>
 */
public class ProtoATSLoggingService extends ProtoHistoryLoggingService {

  private final ATSHistoryLoggingService atsHistoryLoggingService;

  public ProtoATSLoggingService() {
    super();
    atsHistoryLoggingService = new ATSHistoryLoggingService();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    atsHistoryLoggingService.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    atsHistoryLoggingService.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
    atsHistoryLoggingService.serviceStop();
  }

  @Override
  public void setAppContext(AppContext appContext) {
    super.setAppContext(appContext);
    atsHistoryLoggingService.setAppContext(appContext);
  }

  @Override
  public void handle(DAGHistoryEvent event) {
    super.handle(event);
    atsHistoryLoggingService.handle(event);
  }
}
