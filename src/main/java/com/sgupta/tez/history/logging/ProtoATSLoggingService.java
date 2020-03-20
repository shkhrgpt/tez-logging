package com.sgupta.tez.history.logging;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.LifecycleEvent;
import org.apache.hadoop.service.ServiceStateChangeListener;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.logging.HistoryLoggingService;
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
public class ProtoATSLoggingService extends HistoryLoggingService {

  private final ATSHistoryLoggingService atsHistoryLoggingService;
  private final ProtoHistoryLoggingService protoHistoryLoggingService;

  public ProtoATSLoggingService() {
    super(ProtoATSLoggingService.class.getName());
    atsHistoryLoggingService = new ATSHistoryLoggingService();
    protoHistoryLoggingService = new ProtoHistoryLoggingService();
  }

  @Override
  public void setAppContext(AppContext appContext) {
    atsHistoryLoggingService.setAppContext(appContext);
    protoHistoryLoggingService.setAppContext(appContext);
  }

  @Override
  public void handle(DAGHistoryEvent event) {
    atsHistoryLoggingService.handle(event);
    protoHistoryLoggingService.handle(event);
  }

  @Override
  public synchronized STATE getFailureState() {
    return protoHistoryLoggingService.getFailureState();
  }

  @Override
  public void init(Configuration conf) {
    atsHistoryLoggingService.init(conf);
    protoHistoryLoggingService.init(conf);
  }

  @Override
  public void start() {
    atsHistoryLoggingService.start();
    protoHistoryLoggingService.start();
  }

  @Override
  public void stop() {
    atsHistoryLoggingService.stop();
    protoHistoryLoggingService.stop();
  }

  @Override
  public void registerServiceListener(ServiceStateChangeListener l) {
    atsHistoryLoggingService.registerServiceListener(l);
    protoHistoryLoggingService.registerServiceListener(l);
  }

  @Override
  public void unregisterServiceListener(ServiceStateChangeListener l) {
    atsHistoryLoggingService.unregisterServiceListener(l);
    protoHistoryLoggingService.unregisterServiceListener(l);
  }

  @Override
  public String getName() {
    return protoHistoryLoggingService.getName();
  }

  @Override
  public synchronized Configuration getConfig() {
    return protoHistoryLoggingService.getConfig();
  }

  @Override
  public long getStartTime() {
    return protoHistoryLoggingService.getStartTime();
  }

  @Override
  public synchronized List<LifecycleEvent> getLifecycleHistory() {
    return protoHistoryLoggingService.getLifecycleHistory();
  }

  @Override
  public String toString() {
    return protoHistoryLoggingService.toString();
  }

  @Override
  public void removeBlocker(String name) {
    atsHistoryLoggingService.removeBlocker(name);
    protoHistoryLoggingService.removeBlocker(name);
  }

  @Override
  public Map<String, String> getBlockers() {
    return protoHistoryLoggingService.getBlockers();
  }
}
