# tez-logging
Provides a logging service for Tez to log events in ATS V1 and protobuf

To build: mvn clean package

Update tez-site.xml:
    
    <property>
      <name>tez.history.logging.service.class</name>
      <value>com.sgupta.tez.history.logging.ProtoATSLoggingService</value>
    </property>
    
Also, include the built jar to tez classpath, and restart tez.
