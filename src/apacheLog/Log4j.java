package apacheLog;

import java.util.Properties;

public class Log4j {
	
    
    public Log4j() {
        Properties props = new Properties();
        
        props.put("kafka.logs.dir", "/usr/local/var/log/kafka");
        props.put("log4j.rootLogger", "INFO");

        props.put("log4j.appender.requestAppender.layout", "org.apache.log4j.PatternLayout");
        props.put("log4j.appender.stdout", "org.apache.log4j.ConsoleAppender");
        props.put("log4j.appender.stdout.layout", "org.apache.log4j.PatternLayout");
        props.put("log4j.appender.stdout.layout.ConversionPattern", "[%d] %p %m (%c)%n");

        props.put("log4j.appender.kafkaAppender", "org.apache.log4j.DailyRollingFileAppender");
        props.put("log4j.appender.kafkaAppender.DatePattern", "'.'yyyy-MM-dd-HH");
        props.put("log4j.appender.kafkaAppender.File", "${kafka.logs.dir}/server.log");
        props.put("log4j.appender.kafkaAppender.layout", "org.apache.log4j.PatternLayout");
        props.put("log4j.appender.kafkaAppender.layout.ConversionPattern", "[%d] %p %m (%c)%n");

        props.put("log4j.appender.stateChangeAppender", "org.apache.log4j.DailyRollingFileAppender");
        props.put("log4j.appender.stateChangeAppender.DatePattern", "'.'yyyy-MM-dd-HH");
        props.put("log4j.appender.stateChangeAppender.File", "${kafka.logs.dir}/state-change.log");
        props.put("log4j.appender.stateChangeAppender.layout", "org.apache.log4j.PatternLayout");
        props.put("log4j.appender.stateChangeAppender.layout.ConversionPattern", "[%d] %p %m (%c)%n");

        props.put("log4j.appender.requestAppender", "org.apache.log4j.DailyRollingFileAppender");
        props.put("log4j.appender.requestAppender.DatePattern", "'.'yyyy-MM-dd-HH");
        props.put("log4j.appender.requestAppender.layout.ConversionPattern", "[%d] %p %m (%c)%n");

        props.put("log4j.appender.cleanerAppender", "org.apache.log4j.DailyRollingFileAppender");
        props.put("log4j.appender.cleanerAppender.DatePattern", "'.'yyyy-MM-dd-HH");
        props.put("log4j.appender.cleanerAppender.File", "${kafka.logs.dir}/log-cleaner.log");
        props.put("log4j.appender.cleanerAppender.layout", "org.apache.log4j.PatternLayout");
        props.put("log4j.appender.cleanerAppender.layout.ConversionPattern", "[%d] %p %m (%c)%n");

        props.put("log4j.appender.controllerAppender", "org.apache.log4j.DailyRollingFileAppender");
        props.put("log4j.appender.controllerAppender.DatePattern", "'.'yyyy-MM-dd-HH");
        props.put("log4j.appender.controllerAppender.File", "${kafka.logs.dir}/controller.log");
        props.put("log4j.appender.controllerAppender.layout", "org.apache.log4j.PatternLayout");
        props.put("log4j.appender.controllerAppender.layout.ConversionPattern", "[%d] %p %m (%c)%n");

        props.put("log4j.logger.kafka", "INFO, kafkaAppender");

        props.put("log4j.logger.kafka.network.RequestChannel$", "WARN, requestAppender");
        props.put("log4j.additivity.kafka.network.RequestChannel$", "false");

        props.put("log4j.logger.kafka.request.logger", "WARN, requestAppender");
        props.put("log4j.additivity.kafka.request.logger", "false");

        props.put("log4j.logger.kafka.controller", "TRACE, controllerAppender");
        props.put("log4j.additivity.kafka.controller", "false");

        props.put("log4j.logger.kafka.log.LogCleaner", "INFO, cleanerAppender");
        props.put("log4j.additivity.kafka.log.LogCleaner", "false");

        props.put("log4j.logger.state.change.logger", "TRACE, stateChangeAppender");
        props.put("log4j.additivity.state.change.logger", "false");
    	
    	org.apache.log4j.PropertyConfigurator.configure(props);
    }
}