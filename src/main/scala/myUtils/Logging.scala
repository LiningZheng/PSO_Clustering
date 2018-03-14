package myUtils

import org.apache.log4j.{Level, Logger, LogManager, PropertyConfigurator, DailyRollingFileAppender}

/**
  * Created by lining on 8/3/17.
  */
trait Logging {
  @transient private  var  _log:Logger = null

  def log: Logger = {
    if(_log == null ){
      _log = LogManager.getLogger("myLogger")
      val appender = new DailyRollingFileAppender()
      appender.setFile("/home/lining/log/liningPSOClustering.log",false,false,4096)
      _log.addAppender(appender)
    }
      _log
  }
}
