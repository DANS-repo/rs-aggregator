package nl.knaw.dans.rs.aggregator.main;

import nl.knaw.dans.rs.aggregator.schedule.JobScheduler;
import nl.knaw.dans.rs.aggregator.sync.SyncMaster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.FileSystemXmlApplicationContext;

/**
 * Created on 2017-05-04 16:47.
 */
public class SyncApp {

  public static final String APP_CONTEXT_LOCATION = "cfg/syncapp-context.xml";
  public static final String BN_JOB_SCHEDULER = "job-scheduler";
  public static final String BN_SYNC_MASTER = "sync-master";

  private static Logger logger = LoggerFactory.getLogger(SyncApp.class);

  public static void main(String[] args) throws Exception {
    String appContextLocation;
    if (args.length > 0) {
      appContextLocation = args[0];
    } else {
      appContextLocation = APP_CONTEXT_LOCATION;
    }
    logger.info("Configuration file: {}", appContextLocation);

    JobScheduler scheduler;
    SyncMaster syncMaster;
    try (FileSystemXmlApplicationContext applicationContext = new FileSystemXmlApplicationContext(appContextLocation)) {

      scheduler = (JobScheduler) applicationContext.getBean(BN_JOB_SCHEDULER);
      syncMaster = (SyncMaster) applicationContext.getBean(BN_SYNC_MASTER);
      applicationContext.close();
    } catch (Exception e) {
      logger.error("Could not configure from {}: ", appContextLocation, e);
      throw e;
    }

    try {
      scheduler.schedule(syncMaster);
    } catch (Exception e) {
      logger.error("Last error caught: ", e);
      throw e;
    }
  }

}