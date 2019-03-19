package batchcontrol.services;

import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * This service is started from JBoss microcontainer when JBoss is started. 
 * The declaration is in JBOSS_HOME/server/default/deploy/batchcontrol-jboss-beans.xml.
 * jboss-beans.xml can contains any parameters that are then passed to the service constructor.
 * Parameters are declared inside bean element, for example:
 * <constructor>
 * 	<parameter>value1</parameter>
 * 	<parameter>value1</parameter>
 * </constructor>
 * 
 * @author skonov
 *
 */
public class BatchControlService {
	private static final Logger log = Logger.getLogger(BatchControlService.class);

	private BatchControlServiceImpl servImpl;
	
	/**
	 * Additional parameters can be passed to the constructor, they are declared in batchcontrol-jboss-beans.xml
	 */
	public BatchControlService() {}
	
	public void stop() throws Exception {
		log.info("Stopping service...");
		synchronized(servImpl.getMonitor()) {
			Map<String,DefaultBatch> batches = servImpl.getBatches();

			// mark all tasks as stopped
			log.debug("Stopping service: stopping all tasks...");
			for(Iterator<DefaultBatch> i = batches.values().iterator(); i.hasNext(); ) {
				(i.next()).stopTask();
			}

			// wait for all tasks to finish
			while(servImpl.isRunning()) {
				log.info("Stopping service: waiting for all tasks to complete...");
				servImpl.getMonitor().wait();
			}

			// stop all timers
			log.debug("Stopping service: cancelling all batches...");
			for(Iterator<DefaultBatch> i = batches.values().iterator(); i.hasNext(); ) {
				(i.next()).cancel();
			}
		}
		log.info("Service stopped.");
	}

	/**
	 * Start the service by starting all the batches.
	 * 
	 * @throws Exception if an error occurs when batches are created.
	 */
	public void start() throws Exception {
		log.info("Starting service...");
		try {
			servImpl = BatchControlServiceImpl.getInstance(); 
			synchronized(servImpl.getMonitor()) {
				servImpl.loadAllBatches();
				int countStarted = 0;
				Map<String,DefaultBatch> batches = servImpl.getBatches();
				for(Iterator<DefaultBatch> i = batches.values().iterator(); i.hasNext(); ) {
					DefaultBatch batch = (DefaultBatch)i.next();
					try {
						if(batch.start()) {
							countStarted++;
						}
					} catch (Exception e) {
						log.error("Batch "+batch.getName()+" failed to start: "+e, e);
					}
				}
				log.info("Service started: "+countStarted+" of "+batches.size()+" batches started.");
			}
		} catch (Exception e) {
			log.fatal("Service failed to start: "+e, e);
			throw new Exception("Service failed to start: "+e);
		}
	}
	
}
