package batchcontrol.services;

/**
 * This service is started from JBoss microcontainer when JBoss is started.
 * The declaration is in JBOSS_HOME/server/default/deploy/batchcontrol-jboss-beans.xml
 * jboss-beans.xml can contains any parameters that are then passed to the service constructor. 
 * 
 * Parameters are declared inside bean element, for example: 
 * <constructor> 
 * 	<parameter>value1</parameter>
 * 	<parameter>value1</parameter> 
 * </constructor>
 * 
 * Schedulers are started automatically when JBoss starts and stopped gracefully when JBoss stops.
 * (server will wait for all task to complete before stopping).
 *
 * org.jboss.system.ServiceMBeanSupport provides two methods:
 *
 * startService()
 * stopService()	
 *
 * These methods are called after server starts and before server stops. 
 * The calling thread can be put on hold using wait() method. 
 * stopService() method completes only after all tasks have notified shutdown thread that it can proceed.
 *
 * The service itself should be deployed as a SAR file with the following minimum deployment descriptor jboss-service.xml:
 * 
 *  <server>
 *    <mbean code="com.modasolutions.batchcontrol.services.BatchControlService" 
 *           name="modasolutions:service=BatchControlService"/>
 *  </server>
 *
 * @author skonov
 *
 */
public class BatchControlService {

	private BatchControlScheduleSupport service;

	/**
	 * Additional parameters can be passed to the constructor, 
	 * they are declared in batchcontrol-jboss-beans.xml
	 */
	public BatchControlService() {
	}

	public void stop() throws Exception {
		service.stopService();
	}

	/**
	 * Start the service by starting all the batches.
	 * 
	 * @throws Exception
	 *             if an error occurs when batches are created.
	 */
	public void start() throws Exception {
		service = new BatchControlScheduleSupport(BatchControlImpl.getInstance());
		service.startService();
	}

}
