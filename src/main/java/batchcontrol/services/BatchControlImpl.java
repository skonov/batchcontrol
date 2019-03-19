package batchcontrol.services;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;

import org.apache.log4j.Logger;

import batchcontrol.services.iterators.DailyIterator;
import batchcontrol.services.iterators.FirstBusinessDayOfMonthIterator;
import batchcontrol.services.iterators.HourlyIterator;
import batchcontrol.services.iterators.MinuteIterator;
import batchcontrol.services.iterators.MonthlyIterator;
import batchcontrol.services.iterators.RestrictedDailyIterator;
import batchcontrol.services.iterators.SchedulerIterator;

public class BatchControlImpl implements BatchControl {
	private static final Logger log = Logger.getLogger(BatchControlImpl.class);

	private boolean running;
	private int numOfProcesses;
	private Map<String, DefaultBatch> batches = new Hashtable<String, DefaultBatch>();
	private String host; // final

	/** Scheduler types */
	public static final String DAILY_TYPE = "D";
	public static final String HOURLY_TYPE = "H";
	public static final String RESTRICTED_DAILY_TYPE = "R";
	public static final String MINUTE_TYPE = "M";
	public static final String MONTHLY_TYPE = "O";
	public static final String FIRST_BUSINESS_DAY_OF_MONTH_TYPE = "F";

	private static final String PROPERTY_FILE = "/modasolutions/conf/backendsystem.properties";
	private static final String HOST_PROPERTY = "batches_host_name";
	private static final String JNDI_DB_PROPERTY = "batches_modadb_jndi_name";

	private static String JNDI_DB_NAME;
	
	private List<ProcessListener> processListeners = new ArrayList<ProcessListener>(); 
	
	private static BatchControlImpl instance;

	/**
	 * Returns instance of this service.
	 * 
	 * @return BatchControlServiceImpl instance.
	 * @throws Exception
	 *             if instance could not be created.
	 */
	public static synchronized BatchControlImpl getInstance() throws Exception {
		if (instance == null) {
			try {
				instance = new BatchControlImpl();
			} catch (Exception e) {
				log.error("Failed to create instance: " + e, e);
				throw new Exception("Failed to create instance: " + e.getMessage());
			}
		}
		return instance;
	}

	/**
	 * Private constructor.
	 * 
	 * @throws Exception
	 *             if errors occur when reading properties.
	 */
	private BatchControlImpl() throws Exception {
		FileInputStream fis = null;
		try {
			Properties prop = new Properties();
			fis = new FileInputStream(PROPERTY_FILE);
			prop.load(fis);
			host = prop.getProperty(HOST_PROPERTY);
			JNDI_DB_NAME = prop.getProperty(JNDI_DB_PROPERTY);
		} catch (Exception e) {
			throw e;
		} finally {
			try {
				if (fis != null) 
					fis.close();
			} catch (IOException e) {
				throw e;
			}
		}
	}
	
	public void setProcessListeners(List<ProcessListener> processListeners) {
		this.processListeners = processListeners;
	}
	
	public void reloadBatches() throws Exception {
		if (running == true) {
			log.info("Service is active: " + numOfProcesses + " process(es) running.");
			throw new Exception("Service is active: " + numOfProcesses + " process(es) running.");
		}
		for (Iterator<DefaultBatch> i = batches.values().iterator(); i.hasNext();) {
			DefaultBatch batch = i.next();
			if (batch.getActive() == 1 || batch.getStatus() == 1) {
				log.info("Service is active: " + batch.getName() + " is active or scheduled.");
				throw new Exception("Service is active: " + batch.getName() + " is active or scheduled.");
			}
		}
		// nothing is running, we can reload
		log.debug("Reloading all batches...");
		batches.clear();
		loadAllBatches();
	}

	/**
	 * Loads all batches from database.
	 * 
	 * @throws Exception
	 *             if database error occurs during operation.
	 */
	private void loadAllBatches() throws Exception {
		loadBatch(null);
		log.debug("All batches are loaded.");
	}

	/**
	 * Loads all iterators from database. All previous iterators are removed.
	 * 
	 * @param taskClassName
	 * @throws Exception
	 */
	private void loadBatch(String taskClassName) throws Exception {
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			con = getConnection();
			stmt = con.createStatement();
			String sql = "select b.name, b.status, b.classname, b.active, b.server, "
					+ "s.id as s_id, s.s_type, s.s_hour, s.s_minute, s.s_second, s.s_days "
					+ "from batches b left join schedulers s on b.id=s.batch_id " + "where b.server='" + host + "'";
			if (taskClassName != null) {
				sql += " and b.classname='" + taskClassName + "'";
			}

			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				String name = rs.getString("name");
				String className = rs.getString("classname");
				DefaultBatch batch = (DefaultBatch) batches.get(className);
				if (batch == null) {
					batch = initBatch(className, name, rs.getInt("active"));
				}
				// server can be changed manually in database during runtime
				batch.setServer(rs.getString("server"));

				String schedulerType = rs.getString("s_type");
				// if batch has schedulers
				if (schedulerType != null) {
					int id = rs.getInt("s_id");
					int hour = rs.getInt("s_hour");
					int minute = rs.getInt("s_minute");
					int second = rs.getInt("s_second");
					String s_days = rs.getString("s_days");
					s_days = (s_days == null ? "" : s_days.trim());
					int[] days = new int[s_days.length()];
					for (int i = 0; i < s_days.length(); i++) {
						days[i] = Integer.parseInt(String.valueOf(s_days.charAt(i)));
					}

					SchedulerIterator iterator = null;
					if (schedulerType.equals(DAILY_TYPE)) {
						iterator = new DailyIterator(hour, minute, second, id);
					} else if (schedulerType.equals(HOURLY_TYPE)) {
						iterator = new HourlyIterator(minute, second, id);
					} else if (schedulerType.equals(RESTRICTED_DAILY_TYPE)) {
						iterator = new RestrictedDailyIterator(hour, minute, second, days, id);
					} else if (schedulerType.equals(MINUTE_TYPE)) {
						iterator = new MinuteIterator(second, id);
					} else if (schedulerType.equals(MONTHLY_TYPE)) {
						iterator = new MonthlyIterator(days[0], hour, minute, second, id);
					} else if (schedulerType.equals(FIRST_BUSINESS_DAY_OF_MONTH_TYPE)) {
						iterator = new FirstBusinessDayOfMonthIterator(hour, minute, second, id);
					}
					batch.addSchedulerIterator(iterator);
					log.debug("Iterator was added to " + name + " batch: " + iterator);
				}
			}
			if (taskClassName != null) {
				log.debug("Batch loaded: " + taskClassName);
			}
		} catch (Exception e) {
			log.error("Error loading batches: " + e, e);
			throw new Exception("Error loading batches: " + e);
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (stmt != null)
					stmt.close();
				if (con != null)
					con.close();
			} catch (Exception e) {
				log.error("loadBatches: error closing database objects: " + e, e);
			}
		}
	}

	private Connection getConnection() throws Exception {
		Connection con = null;
		try {
			Context ctx = new InitialContext();
			DataSource ds = (DataSource) ctx.lookup(JNDI_DB_NAME);
			con = ds.getConnection();
		} catch (Exception e) {
			log.error(e.toString());
			throw new Exception("Error getting database connection: " + e, e);
		}
		return con;
	}

	/**
	 * Creates new batch and adds it to the service.
	 * 
	 * @param className
	 *            fully qualified task class name,
	 * @param batchName
	 *            short batch name
	 * @param active
	 *            active flag
	 * @param server
	 *            server name
	 * @return ({@link DefaultBatch}
	 */
	private DefaultBatch initBatch(String className, String batchName, int active) {
		DefaultBatch batch = new DefaultBatch(batchName, host, className);
		batch.setActive(active);
		batches.put(className, batch);
		return batch;
	}

	/**
	 * Cancels batch. If task is still running it is allowed to finish and then
	 * batch is cancelled.
	 * 
	 * @param name
	 *            fully qualified task class name.
	 * @throws Exception
	 *             if another thread interrupted the current thread before or while
	 *             the current thread was waiting for a notification.
	 */
	public void stopBatch(String name) throws Exception {
		DefaultBatch batch = (DefaultBatch) batches.get(name);
		if (batch.getStatus() == 1) {
			batch.stopTask();
			while (batch.getTaskState() == SchedulerTask.BUSY) {
				log.info("STOP: Stopping batch " + name + ": waiting for task to complete...");
				notifyListenersToWait();
			}
			batch.cancel();
			log.info("STOP: Batch stopped: " + name);
		}
	}
	
	private void notifyListenersToWait() throws Exception {
		for (ProcessListener l : processListeners) {
			l.waitForBusyTask();
		}
	}

	/**
	 * This method is called from {@link SchedulerTask} every time the task/process
	 * has finished its single run. It decreases process count by one.
	 */
	public void processStopped() {
		numOfProcesses--;
		if (numOfProcesses == 0) {
			running = false;
			notifyListenersProcessStopped();
		}
		log.debug("Process stopped: number of processes " + numOfProcesses + ", running=" + running);
	}
	
	private void notifyListenersProcessStopped() {
		for (ProcessListener l : processListeners) {
			l.notifyProcessStopped();
		}
	}

	/**
	 * Start batch.
	 * 
	 * @param name
	 *            fully qualified task class name.
	 * @param runOnce
	 *            if true, the batch is scheduled for one-time execution
	 *            immediately, otherwise it uses associated schedulers.
	 * @throws Exception
	 *             if batch fails to start.
	 */
	public void startBatch(String taskClassName, boolean runOnce) throws Exception {
		DefaultBatch batch = (DefaultBatch) batches.get(taskClassName);
		try {
			// clear all iterators they will be all reloaded during load
			batch.clearIterators();
			loadBatch(taskClassName);
			batch.setRunOnce(runOnce);
			String result = batch.start() ? "Batch started: " + batch.getName() : "Batch NOT started: " + taskClassName;
			log.info("START: " + result);
		} catch (Exception e) {
			log.error("START: Batch " + batch.getName() + " failed to start: " + e, e);
			throw new Exception("Batch " + batch.getName() + " failed to start: " + e);
		}
	}

	/**
	 * Updates batch active status in database and in the service. Setting active to
	 * 0 makes batch ineligible to run.
	 * 
	 * @param batchClassName
	 *            fully qualified task class name.
	 * @param active
	 *            active flag: 1 or 0
	 * @throws Exception
	 *             if database error occurs.
	 */
	public void setActive(String batchClassName, int active) throws Exception {
		Connection con = null;
		PreparedStatement stmt = null;
		try {
			con = getConnection();
			String sql = "update batches set active=? where classname=?";
			stmt = con.prepareStatement(sql);
			stmt.setInt(1, active);
			stmt.setString(2, batchClassName);
			stmt.execute();
			DefaultBatch batch = (DefaultBatch) batches.get(batchClassName);
			batch.setActive(active);
			log.info(batchClassName + ": active=" + active);
		} catch (Exception e) {
			log.error("Error setting batch active field: " + e, e);
			throw new Exception("Error setting batch active field: " + e);
		} finally {
			try {
				if (stmt != null)
					stmt.close();
				if (con != null)
					con.close();
			} catch (Exception e) {
				log.error("setActive: error closing database objects: " + e, e);
			}
		}
	}
	
	/**
	 * Returns Map that contains copies of all batches. Each batch is mapped to the
	 * task class name.
	 * 
	 * @return Map of copies of all batches.
	 */
	public Map<String, DefaultBatch> getAllBatches() {
		Map<String, DefaultBatch> copy = new TreeMap<String, DefaultBatch>();
		for (Iterator<String> i = batches.keySet().iterator(); i.hasNext();) {
			String key = i.next();
			DefaultBatch batch = (DefaultBatch) batches.get(key);
			copy.put(key, (DefaultBatch) batch.clone());
		}
		log.debug("Returning copies of all batches.");
		return copy;
	}

	/**
	 * Returns copy of the batch.
	 * 
	 * @param batchId
	 *            fully qualified class name of a batch
	 * @return Copy of batch.
	 */
	public DefaultBatch getBatch(String batchId) {
		return (DefaultBatch) ((DefaultBatch) batches.get(batchId)).clone();
	}

	/**
	 * This method is called from {@link SchedulerTask} every time the task/process
	 * is started. It increases process count by one.
	 */
	public void processStarted() {
		running = true;
		numOfProcesses++;
		log.debug("Process started: number of processes " + numOfProcesses);
	}

}
