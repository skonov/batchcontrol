package batchcontrol.services;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Hashtable;
import java.util.Iterator;
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

public class BatchControlServiceImpl {
	private static final Logger log = Logger.getLogger(BatchControlServiceImpl.class);
	private static final String PROPERTY_FILE = "/modasolutions/conf/backendsystem.properties";
	private static final String HOST_PROPERTY = "batches_host_name";
	private static final String JNDI_DB_PROPERTY = "batches_modadb_jndi_name";
	private static String JNDI_DB_NAME;

	/** Scheduler types */
	public static final String DAILY_TYPE = "D";
	public static final String HOURLY_TYPE = "H";
	public static final String RESTRICTED_DAILY_TYPE = "R";
	public static final String MINUTE_TYPE = "M";
	public static final String MONTHLY_TYPE = "O";
	public static final String FIRST_BUSINESS_DAY_OF_MONTH_TYPE = "F";

	/** contains {@link DefaultBatch}s */
	private Map<String, DefaultBatch> batches = new Hashtable<String, DefaultBatch>();

	private boolean running;
	private final Object monitor = new Object();
	private int numOfProcesses;
	private final String host;

	private static BatchControlServiceImpl instance;

	/**
	 * Private constructor.
	 * @throws Exception if errors occur when reading properties.
	 */
	private BatchControlServiceImpl() throws Exception {
		FileInputStream fis = null;
		try
		{
			Properties prop = new Properties();
			fis = new FileInputStream(PROPERTY_FILE);
			prop.load(fis);
			host = prop.getProperty(HOST_PROPERTY);
			JNDI_DB_NAME = prop.getProperty(JNDI_DB_PROPERTY);
		}
		catch (Exception e)
		{
			throw e;
		}
		finally 
		{
			try 
			{
				if ( fis!=null ) 
				{
					fis.close();
				}
			}
			catch (IOException e) 
			{
				throw e;
			}
		}
	}

	/**
	 * Returns instance of this service.
	 * @return BatchControlServiceImpl instance.
	 * @throws Exception if instance could not be created.
	 */
	public static synchronized BatchControlServiceImpl getInstance() throws Exception {
		if(instance==null) {
			try {
				instance = new BatchControlServiceImpl();
			} catch (Exception e) {
				log.error("Failed to create instance: "+e, e);
				throw new Exception("Failed to create instance: "+e.getMessage());
			}
		}
		return instance;
	}

	/**
	 * Reloads all batches and schedulers from database only if no process is running and
	 * no batch is active.
	 * 
	 * @throws Exception if there is any process running or any batch is active.
	 */
	public synchronized void reloadBatches() throws Exception {
		synchronized(monitor) {
			if(running==true) {
				log.info("Service is active: " + numOfProcesses + " process(es) running.");
				throw new Exception("Service is active: " + numOfProcesses + " process(es) running.");
			}
			for(Iterator<DefaultBatch> i = batches.values().iterator(); i.hasNext(); ) {
				DefaultBatch batch = i.next();
				if(batch.getActive()==1 || batch.getStatus()==1) {
					log.info("Service is active: " + batch.getName() + " is active or scheduled.");
					throw new Exception("Service is active: " + batch.getName() + " is active or scheduled.");
				}
			}
			// nothing is running, we can reload
			log.debug("Reloading all batches...");
			batches.clear();
			loadAllBatches();
		}
		log.info("All batches were reloaded.");
	}

	/**
	 * Cancels batch. If task is still running it is allowed to finish and then batch is cancelled.
	 * 
	 * @param name fully qualified task class name.
	 * @throws Exception if another thread interrupted the current thread before or while the current 
	 * thread was waiting for a notification.
	 */
	public synchronized void stopBatch(String name) throws Exception {
		synchronized(monitor) {
			DefaultBatch batch = (DefaultBatch)batches.get(name);
			if(batch.getStatus()==1) {
				batch.stopTask();
				while(batch.getTaskState()==SchedulerTask.BUSY) {
					log.info("STOP: Stopping batch "+name+": waiting for task to complete...");
					monitor.wait();
				}
				batch.cancel();
				log.info("STOP: Batch stopped: "+name);
			}
		}
	}

	/**
	 * Start batch.
	 * 
	 * @param name fully qualified task class name.
	 * @param runOnce if true, the batch is scheduled for one-time execution immediately, 
	 *                otherwise it uses associated schedulers.
	 * @throws Exception if batch fails to start.
	 */
	public synchronized void startBatch(String taskClassName, boolean runOnce) throws Exception {
		synchronized(monitor) {
			DefaultBatch batch = (DefaultBatch)batches.get(taskClassName);
			try {
				// clear all iterators they will be all reloaded during load
				batch.clearIterators();
				loadBatch(taskClassName);
				batch.setRunOnce(runOnce);
				String result = batch.start() ? 
						"Batch started: "+batch.getName() : "Batch NOT started: "+taskClassName;
						log.info("START: "+result);
			} catch (Exception e) {
				log.error("START: Batch "+batch.getName()+" failed to start: "+e, e);
				throw new Exception("Batch "+batch.getName()+" failed to start: "+e);
			}
		}
	}

	/**
	 * Updates batch active status in database and in the service. 
	 * Setting active to 0 makes batch ineligible to run.
	 * 
	 * @param batchClassName  fully qualified task class name.
	 * @param active active flag: 1 or 0
	 * @throws Exception if database error occurs.
	 */
	public synchronized void setActive(String batchClassName, int active) throws Exception {
		Connection con = null;
		PreparedStatement stmt = null;
		try {
			con = getConnection();
			String sql = "update batches set active=? where classname=?";
			stmt = con.prepareStatement(sql);
			stmt.setInt(1, active);
			stmt.setString(2, batchClassName);
			stmt.execute();
			synchronized(monitor) {
				DefaultBatch batch = (DefaultBatch)batches.get(batchClassName);
				batch.setActive(active);
			}
			log.info(batchClassName+": active="+active);
		} catch (Exception e) {
			log.error("Error setting batch active field: " + e, e);
			throw new Exception("Error setting batch active field: " + e);
		} finally {
			try {
				if(stmt!=null) stmt.close();
				if(con!=null) con.close();
			} catch (Exception e) {
				log.error("setActive: error closing database objects: "+e, e);
			}
		}
	}

	/**
	 * Returns Map that contains copies of all batches.
	 * Each batch is mapped to the task class name.
	 * 
	 * @return Map of copies of all batches.
	 */
	public Map<String, DefaultBatch> getAllBatches() {
		Map<String, DefaultBatch> copy = new TreeMap<String, DefaultBatch>();
		synchronized(monitor) {
			for(Iterator<String> i=batches.keySet().iterator(); i.hasNext(); ) {
				String key = i.next();
				DefaultBatch batch = (DefaultBatch)batches.get(key);
				copy.put(key, (DefaultBatch)batch.clone());
			}
		}
		log.debug("Returning copies of all batches.");
		return copy;
	}

	/**
	 * Returns copy of the batch.
	 * @param batchId fully qualified class name of a batch
	 * @return Copy of batch.
	 */
	public DefaultBatch getBatch(String batchId) {
		synchronized(monitor) {
			return (DefaultBatch)((DefaultBatch)batches.get(batchId)).clone();
		}
	}

	/**
	 * Returns monitor.
	 * @return monitor.
	 */
	Object getMonitor() {
		return monitor;
	}

	/**
	 * Loads all batches from database.
	 * @throws Exception if database error occurs during operation.
	 */
	void loadAllBatches() throws Exception {
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
			String sql = "select b.name, b.status, b.classname, b.active, b.server, " +
			"s.id as s_id, s.s_type, s.s_hour, s.s_minute, s.s_second, s.s_days " +
			"from batches b left join schedulers s on b.id=s.batch_id "+
			"where b.server='"+host+"'";
			if(taskClassName!=null) {
				sql += " and b.classname='"+taskClassName+"'";
			}

			rs = stmt.executeQuery(sql);
			while(rs.next()) {
				String name = rs.getString("name");
				String className = rs.getString("classname");
				DefaultBatch batch = (DefaultBatch)batches.get(className); 
				if(batch==null) {
					batch = initBatch(className, name, rs.getInt("active"));
				}
				// server can be changed manually in database during runtime
				batch.setServer(rs.getString("server"));

				String schedulerType = rs.getString("s_type");
				// if batch has schedulers
				if(schedulerType != null) {
					int id = rs.getInt("s_id");
					int hour = rs.getInt("s_hour");
					int minute = rs.getInt("s_minute");
					int second = rs.getInt("s_second");
					String s_days = rs.getString("s_days");
					s_days = (s_days==null ? "" : s_days.trim());
					int[] days = new int[s_days.length()];
					for(int i=0; i<s_days.length(); i++) {
						days[i] = Integer.parseInt(String.valueOf(s_days.charAt(i)));
					}

					SchedulerIterator iterator = null;
					if(schedulerType.equals(DAILY_TYPE)) 
					{ 
						iterator = new DailyIterator(hour, minute, second, id);
					} 
					else if(schedulerType.equals(HOURLY_TYPE)) 
					{ 
						iterator = new HourlyIterator(minute, second, id);
					} 
					else if(schedulerType.equals(RESTRICTED_DAILY_TYPE)) 
					{ 
						iterator = new RestrictedDailyIterator(hour, minute, second, days, id);
					}
					else if(schedulerType.equals(MINUTE_TYPE)) {
						iterator = new MinuteIterator(second, id);
					}
					else if(schedulerType.equals(MONTHLY_TYPE)) {
						iterator = new MonthlyIterator(days[0], hour, minute, second, id);
					}
					else if(schedulerType.equals(FIRST_BUSINESS_DAY_OF_MONTH_TYPE)) {
						iterator = new FirstBusinessDayOfMonthIterator(hour, minute, second, id);
					}
					batch.addSchedulerIterator(iterator);
					log.debug("Iterator was added to " + name + " batch: " + iterator);
				}
			}
			if(taskClassName!=null) {
				log.debug("Batch loaded: " + taskClassName);
			}
		} catch (Exception e) {
			log.error("Error loading batches: " + e, e);
			throw new Exception("Error loading batches: " + e);
		} finally {
			try {
				if(rs!=null) rs.close();
				if(stmt!=null) stmt.close();
				if(con!=null) con.close();
			} catch (Exception e) {
				log.error("loadBatches: error closing database objects: " + e, e);
			}
		}
	}

	/**
	 * Returns all batches managed by this service.
	 * @return Map of task class name - batch pairs.
	 */
	Map<String,DefaultBatch> getBatches() {
		return batches;
	}

	/**
	 * Returns runnning status.
	 * @return true if any of the tasks is currently executing.
	 */
	boolean isRunning() {
		return running;
	}

	/**
	 * Creates new batch and adds it to the service. 
	 * 
	 * @param className fully qualified task class name,
	 * @param batchName short batch name
	 * @param active    active flag
	 * @param server    server name
	 * @return ({@link DefaultBatch}
	 */
	private DefaultBatch initBatch(String className, String batchName, int active) {
		DefaultBatch batch = new DefaultBatch(batchName, host, className);
		batch.setActive(active);
		batches.put(className, batch);
		return batch;
	}

	public synchronized void updateScheduler(int id, String type, int hour, int minute, int second, String days) throws Exception {
		Connection con = null;
		PreparedStatement stmt = null;
		try {
			con = getConnection();
			String sql = "update schedulers set s_type=?, s_hour=?, s_minute=?, s_second=?, s_days=? where id=?";
			stmt = con.prepareStatement(sql);
			stmt.setString(1, type);
			stmt.setInt(2, hour);
			stmt.setInt(3, minute);
			stmt.setInt(4, second);
			stmt.setString(5, days);
			stmt.setInt(6, id);
			stmt.execute();
			log.debug("Scheduler updated: id="+id+", type="+type+", hour="+hour+", min="+minute+", sec="+second+", days="+days);
		} catch (Exception e) {
			log.error("Error updating scheduler: " + e, e);
			throw new Exception("Error updating scheduler: " + e);
		} finally {
			try {
				if(stmt!=null) stmt.close();
				if(con!=null) con.close();
			} catch (Exception e) {
				log.error("updateScheduler: error closing database objects: "+e, e);
			}
		}
	}

	/**
	 * Creates iterator for the specified batch in database. It does not update current service state.
	 * After the iterator has been created batches can be reloaded and the newly created iterator
	 * will be registered by the service. Return database ID of newly inserted scheduler.
	 * 
	 * @param type       iterator type: 
	 * 						H(every hour), 
	 * 						M(every minute), 
	 * 						D(daily), 
	 * 						R(restricted daily), 
	 * 						O(monthly), 
	 * 						F(first business day of month)
	 * @param hour       hour (0-24)
	 * @param minute     minute
	 * @param second     second
	 * @param days       arrays of integers representing days of week, where 1 is Sunday, 2 - Monday, and so on.
	 * @param batchName  short batch name.
	 * 
	 * @return database ID of new scheduler.
	 * 
	 * @throws Exception if database error occurs.
	 */
	public synchronized int createSchedulerIterator(String type, int hour, int minute, int second, 
			String days, String batchName) throws Exception 
			{
		Connection con = null;
		PreparedStatement stmt = null;
		PreparedStatement stmt2 = null;
		int schedulerId = -1;
		try {
			con = getConnection();
			con.setAutoCommit(false);
			String sql = "insert into schedulers (s_type, s_hour, s_minute, s_second, s_days, batch_id)"+
			" values (?,?,?,?,?,(select id from batches where classname=?))";
			stmt = con.prepareStatement(sql);
			stmt.setString(1, type);
			stmt.setInt(2, hour);
			stmt.setInt(3, minute);
			stmt.setInt(4, second);
			stmt.setString(5, days);
			stmt.setString(6, batchName);
			stmt.execute();

			stmt2 = con.prepareStatement("select max(id) from schedulers");
			ResultSet rs = stmt2.executeQuery();
			if(rs.next()) {
				schedulerId = rs.getInt(1);
			}
			log.debug("New scheduler created: ID="+schedulerId);
			if(schedulerId < 0) {
				throw new Exception("Failed to get scheduler ID.");
			}
			con.commit();

		} catch (Exception e) {
			con.rollback();
			log.error("Error creating new scheduler: " + e, e);
			throw new Exception("Error creating new scheduler: " + e);
		} finally {
			try {
				if(stmt!=null) stmt.close();
				if(con!=null) con.close();
			} catch (Exception e) {
				log.error("createSchedulerIterator: error closing database objects: "+e, e);
			}
		}
		return schedulerId;
			}

	/**
	 * Creates new batch in database. It does not update current service state.
	 * After the batch has been created batches can be reloaded and the newly created batch 
	 * will be registered by the service.
	 * 
	 * @param name          short batch name.
	 * @param taskClassName fully qualified task class name.
	 * @param active        active status: 1 or 0
	 * @param server        server name
	 * @throws Exception if database error occurs.
	 */
	public void createBatch(String name, String taskClassName, int active, String server) throws Exception 
	{
		Connection con = null;
		PreparedStatement stmt = null;
		try {
			con = getConnection();
			String sql = "insert into batches (name, classname, active, server) values (?,?,?,?)";
			stmt = con.prepareStatement(sql);
			stmt.setString(1, name);
			stmt.setString(2, taskClassName);
			stmt.setInt(3, active);
			stmt.setString(4, server);
			stmt.execute();
		} catch (Exception e) {
			log.error("Error creating new batch: " + e, e);
			throw new Exception("Error creating new batch: " + e);
		} finally {
			try {
				if(stmt!=null) stmt.close();
				if(con!=null) con.close();
			} catch (Exception e) {
				log.error("createBatch: error closing database objects: "+e, e);
			}
		}
	}

	/**
	 * Deletes scheduler from database.
	 * @param schedulerId scheduler ID
	 * @throws Exception if database error occurs.
	 */
	public synchronized void deleteScheduler(String schedulerId) throws Exception {
		Connection con = null;
		PreparedStatement stmt = null;
		try {
			con = getConnection();
			String sql = "delete from schedulers where id=?";
			stmt = con.prepareStatement(sql);
			stmt.setString(1, schedulerId);
			stmt.execute();
		} catch (Exception e) {
			log.error("Error deleting scheduler "+schedulerId+": " + e, e);
			throw new Exception("Error deleting scheduler "+schedulerId+": " + e);
		} finally {
			try {
				if(stmt!=null) stmt.close();
				if(con!=null) con.close();
			} catch (Exception e) {
				log.error("deleteScheduler: error closing database objects: "+e, e);
			}
		}
	}

	/**
	 * This method is called from {@link SchedulerTask} every time 
	 * the task/process is started. It increases process count by one.
	 */
	void processStarted() {
		synchronized(monitor) {
			running = true;
			numOfProcesses++;
			log.debug("Process started: number of processes "+numOfProcesses);
		}
	}

	/**
	 * This method is called from {@link SchedulerTask} every time 
	 * the task/process has finished its single run.
	 * It decreases process count by one.
	 */
	void processStopped() {
		synchronized(monitor) {
			numOfProcesses--;
			if(numOfProcesses==0) {
				running = false;
				monitor.notifyAll();
			}
			log.debug("Process stopped: number of processes "+numOfProcesses+", running="+running);
		}
	}

	/**
	 * Reads batch active flag from database. The active flag can be changed directly in database
	 * by anyone.
	 * 
	 * @param batchClassName  fully qualified task class name.
	 * @return batch active flag 1 or 0, or -1 if there was an error.
	 * @throws Exception  if database error occurs.
	 */
	int getActive(String batchClassName) throws Exception {
		Connection con = null;
		ResultSet rs = null;
		PreparedStatement stmt = null;
		int active = -1;
		try {
			con = getConnection();
			String sql = "select active from batches where classname=?";
			stmt = con.prepareStatement(sql);
			stmt.setString(1, batchClassName);
			rs = stmt.executeQuery();
			while(rs.next()) {
				active  = rs.getInt("active");
			}
			log.debug("getActive: "+batchClassName+": active="+active);
		} catch (Exception e) {
			log.error("Error getting batch active field: " + e, e);
			throw new Exception("Error getting batch active field: " + e);
		} finally {
			try {
				if(rs!=null) rs.close();
				if(stmt!=null) stmt.close();
				if(con!=null) con.close();
			} catch (Exception e) {
				log.error("getActive: error closing database objects: "+e, e);
			}
		}
		return active;
	}

	/**
	 * Changes batch schedule status in database.
	 * 
	 * @param batchName  batch short name
	 * @param status     batch schedule status (1 or 0)
	 * @throws Exception if database error occurs.
	 */
	void updateBatchStatus(String batchName, int status) throws Exception {
		Connection con = null;
		PreparedStatement stmt = null;
		try {
			con = getConnection();
			String sql = "update batches set status=? where name=?";
			stmt = con.prepareStatement(sql);
			stmt.setInt(1, status);
			stmt.setString(2, batchName);
			stmt.execute();
		} catch (Exception e) {
			log.error("Error updating batch status: " + e, e);
			throw new Exception("Error updating batch status: " + e);
		} finally {
			try {
				if(stmt!=null) stmt.close();
				if(con!=null) con.close();
			} catch (Exception e) {
				log.error("updateBatchStatus: error closing database objects: "+e, e);
			}
		}
	}

	private Connection getConnection() throws Exception {
		Connection con = null;
		try {
			Context ctx = new InitialContext();
			DataSource ds = (DataSource)ctx.lookup(JNDI_DB_NAME);
			con = ds.getConnection();
		} catch (Exception e) {
			log.error(e.toString());
			throw new Exception("Error getting database connection: " + e, e);
		}
		return con;
	}

}
