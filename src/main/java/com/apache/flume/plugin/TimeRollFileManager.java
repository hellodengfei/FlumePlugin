package com.apache.flume.plugin;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.IOUtils;
import org.apache.flume.Context;
import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.serialization.EventSerializerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class TimeRollFileManager {

	private static final String JOINER = "-";

	private static final Logger logger = LoggerFactory
			.getLogger(TimeRollFileManager.class);

	private TreeMap<Long, String> rollFiles = new TreeMap<Long, String>();

	private String directory;

	private DateFormat format;

	private long interval;

	private int maxHistory;

	private long lastOverTime = -1;

	private long lastRotateTime = -1;

	private String prefix;

	private String suffix;

	private ScheduledExecutorService scheduler;

	private volatile boolean shouldRotate = false;

	private volatile boolean shouldOver = false;

	private AtomicInteger atomicRotateCounter = new AtomicInteger(0);

	private AtomicInteger atomicOverCounter = new AtomicInteger(0);

	private String lastFile;

	private OutputStream outputStream;

	private boolean isOverOrRotate = false;

	private File baseDir;

	private boolean initialized = false;

	private boolean privateScheduler = false;

	TimeRollFileManager(String directory, long interval, int maxHistory,
			String prefix, String suffix, DateFormat format,
			ScheduledExecutorService scheduler) {
		this.directory = directory;
		this.interval = interval;
		this.maxHistory = maxHistory;
		this.prefix = prefix;
		this.suffix = suffix;
		this.format = format;
		this.scheduler = scheduler;

	}

	public void start() {
		if (format == null) {
			throw new IllegalArgumentException("date format can't be null");
		}

		initialBaseDir();
		if (scheduler == null) {
			privateScheduler = true;
			scheduler = Executors.newScheduledThreadPool(
					1,
					new ThreadFactoryBuilder().setNameFormat(
							"rollingFileSink-roller-"
									+ Thread.currentThread().getId() + "-%d")
							.build());
		}
		scheduleFixRotate();
		scheduleOver();
		scheduleClean();
		initialized = true;
	}

	private void initialBaseDir() {
		baseDir = new File(directory);
		if (!baseDir.exists()) {
			baseDir.mkdirs();
		}
	}

	public void stop() {
		closeOutputStream();
		if (privateScheduler && scheduler != null) {
			scheduler.shutdown();
			scheduler.shutdownNow();
		}
		initialized = false;
	}

	public void closeOutputStream() {
		if (outputStream != null) {
			IOUtils.closeQuietly(outputStream);
			outputStream = null;
		}
		initialBaseDir();
	}

	private void scheduleOver() {
		Date date = new Date();
		Date zeroTime = DateUtil.getZeroTime(date);
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(zeroTime);
		calendar.add(Calendar.DATE, 1);
		long delay = calendar.getTimeInMillis() - System.currentTimeMillis();

		scheduler.schedule(new Callable<Void>() {

			public Void call() throws Exception {
				shouldOver = true;
				return null;
			}
		}, delay, TimeUnit.MILLISECONDS);

		scheduler.scheduleAtFixedRate(new Runnable() {

			public void run() {
				shouldOver = true;
			}

		}, delay + DateUtil.DAY_MILLI_SECS, DateUtil.DAY_MILLI_SECS,
				TimeUnit.MILLISECONDS);
	}

	private void scheduleClean() {
		if (maxHistory != -1) {
			scheduler.schedule(new Cleaner(maxHistory, directory, prefix,
					suffix, format), 60, TimeUnit.SECONDS);

			long peroid = DateUtil.DAY_SECS;
			if (maxHistory < DateUtil.DAY_SECS) {
				if (maxHistory / DateUtil.HOUR_SECS > 0) {
					peroid = DateUtil.HOUR_SECS;
				} else {
					peroid = 5;
				}
			}

			scheduler.scheduleAtFixedRate(new Cleaner(maxHistory, directory,
					rollFiles), 1, peroid, TimeUnit.MINUTES);
		}

	}

	/**
	 * 忽略over后interval被切分
	 * 
	 * @param interval
	 */
	private void scheduleFixRotate() {
		if (interval != -1) {

			scheduler.scheduleAtFixedRate(new Runnable() {
				public void run() {
					shouldRotate = true;
				}
			}, interval, interval, TimeUnit.SECONDS);
		}
	}

	public OutputStream getOutputStream() throws IOException {

		if (!initialized) {
			start();
		}

		if (outputStream == null || shouldOver || shouldRotate) {
			if (logger.isInfoEnabled()) {
				logger.info(
						"something happen over:{},lastOverTime:{},overCount:{}, rotate:{},lastRotateTime:{},rotateCount:{},lastFile:{},directory:{},",
						shouldOver, lastOverTime, atomicOverCounter.get(),
						shouldRotate, lastRotateTime,
						atomicRotateCounter.get(), lastFile, directory);
			}

			isOverOrRotate = true;

			if (shouldOver) {
				lastOverTime = System.currentTimeMillis();
				atomicRotateCounter.set(0);
				atomicOverCounter.incrementAndGet();
				shouldOver = false;
			}

			if (shouldRotate) {
				lastRotateTime = System.currentTimeMillis();
				atomicRotateCounter.incrementAndGet();
				shouldRotate = false;
			}

			if (outputStream != null) {
				try {

					outputStream.flush();
					outputStream.close();
					if (logger.isInfoEnabled()) {
						logger.info("outputstream is closed[" + lastFile
								+ ",directory[" + directory + "]");
					}
					outputStream = null;
				} catch (IOException e) {
					logger.error("close file output stream error[" + lastFile
							+ "]" + ",directory[" + directory + "]", e);
					IOUtils.closeQuietly(outputStream);
					throw e;
				}

			}
			buildOutpuStream();
		}

		return outputStream;
	}

	public OutputStream getCurrentOutputStream() {
		return this.outputStream;
	}

	public EventSerializer getSerializer(String serializerType,
			Context serializerContext, OutputStream outputStream)
			throws IOException {
		EventSerializer serializer = EventSerializerFactory.getInstance(
				serializerType, serializerContext, outputStream);
		serializer.afterCreate();

		return serializer;
	}

	public boolean isOverOrRotated() {
		if (isOverOrRotate) {
			isOverOrRotate = false;
			return true;
		}
		return false;
	}

	private void buildOutpuStream() throws IOException {
		int num = atomicRotateCounter.get();
		String middleName = "";
		middleName = format.format(new Date());

		if (num > 0) {
			middleName += JOINER + num;
		}

		try {
			File file = new File(baseDir, prefix + middleName + suffix);
			lastFile = file.getName();
			synchronized (rollFiles) {
				rollFiles.put(System.currentTimeMillis(), lastFile);
			}
			if (logger.isInfoEnabled()) {
				logger.info("outputstream is building[" + lastFile
						+ "],dirctory[" + directory + "]");
			}
			outputStream = new FileOutputStream(file);
		} catch (FileNotFoundException e) {
			logger.error("file not found[" + lastFile + "],dirctory["
					+ directory + "]", e);
			throw e;
		}

	}

	public String getCurrentFile() {
		return this.lastFile;
	}

	public static class Cleaner implements Runnable {

		private boolean useCache;

		private String directory;

		private String prefix;

		private String suffix;

		private DateFormat format;

		private TreeMap<Long, String> rollFiles;

		private long maxHistory;

		Cleaner(long endTime, String directory, String prefix, String suffix,
				DateFormat format) {
			useCache = false;
			this.maxHistory = endTime;
			this.directory = directory;
			this.prefix = prefix;
			this.suffix = suffix;
			this.format = format;

		}

		Cleaner(long maxHistory, String directory,
				TreeMap<Long, String> rollFiles) {
			useCache = true;
			this.maxHistory = maxHistory;
			this.directory = directory;
			this.rollFiles = rollFiles;
		}

		public void run() {
			if (logger.isInfoEnabled()) {
				logger.info("start cleaing,directory[" + directory + "]");
			}
			File[] delFiles = null;
			final long endTime = System.currentTimeMillis() - maxHistory
					* DateUtil.MINITE_MILLI_SECS;
			if (useCache) {
				synchronized (rollFiles) {
					Map<Long, String> elapseFiles = null;
					if (MapUtils.isNotEmpty(rollFiles)
							&& MapUtils.isNotEmpty(elapseFiles = rollFiles
									.headMap(endTime, false))) {
						List<File> deletedFiles = new ArrayList<File>();
						Iterator<Entry<Long, String>> iterator = elapseFiles
								.entrySet().iterator();
						for (; iterator.hasNext();) {
							Entry<Long, String> entry = iterator.next();
							File file = new File(directory, entry.getValue());
							if (file.lastModified() < endTime) {
								deletedFiles.add(file);
								iterator.remove();
								rollFiles.remove(entry.getKey());
							}
						}
						delFiles = deletedFiles.toArray(new File[deletedFiles
								.size()]);
					}
				}

			} else {
				delFiles = new File(directory).listFiles(new FileFilter() {

					public boolean accept(File file) {
						String fileName = file.getName();
						int idx = -1;

						if (!Strings.isNullOrEmpty(prefix)) {
							if (!fileName.startsWith(prefix)) {
								return false;
							}
							fileName = fileName.substring(prefix.length());
						}

						if (!Strings.isNullOrEmpty(suffix)) {
							if ((idx = fileName.lastIndexOf(suffix)) == -1) {
								return false;
							}
							fileName = fileName.substring(0, idx);
						}

						if ((idx = fileName.lastIndexOf(JOINER)) != -1) {
							fileName = fileName.substring(0, idx);
						}

						try {
							format.parse(fileName);
						} catch (Exception e) {
							logger.error(
									"file date format paser error["
											+ file.getName() + "]directory["
											+ directory + "]", e);
							return false;
						}

						return file.lastModified() < endTime;
					}
				});

			}

			if (delFiles != null && delFiles.length > 0) {

				for (File file : delFiles) {
					file.deleteOnExit();
					if (logger.isInfoEnabled()) {
						logger.info("file is deleted[" + file.getName()
								+ "],directory[" + directory + "]");
					}

				}
				return;
			}

			if (logger.isInfoEnabled()) {
				logger.info("clean end,no expired file found,directory["
						+ directory + "]");
			}

		}
	}

	public static class DateUtil {

		private static final long DELTA;

		public static final int MINITE_MILLI_SECS = 60 * 1000;

		public static final long HOUR_MILLLI_SECS = 60 * MINITE_MILLI_SECS;

		public static final long DAY_MILLI_SECS = 24 * HOUR_MILLLI_SECS;

		public static final int DAY_SECS = 24 * 60;

		public static final int HOUR_SECS = 60;

		static {

			Calendar instance = Calendar.getInstance();

			DELTA = -(instance.get(Calendar.ZONE_OFFSET) + instance
					.get(Calendar.DST_OFFSET));

		}

		public static Date getZeroTime(Date date) {
			if (date == null) {
				date = new Date();
			}

			long milliSecs = date.getTime();

			return new Date(milliSecs - ((milliSecs - DELTA) % DAY_MILLI_SECS));
		}

		public static long getIntevalOfZeroTime(Date date) {
			if (date == null) {
				date = new Date();
			}
			return date.getTime() - getZeroTime(date).getTime();
		}

	}

}
