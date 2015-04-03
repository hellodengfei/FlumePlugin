package com.apache.flume.plugin;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class TimeRollingFileSink extends AbstractSink implements Configurable {

	private static final int DAY_MINITES = 7 * 24 * 60;

	private static final String YYYY_MM_DD = "yyyy-MM-dd";

	private static Logger logger = LoggerFactory
			.getLogger(TimeRollingFileSink.class);

	private static final String DEFAULT = "default";

	private static final int defaultRollInterval = 3600;
	private static final int defaultBatchSize = 100;

	private int batchSize = defaultBatchSize;

	/**
	 * 序列化类型
	 */
	private String serializerType;
	/**
	 * 序列化上下文
	 */
	private Context serializerContext;
	/**
	 * 选择器
	 */
	private String selector;

	/**
	 * 默认子路径
	 */
	private String defaultSubDir;

	private SinkCounter sinkCounter;

	/**
	 * 路径
	 */
	private String directory;

	/**
	 * 前缀
	 */
	private String suffix;

	/**
	 * 后缀
	 */
	private String prefix;

	/**
	 * 时间格式(最小时间单位为天)
	 */
	private String dateFormat;

	private DateFormat format;

	/**
	 * 回滚时间(分钟)
	 */
	private int rollInterval = defaultRollInterval;

	/**
	 * 保留最长时间(分钟)
	 */
	private int maxHistory = DAY_MINITES;

	private TimeRollFileManager defaultFileManager;

	private ScheduledExecutorService scheduler;

	private Map<String, TimeRollFileManager> selectableManager = new HashMap<String, TimeRollFileManager>();

	private Context properties;

	public void configure(Context context) {
		properties = new Context(context.getParameters());

		directory = context.getString("sink.directory");
		String rollInterval = context.getString("sink.rollInterval");
		maxHistory = context.getInteger("sink.maxHistory",
				DAY_MINITES);

		serializerType = context.getString("sink.serializer", "TEXT");
		serializerContext = new Context(context.getSubProperties("sink."
				+ EventSerializer.CTX_PREFIX));

		Preconditions.checkArgument(directory != null,
				"Directory may not be null");
		Preconditions.checkNotNull(serializerType,
				"Serializer type is undefined");

		prefix = context.getString("sink.prefix", "");
		suffix = context.getString("sink.suffix", "log");

		dateFormat = context.getString("sink.dateFormat", YYYY_MM_DD);

		selector = context.getString("sink.selector", "");
		defaultSubDir = context.getString("sink.defaultSubDir", DEFAULT);

		if (rollInterval == null) {
			this.rollInterval = defaultRollInterval;
		} else {
			this.rollInterval = Integer.parseInt(rollInterval);
		}
		if (!Strings.isNullOrEmpty(dateFormat)
				&& dateFormat.indexOf("dd") != -1) {
			try {
				format = new SimpleDateFormat(dateFormat);
			} catch (Exception e) {
				format = new SimpleDateFormat(YYYY_MM_DD);
			}
		}

		batchSize = context.getInteger("sink.batchSize", defaultBatchSize);

		if (sinkCounter == null) {
			sinkCounter = new SinkCounter(getName());
		}

		if (logger.isDebugEnabled()) {
			logger.debug(ToStringBuilder.reflectionToString(this,
					ToStringStyle.SHORT_PREFIX_STYLE));
		}
	}

	@Override
	public void start() {
		logger.info("Starting {}...", this);
		sinkCounter.start();
		scheduler = Executors.newScheduledThreadPool(
				8,
				new ThreadFactoryBuilder().setNameFormat(
						"rollingFileSink-roller-"
								+ Thread.currentThread().getId() + "-%d")
						.build());

		try {
			defaultFileManager = new TimeRollFileManager(directory
					+ File.separator + defaultSubDir, rollInterval, maxHistory,
					prefix, suffix, format, scheduler);
			defaultFileManager.start();

		} catch (Exception e) {
			logger.error(
					"file manager start error["
							+ defaultFileManager.getCurrentFile() + "]", e);
		}
		selectableManager.put(DEFAULT, defaultFileManager);

		super.start();
		logger.info("RollingFileSink {} started.", getName());
	}

	public Status process() throws EventDeliveryException {

		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();
		Event event = null;
		Status result = Status.READY;

		try {
			transaction.begin();
			int eventAttemptCounter = 0;
			Map<String, OutputAndSerializerPair> cachePairs = new HashMap<String, TimeRollingFileSink.OutputAndSerializerPair>();

			for (int i = 0; i < batchSize; i++) {
				sinkCounter.incrementEventDrainAttemptCount();
				eventAttemptCounter++;
				event = channel.take();
				if (event != null) {
					String type = DEFAULT;
					if (!Strings.isNullOrEmpty(selector)
							&& MapUtils.isNotEmpty(event.getHeaders())) {
						type = event.getHeaders().get(selector);
					}

					OutputAndSerializerPair toolKit = null;

					if (Strings.isNullOrEmpty(type)) {
						type = DEFAULT;
					} else {
						type = type.trim();
					}

					if ((toolKit = cachePairs.get(type)) == null) {
						TimeRollFileManager manager = null;

						if ((manager = this.selectableManager.get(type)) == null) {
							manager = buildManager(type);
							selectableManager.put(type, manager);
						}
						try {
							OutputStream outputStream = manager
									.getOutputStream();
							toolKit = new OutputAndSerializerPair(outputStream,
									manager.getSerializer(serializerType,
											serializerContext, outputStream));
							if (manager.isOverOrRotated()) {
								sinkCounter.incrementConnectionCreatedCount();
							}
							cachePairs.put(type, toolKit);
						} catch (IOException e) {
							sinkCounter.incrementConnectionFailedCount();
							manager.stop();
							throw new EventDeliveryException(
									"Failed to open file "
											+ manager.getCurrentFile()
											+ " while delivering event", e);
						}

					}

					toolKit.getSerializer().write(event);

				} else {
					// No events found, request back-off semantics from runner
					result = Status.BACKOFF;
				}
			}
			for (OutputAndSerializerPair pair : cachePairs.values()) {
				pair.getSerializer().flush();
				pair.getOutputStream().flush();
			}
			transaction.commit();
			sinkCounter.addToEventDrainSuccessCount(eventAttemptCounter);
		} catch (Exception ex) {
			transaction.rollback();
			throw new EventDeliveryException("Failed to process transaction",
					ex);
		} finally {
			transaction.close();
		}

		return result;
	}

	/**
	 * 可定义
	 * 
	 * @param type
	 * @return
	 * @throws IOException
	 */
	private TimeRollFileManager buildManager(String type) throws Exception {
		TimeRollFileManager manager;

		String innerDirectory = properties.getString(type + ".directory",
				directory);
		int innerRollInterval = properties.getInteger(type + ".rollInterval",
				rollInterval);
		int innerMaxHistory = properties.getInteger(type + ".maxHistory",
				maxHistory);
		String innerPrefix = properties.getString(type + ".prefix", prefix);
		String innerSuffix = properties.getString(type + ".suffix", suffix);
		String innerDateFormat = properties.getString(type + ".dateFormat",
				this.dateFormat);
		DateFormat innerFormat = format;

		if (!Strings.isNullOrEmpty(innerDateFormat)
				&& innerDateFormat.indexOf("dd") != -1) {
			try {
				innerFormat = new SimpleDateFormat(innerDateFormat);
			} catch (Exception e) {
				logger.error("date format error[" + innerDateFormat + "]");
			}
		}

		manager = new TimeRollFileManager(innerDirectory + File.separator
				+ type, innerRollInterval, innerMaxHistory, innerPrefix,
				innerSuffix, innerFormat, scheduler);
		manager.start();
		return manager;
	}

	@Override
	public void stop() {
		logger.info("RollingFile sink {} stopping...", getName());
		sinkCounter.stop();
		for (TimeRollFileManager manager : selectableManager.values()) {
			manager.stop();
		}
		super.stop();
		logger.info("RollingFile sink {} stopped. Event metrics: {}",
				getName(), sinkCounter);
	}

	static class OutputAndSerializerPair {
		public OutputAndSerializerPair(OutputStream outputStream,
				EventSerializer serializer) {
			super();
			this.outputStream = outputStream;
			this.serializer = serializer;
		}

		private OutputStream outputStream;
		private EventSerializer serializer;

		public OutputStream getOutputStream() {
			return outputStream;
		}

		public EventSerializer getSerializer() {
			return serializer;
		}

	}

}
