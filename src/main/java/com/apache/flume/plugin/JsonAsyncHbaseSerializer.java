package com.apache.flume.plugin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import net.sf.json.JSONObject;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.AsyncHbaseEventSerializer;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.PutRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

public class JsonAsyncHbaseSerializer implements AsyncHbaseEventSerializer {

	private static final Logger logger = LoggerFactory
			.getLogger(JsonAsyncHbaseSerializer.class);

	private String charset;
	private String jsonIdentifier;
	private String jsonTimestampIdentifier;
	private String incrementColumn;
	private String incrementRow;
	private String rowkeyJoiner;

	private static final String JOINER = "|";
	private static final String INC_ID = "inc_id";
	private static final String TIMESTAMP = "timeStamp";
	private static final String ID = "id";
	private static final String UTF_8 = "UTF-8";

	private static final String CHARSET_KEY = "charset";
	private static final String JSON_IDENTIFIER_KEY = "josn_identifier";
	private static final String JSON_TIMESTAMP_KEY = "json_timeStamp";
	private static final String INCREMENT_ROW_KEY = "incrementRow";
	private static final String ROWKEY_JOINER_KEY = "rowkeyJoiner";

	private PutRequest putRequest;
	private byte[] table;
	private byte[] cf;

	@Override
	public void configure(Context context) {
		logger.debug("config[" + context + "]");
		charset = context.getString(CHARSET_KEY, UTF_8);
		jsonIdentifier = context.getString(JSON_IDENTIFIER_KEY, ID);
		jsonTimestampIdentifier = context.getString(JSON_TIMESTAMP_KEY,
				TIMESTAMP);
		incrementRow = context.getString(INCREMENT_ROW_KEY, INC_ID);
		rowkeyJoiner = context.getString(ROWKEY_JOINER_KEY, JOINER);
	}

	@Override
	public void configure(ComponentConfiguration conf) {

	}

	@Override
	public void initialize(byte[] table, byte[] cf) {
		this.table = table;
		this.cf = cf;
	}

	@Override
	@SuppressWarnings("rawtypes")
	public void setEvent(Event event) {
		String body = "";
		try {
			body = new String(event.getBody(), charset);
			JSONObject json = null;
			if (Strings.isNullOrEmpty(body)) {
				return;
			}

			json = JSONObject.fromObject(body);
			if (json.isEmpty() || json.isNullObject()) {
				return;
			}

			String jsonId = "";
			if (json.containsKey(jsonIdentifier)) {
				jsonId = json.getString(jsonIdentifier);
			}

			String jsonTimestamp = "";
			if (json.containsKey(jsonTimestampIdentifier)) {
				jsonTimestamp = json.getString(jsonTimestampIdentifier);
			}

			if (Strings.isNullOrEmpty(jsonTimestamp)) {
				jsonTimestamp = event.getHeaders().get(TIMESTAMP);
			}

			String rowKey = Strings.isNullOrEmpty(jsonTimestamp) ? String
					.valueOf(System.currentTimeMillis()) : jsonTimestamp
					+ Strings.isNullOrEmpty(jsonId) != null ? ""
					: (rowkeyJoiner + jsonId);

			int length = json.size();
			byte[][] qualifiers = new byte[length][];
			byte[][] values = new byte[length][];
			Iterator keys = json.keys();
			int idx = 0;
			while (keys.hasNext()) {
				String key = keys.next().toString();
				qualifiers[idx] = key.getBytes();
				values[idx] = json.getString(key).getBytes();
				idx++;
			}
			putRequest = new PutRequest(table, rowKey.getBytes(), cf,
					qualifiers, values);
			if (logger.isDebugEnabled()) {
				logger.debug("putRequest[" + putRequest.toString() + "]");
			}

		} catch (Exception e) {
			logger.error("json serializer failed,body[" + body + "]"
					+ "header[" + event.getHeaders() + "]", e);
		}

	}

	@Override
	public List<PutRequest> getActions() {
		if (putRequest != null) {
			return Arrays.asList(putRequest);
		}
		return Collections.emptyList();
	}

	@Override
	public List<AtomicIncrementRequest> getIncrements() {

		List<AtomicIncrementRequest> actions = new ArrayList<AtomicIncrementRequest>();
		if (incrementColumn != null) {
			AtomicIncrementRequest inc = new AtomicIncrementRequest(table,
					incrementRow.getBytes(), cf, incrementColumn.getBytes());
			actions.add(inc);
		}
		return actions;
	}

	@Override
	public void cleanUp() {

	}

}
