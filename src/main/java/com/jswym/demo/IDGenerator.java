package com.jswym.demo;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class IDGenerator {
	private static final Logger LOGGER = LoggerFactory.getLogger(IDGenerator.class);
	private final static long TW_EPORCH = 1402974492729L;

	private final static long WORKER_ID_BITS = 10L;
	private final static long MAX_WORKER_ID = -1L ^ (-1L << WORKER_ID_BITS);
	private final static long SEQUENCE_BITS = 12L;

	private final static long WORKER_ID_SHIFT = SEQUENCE_BITS;
	private final static long TIMESTAMP_LEFT_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS;
	private final static long SEQUENCE_MASK = -1L ^ (-1L << SEQUENCE_BITS);

	private static long LAST_TIMESTAMP = -1L;

	@Value("${idgenerator.zkConStr}")
	private String zkConStr;

	@Value("${idgenerator.parentNodePath}")
	private String parentNodePath;
	
	@Value("${idgenerator.workerPathPrefix}")
	private String workerPathPrefix;
	
	private CuratorFramework client;

	private long workerId;
	private long sequence = 0L;
	private boolean isInitialized = false;
	
	private void init() {
		client = CuratorFrameworkFactory.newClient(zkConStr, new ExponentialBackoffRetry(1000, 3));
		client.start();
		this.workerId = getUniqueWorkerId();
		isInitialized = true;
		LOGGER.info(String.format(
				"worker starting. timestamp left shift %d, worker id bits %d, sequence bits %d, workerid %d",
				TIMESTAMP_LEFT_SHIFT,  WORKER_ID_BITS, SEQUENCE_BITS, workerId));
	}

	public synchronized long nextId() {
		if(!isInitialized) {
			init();
		}
		long timestamp = System.currentTimeMillis();
		if (timestamp < LAST_TIMESTAMP) {
			LOGGER.error(String.format("clock is moving backwards.  Rejecting requests until %d.", LAST_TIMESTAMP));
			throw new RuntimeException(String.format(
					"Clock moved backwards.  Refusing to generate id for %d milliseconds", LAST_TIMESTAMP - timestamp));
		}
		if (LAST_TIMESTAMP == timestamp) {
			sequence = (sequence + 1) & SEQUENCE_MASK;
			if (sequence == 0) {
				timestamp = tilNextMillis(LAST_TIMESTAMP);
			}
		} else {
			sequence = 0L;
		}
		LAST_TIMESTAMP = timestamp;
		return ((timestamp - TW_EPORCH) << TIMESTAMP_LEFT_SHIFT) | (Long.valueOf(workerId) << WORKER_ID_SHIFT) | sequence;
	}

	private long getUniqueWorkerId() {
		long retVal = -1;
		try {
			if(client.checkExists().forPath(parentNodePath) == null) {
				client.create().forPath(parentNodePath);
			}
			
			List<String> children = client.getChildren().forPath(parentNodePath);
			Set<Long> idSet = new HashSet<Long>();
			for (String child : children) {
				byte[] data = client.getData().forPath(parentNodePath + "/" + child);
				String dataStr = new String(data);
				idSet.add(Long.parseLong(dataStr));
			}
			for (long i = 0; i < MAX_WORKER_ID; i++) {
				Long Longval = Long.valueOf(i);
				if (!idSet.contains(Longval)) {
					retVal = i;
					break;
				}
			}
			if (retVal == -1) {
				throw new RuntimeException("there is no spare workerid between 0 and " + MAX_WORKER_ID);
			}
			client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(parentNodePath+workerPathPrefix,
					(retVal + "").getBytes());
		} catch (Exception e) {

		}
		return retVal;
	}

	private long tilNextMillis(long lastTimestamp) {
		long timestamp = System.currentTimeMillis();
		while (timestamp <= lastTimestamp) {
			timestamp = System.currentTimeMillis();
		}
		return timestamp;
	}
}
