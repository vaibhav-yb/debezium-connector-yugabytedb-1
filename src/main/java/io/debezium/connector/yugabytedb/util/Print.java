package io.debezium.connector.yugabytedb.util;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Class containing some printing methods to be used if needed while debugging.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class Print {
	private static Logger LOGGER = LoggerFactory.getLogger(Print.class);
	public static void tabletCount(List<Pair<String, String>> tabletPairList) {
		LOGGER.info("Total pairs in polling list: {}", tabletPairList.size());
	}

	public static void tabletList(List<Pair<String, String>> tabletPairList) {
		LOGGER.info("Tablets in current polling list:");
		List<String> tablets = new ArrayList<>();
		for (Pair<String, String> pair : tabletPairList) {
			tablets.add(pair.getValue());
		}
		LOGGER.info(tablets.toString());
	}
}
