package io.pivotal.geode.function;

import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ClearPartitionRegion implements Function, Declarable {
	private static final Logger LOG = LoggerFactory.getLogger(ClearPartitionRegion.class);

	public void execute(FunctionContext context) {
		if (LOG.isDebugEnabled())
			LOG.debug(Thread.currentThread().getName() + " executing " + getId());

		if (context instanceof RegionFunctionContext) {
			RegionFunctionContext rfc = (RegionFunctionContext) context;
			Region localRegion = PartitionRegionHelper.getLocalDataForContext(rfc);
			int numLocalEntries = localRegion.size();
			localRegion.keySet().stream().forEach(key -> localRegion.remove(key));
			LOG.info("Cleared " + numLocalEntries + " entries from " + localRegion.getName() + " region local data.");
			context.getResultSender().lastResult(numLocalEntries);
		} else {
			throw new FunctionException("Context must be a RegionFunctionContext");
		}
	}

	public String getId() {
		return getClass().getSimpleName();
	}

	public boolean optimizeForWrite() {
		return true;
	}

	public boolean hasResult() {
		return true;
	}

	public boolean isHA() {
		return true;
	}

	public void init(Properties properties) {
	}
}
