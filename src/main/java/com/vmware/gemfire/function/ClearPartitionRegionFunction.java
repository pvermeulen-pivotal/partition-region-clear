package com.vmware.gemfire.function;

import lombok.extern.slf4j.Slf4j;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.partition.PartitionRegionHelper;

@Slf4j
public class ClearPartitionRegionFunction implements Function {

	public void execute(FunctionContext context) {
		if (log.isDebugEnabled())
			log.debug(Thread.currentThread().getName() + " executing " + getId());

		if (context instanceof RegionFunctionContext) {
			RegionFunctionContext rfc = (RegionFunctionContext) context;
			Region localRegion = PartitionRegionHelper.getLocalDataForContext(rfc);
			int numLocalEntries = localRegion.size();
			localRegion.keySet().stream().forEach(key -> localRegion.remove(key));
			log.info("Cleared " + numLocalEntries + " entries from " + localRegion.getName() + " region local data.");
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
		return false;
	}
}
