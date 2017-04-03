package org.cloudbus.cloudsim.ex.mapreduce.policy;

import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.Cloud;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Request;

public class FIXVms extends Policy {

	@Override
	public Boolean runAlgorithm(Cloud cloud, Request request) {
		// TODO Auto-generated method stub
		FIX fix = new FIX();
		return fix.runAlgorithm(cloud, request);
	}

}
