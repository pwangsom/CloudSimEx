package org.cloudbus.cloudsim.ex.web.experiments;

import java.util.Arrays;
import java.util.List;

import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.ex.disk.HddResCloudlet;
import org.cloudbus.cloudsim.ex.disk.HddVm;
import org.cloudbus.cloudsim.ex.util.CustomLog;
import org.cloudbus.cloudsim.ex.util.TextUtil;
import org.cloudbus.cloudsim.ex.web.ILoadBalancer;
import org.cloudbus.cloudsim.ex.web.WebBroker;

/**
 * 
 * A web broker that logs the performance indicators of the virtual machines.
 * 
 * @author nikolay.grozev
 * 
 */
public class PerformanceLoggingWebBroker extends WebBroker {

    protected static final int LOG_TAG = UPDATE_SESSION_TAG + 1;

    private static final List<? extends Class<?>> HEADER_TYPES = Arrays.asList(Double.class, Integer.class,
	    Double.class, Double.class, Double.class);

    private static final List<String> HEADER_NAMES = Arrays.asList("time", "vmId", "percentCPU", "percentIO",
	    "percentRAM");

    private boolean headerPrinted = false;
    private boolean logStarted = false;
    private double lastTimeCloudletReturned = 0;
    private double lastTimeCloudletSubmited = 0;
    private double offset = 0;

    private final double logPeriod;

    public PerformanceLoggingWebBroker(final String name, final double refreshPeriod, final double lifeLength,
	    final double logPeriod, final double offset) throws Exception {
	super(name, refreshPeriod, lifeLength);
	this.logPeriod = logPeriod;
	this.offset = offset;
    }

    public PerformanceLoggingWebBroker(final String name, final double refreshPeriod,
	    final double lifeLength, final double logPeriod, final double offset, final List<Integer> dataCenterIds) throws Exception {
	super(name, refreshPeriod, lifeLength, dataCenterIds);
	this.logPeriod = logPeriod;
	this.offset = offset;
    }

    @Override
    protected void processCloudletReturn(final SimEvent ev) {
	lastTimeCloudletReturned = CloudSim.clock();
	super.processCloudletReturn(ev);
    }

    @Override
    protected void submitCloudlets() {
	lastTimeCloudletSubmited = CloudSim.clock();
	super.submitCloudlets();
    }

    @Override
    protected void processOtherEvent(final SimEvent ev) {
	switch (ev.getTag()) {
	    case LOG_TAG:
		if (CloudSim.clock() < getLifeLength()) {
		    logUtilisation();
		    send(getId(), logPeriod, LOG_TAG);
		}
		break;
	    case TIMER_TAG:
		if (!logStarted) {
		    logStarted = true;
		    send(getId(), offset, LOG_TAG);
		}
		break;
	}
	super.processOtherEvent(ev);
    }

    private void logUtilisation() {
	// If no cloudlet has been submitted or finished - then there is nothing
	// new to log
	double currTime = CloudSim.clock();
	if (currTime - lastTimeCloudletReturned < getStepPeriod() && currTime - lastTimeCloudletSubmited < getStepPeriod()) {
	    for (ILoadBalancer balancer : getLoadBalancers().values()) {
		for (HddVm vm : balancer.getAppServers()) {
		    logUtilisation(vm);
		}
		for (HddVm vm : balancer.getDbBalancer().getVMs()) {
		    logUtilisation(vm);
		}
	    }
	}
    }

    private void logUtilisation(final HddVm vm) {
	final Double time = CloudSim.clock();
	final Integer vmId = vm.getId();
	final Double percentCPU = 100 * evaluateCPUUtilization(vm);
	final Double percentIO = 100 * evaluateIOUtilization(vm);
	final Double percentRAM = 100 * evaluateRAMUtilization(vm);

	if (!headerPrinted) {
	    CustomLog.printLine(TextUtil.getCaptionLine(HEADER_NAMES,
		    HEADER_TYPES,
		    TextUtil.DEFAULT_DELIM));
	    headerPrinted = true;
	}

	CustomLog.printLine(
		TextUtil.getTxtLine(Arrays.asList(time, vmId, percentCPU, percentIO, percentRAM),
			HEADER_NAMES,
			TextUtil.DEFAULT_DELIM, false));

    }

    private static double evaluateCPUUtilization(final HddVm vm) {
	double sumExecCloudLets = 0;
	for (HddResCloudlet cloudlet : vm.getCloudletScheduler().getCloudletExecList()) {
	    sumExecCloudLets += cloudlet.getCloudletLength();
	}
	double vmMips = vm.getMips() * vm.getNumberOfPes();
	return Math.min(1, sumExecCloudLets / vmMips);
    }

    private static double evaluateIOUtilization(final HddVm vm) {
	double sumExecCloudLets = 0;
	for (HddResCloudlet cloudlet : vm.getCloudletScheduler().getCloudletExecList()) {
	    sumExecCloudLets += cloudlet.getCloudlet().getCloudletIOLength();
	}
	double vmIOMips = vm.getIoMips();
	return Math.min(1, sumExecCloudLets / vmIOMips);
    }

    private static double evaluateRAMUtilization(final HddVm vm) {
	double sumExecCloudLets = 0;
	for (HddResCloudlet cloudlet : vm.getCloudletScheduler().getCloudletExecList()) {
	    sumExecCloudLets += cloudlet.getCloudlet().getRam();
	}
	double vmRam = vm.getRam();
	return Math.min(1, sumExecCloudLets / vmRam);
    }

}
