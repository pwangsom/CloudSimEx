package org.cloudbus.cloudsim.ex.mapreduce.policy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.Cloud;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.VmInstance;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.MapTask;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.ReduceTask;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Request;
import org.cloudbus.cloudsim.ex.mapreduce.policy.Policy.CloudDeploymentModel;

public class FIX {

	public Boolean runAlgorithm(Cloud cloud, Request request) {
		
		CloudDeploymentModel cloudDeploymentModel = request.getCloudDeploymentModel();

		// Fill VPList
		int numTasks = request.job.mapTasks.size() + request.job.reduceTasks.size();
		// List<VmInstance> VPList = Policy.getAllVmInstances(cloud, request, cloudDeploymentModel, numTasks);
		List<VmInstance> VPList = Policy.getAllVmInstances(cloud, request, numTasks);

		// Allocation
		// boolean isJobAlloc = false;
		boolean isJobAlloc = ProvisioningVMs(request, VPList);
		isJobAlloc =  MapTasksAlloc(request, VPList);
		ReduceTasksAlloc(request, VPList);
		
/*		while (isJobAlloc == false) {
			// Allocate all Map Tasks
			boolean isMapAlloc = ProvisioningVMs(request, VPList);
			
			if(isMapAlloc){
				isMapAlloc = MapTasksAlloc(request, VPList);
			}
			
			if (isMapAlloc) {
				// Allocate all reduce Tasks
				ReduceTasksAlloc(request, VPList);

				// Calculate the execution time for each map task
				List<Double> mapET = new ArrayList<Double>();
				for (int i = 0; i < request.job.mapTasks.size(); i++)
					// mapET.add(getMapExecutionTime(request.job.mapTasks.get(i),
					// request, cloud));
					mapET.add(request.job.mapTasks.get(i).getTaskExecutionTimeInSeconds());

				// Map Finish time = The max of mapETs
				double mapFT = 0.0;
				for (Double oneMapET : mapET) {
					mapFT = Math.max(mapFT, oneMapET);
				}

				// Calculate the execution time for each reduce task
				List<Double> reduceET = new ArrayList<Double>();
				for (int i = 0; i < request.job.reduceTasks.size(); i++)
					// reduceET.add(getReduceExecutionTime(request.job.reduceTasks.get(i),
					// request, cloud, mapFT));
					reduceET.add(mapFT + request.job.reduceTasks.get(i).getTaskExecutionTimeInSeconds());

				// Map Finish time = The max of mapETs
				double reduceFT = 0.0;
				for (Double oneReduceET : reduceET) {
					reduceFT = Math.max(reduceFT, oneReduceET);
				}

				if (reduceFT <= request.deadline) {
					isJobAlloc = true;
					Log.printLine("Hwang and Kim 2012 Policy: Execution Time For Request ID: " + request.id + " is: "
							+ reduceFT + " (Map Finish Time:" + mapFT + ")");
				} else {
					// FDeallocate all VMs
					request.mapAndReduceVmProvisionList = new ArrayList<VmInstance>();
					request.schedulingPlan = new HashMap<Integer, Integer>();
				}
				
				if (isJobAlloc == false)
					VPList.remove(0);
			}
		}*/
		
		if (request.getAlgoFirstSoulationFoundedTime() == null) {
			Long algoStartTime = request.getAlgoStartTime();
			Long currentTime = System.currentTimeMillis();
			request.setAlgoFirstSoulationFoundedTime((currentTime - algoStartTime));
		}
		
		return true;
	}
	
	/*
	 *  Provisioning VMs
	 */
	
	private boolean ProvisioningVMs(Request request, List<VmInstance> VPList){
		
		for(int i = 0; i < VPList.size(); i++){
			try {
				// 1- Provisioning
				VmInstance vm = VPList.get(i);
				request.mapAndReduceVmProvisionList.add(vm);				
			} catch (Exception e) {
				// TODO: handle exception
				// For any error, deallocate all VMs
				request.mapAndReduceVmProvisionList = new ArrayList<VmInstance>();
				return false;
			}
		}
		
		return true;
	}

	/*
	 * Allocate all map tasks
	 */
	private boolean MapTasksAlloc(Request request, List<VmInstance> VPList) {
/*		for (int i = 0; i < request.job.mapTasks.size(); i++) {
			try {
				// 1- Provisioning
				VmInstance vm = VPList.get(i%8);
				// 2- Scheduling
				MapTask mapTask = request.job.mapTasks.get(i);
				request.schedulingPlan.put(mapTask.getCloudletId(), vm.getId());				
			} catch (Exception e) {
				e.printStackTrace();
				// For any error, deallocate all VMs
				request.mapAndReduceVmProvisionList = new ArrayList<VmInstance>();
				request.schedulingPlan = new HashMap<Integer, Integer>();
				return false;
			}
		}*/
		return true;
	}

	/*
	 * Allocate all reduce tasks
	 */
	private void ReduceTasksAlloc(Request request, List<VmInstance> VPList) {
/*		for (int i = 0; i < request.job.reduceTasks.size(); i++) {
			// Scheduling (Provisioning already done in MapTasksAlloc)
			VmInstance vm = VPList.get(i%8);
			ReduceTask reduceTask = request.job.reduceTasks.get(i);
			request.schedulingPlan.put(reduceTask.getCloudletId(), vm.getId());
		}*/
	}
	
}
