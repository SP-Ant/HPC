package org.apache.storm.scheduler;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class WorkerNodeUsageDetails  {


    public   String SupervisorID;
    public   Double CpuUsage;
    public   Double MemoryUsage;


    public static Comparator<WorkerNodeUsageDetails> cpuusageComparator = new Comparator<WorkerNodeUsageDetails>() {

        public int compare(WorkerNodeUsageDetails o1, WorkerNodeUsageDetails o2) {

            return Double.compare(o1.getCpuUsage(), o2.getCpuUsage());

        }};


    public WorkerNodeUsageDetails() {
    }





    public Double getCpuUsage() {
        return CpuUsage;
    }

    public Double getMemoryUsage() {
        return MemoryUsage;
    }

    public String getSupervisorID() {
        return SupervisorID;
    }



    public void  setCpuUsage(double cpuusage) {
         this.CpuUsage = cpuusage;
    }

    public void setMemoryUsage(Double  memoryusage) {

         this.MemoryUsage = memoryusage;
    }

    public void  setSupervisorID(String supervisorid) {

        this.SupervisorID = supervisorid;
    }











    public String getId() {
        return  getSupervisorID() + getCpuUsage() + ":" + getMemoryUsage();
    }

    public List<Object> toList() {
        return Arrays.asList(CpuUsage, MemoryUsage);
    }



}
