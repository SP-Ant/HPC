package org.apache.storm.scheduler;


import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GetWorkerNodeInfo {

   // private  List<WorkerNodeUsageDetails> workerNodeUsageDetails = null;
   private static  boolean FirstRun = false;
  //  public static List<WorkerNodeUsageDetails> workerNodeUsageDetails = new ArrayList<WorkerNodeUsageDetails>();
    public static List<WorkerNodeUsageDetails> workerNodeUsageDetails = null;

    public static List<WorkerSlot> workerSlots = new ArrayList<WorkerSlot>();


    public class MyClass extends Thread {
        public void run(){
            System.out.println("MyClass running");
        }
    }


    public static List<WorkerNodeUsageDetails> getWorkernodeInfo(Topologies topologies, Cluster cluster) {





        //display the list
        List<WorkerNodeUsageDetails> workerNodeUsageDetails = null;


        List<String> CandidtaeListForMigration = new ArrayList<String>();
        Map<String, SupervisorDetails> supervisordetls = cluster.getSupervisors();


       // SupervisorDetails supd = cluster.getSupervisorById("");
        //supd.






        //cluster.getS

        for (  Map.Entry<String,  SupervisorDetails > entry : supervisordetls.entrySet()) {
            String id_supervisor = entry.getKey();





            //System.out.println(":::::::::::::::::id_supervisor::::::"+ id_supervisor);

            if (!id_supervisor.contains(".10")) {
                CandidtaeListForMigration.add(id_supervisor);
            }
           // System.out.println("::::::::::::id_supervisor:::::::::::::::::::::"+  (id_supervisor.toString()) );
        }






       return workerNodeUsageDetails;




    }


    public  static   List<SupervisorDetails>  workerNodesNeedsMigration(List<WorkerNodeUsageDetails>workerNodeUsageDetails) {

        /// this function specify the worker node wich is overutilized d return the supervisorID

        List<SupervisorDetails> supervisorDetails= null;
        return supervisorDetails;

    }



    public  static  List<SupervisorDetails> SelectworkerNodesCandidtae(String SupervisorID) {


        List<SupervisorDetails> supervisorDetails= null;


        return supervisorDetails;
    }





    public static void getUsageInfoFromHosts(String IP, String id_supervisor) {

        Double CpuUsage=0.0;
        Double MemoryUsage=0.0;

        try{
            //String command = "ls -la";
            //String command = "top";
            //String command = "ls -l";
            //String command = " top -b -d1 -n1|grep -i \"Cpu(s)\"|head -c21|cut -d ' ' -f3|cut -d '%' -f1";
            //String command = " iostat";
            String command = "top -b -n1";
            // String command = "top -i";
            String host = IP;
            String user = "ali";
            String password = "cheraakhe";
            JSch jsch = new JSch();
            Session session = jsch.getSession(user, host, 22);
            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);;
            session.setPassword(password);
            session.connect();
            Channel channel = session.openChannel("exec");
            ((ChannelExec)channel).setCommand(command);
            channel.setInputStream(null);
            ((ChannelExec)channel).setErrStream(System.err);
            InputStream input = channel.getInputStream();
            channel.connect();
            //System.out.println("Channel Connected to machine " + host + " server with command: " + command );
            try{
                InputStreamReader inputReader = new InputStreamReader(input);
                BufferedReader bufferedReader = new BufferedReader(inputReader);
                String line = null;

                while((line = bufferedReader.readLine()) != null){
                   // System.out.println(line);



                    if (line.startsWith("top") ){


                        String result = line.substring(line.indexOf("load average:") , line.length());
                        result=result.replace("load average:","");

                        String[] arrOfStr = result.split(",");
                        String tmp=arrOfStr[1].toString();
                        CpuUsage= Double.parseDouble(tmp);
                        //System.out.println("::cpuusage::IP"+ IP + " :::" + CpuUsage);




                    }

                    if (line.startsWith("KiB Swap") ) {
                        String result = line.substring(line.indexOf("used"), line.length());
                        result = result.replace("used.", "");
                        result = result.replace("avail Mem", "");
                        result = result.trim();

                        MemoryUsage= Double.parseDouble(result);
                        //System.out.println("::MemoryUsage::+IP" + IP +  ":::::" + MemoryUsage);
                    }





                   }

                bufferedReader.close();
                inputReader.close();

                WorkerNodeUsageDetails workerDetails = new WorkerNodeUsageDetails();
                workerDetails.CpuUsage=CpuUsage;
                workerDetails.MemoryUsage=MemoryUsage;
                workerDetails.SupervisorID=id_supervisor;


                workerNodeUsageDetails.add(workerDetails);

                // we should return this list  workerNodeUsageDetails






            }catch(IOException ex){
                ex.printStackTrace();
            }

            channel.disconnect();
            session.disconnect();
        }catch(Exception ex){
            ex.printStackTrace();
        }


    }


    public  static  String extractIPfromSupervisorID(String SupervisorID) {

        String IPADDRESS_PATTERN =
                "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";

        Pattern pattern = Pattern.compile(IPADDRESS_PATTERN);
        Matcher matcher = pattern.matcher(SupervisorID);
        if (matcher.find()) {
            return matcher.group();
            //System.out.println("*************Found**********"+ matcher.group().toString());
        } else{
            return "0.0.0.0";
            //System.out.println("*************Not Found*********"+ matcher.group().toString());
        }







    }







}
