package org.apache.storm.scheduler;


import org.apache.storm.container.cgroup.core.MemoryCore.Stat;
import org.apache.storm.shade.org.apache.zookeeper.CreateMode;
import org.apache.storm.shade.org.apache.zookeeper.KeeperException;
import org.apache.storm.shade.org.apache.zookeeper.ZooDefs;
import org.apache.storm.shade.org.apache.zookeeper.ZooKeeper;
import org.apache.storm.shade.org.apache.zookeeper.ZooKeeper.States;

import java.io.IOException;
import java.util.List;

public class SimpleZookeeperClient {

    private static final int IGNORE_VERSION = -1;
    private static final int SESSION_TIMEOUT = 3000;
    private static final byte[] EMPTY_NODE = "".getBytes();

    private ZooKeeper zk;

    public SimpleZookeeperClient(List<String> servers, int port) throws IOException {

        String connectionString = "";
        int added = 0;

        for(String s : servers){
            if (added != 0){
                connectionString += ",";
            }

            connectionString += s + ":" + port;
            added++;
        }

        zk = new ZooKeeper(connectionString, SESSION_TIMEOUT, new SimpleWatcher());

       // zk =new ZooKeeper(connectionString,SESSION_TIMEOUT,watchedEvent -> e);

    }

    public boolean isConnected(){
        States s = zk.getState();

        return ((States) s).isConnected();
    }

    private String fullPath(String path, String child){

        String childAbsPath = path;
        if (!path.endsWith("/")){
            childAbsPath += "/";
        }
        childAbsPath += child;

        return childAbsPath;

    }

    private String parentPath(String dirname){

        if (dirname == null){
            return null;
        }

        String[] parts = dirname.split("/");
        String parent = "";

        if (parts.length < 3){
            /* dirname was /something, no parent */
            return null;
        }

        /* everything but last */
        for (int i = 1; i < (parts.length - 1); i++){
            if (parts[i].equals(""))
                continue;

            parent += "/" + parts[i];
        }

        return parent;

    }

    public void mkdirs(String dirname) throws KeeperException, InterruptedException{

        if (dirname == null)
            return;

        if (!dirname.equals("/") && (zk.exists(dirname, false) == null)){
            mkdirs(parentPath(dirname));
            System.out.println(".... SimpleZookeeper  mkdirs..00.dirname"+ dirname);

            setData(dirname, EMPTY_NODE);
        }
    }

    public boolean exists(String dirname){

        boolean exists = false;

        if (dirname == null)
            return exists;

        try {
            exists = (zk.exists(dirname, false) != null);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return exists;

    }

    public void deleteRecursive(String dirname){

        try {
            if (zk.exists(dirname, false) != null){

                List<String> children = zk.getChildren(dirname, false);


                if (children != null && !children.isEmpty()){
                    for(String child : children){
                        deleteRecursive(fullPath(dirname, child));
                    }
                }

                zk.delete(dirname, IGNORE_VERSION);

            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Set data to a zookeeper path; if node does not exist it will be created.
     *
     * The parentPath must exists.
     *
     * @param path
     * @param data
     * @return
     */
    public Stat setData(String path, byte[] data){
        try {


            //System.out.println(".... SimpleZookeeper  setData..."+path +"::"+ data);
           // System.out.println(".... SimpleZookeeper  setData..."+data);

            if (zk.exists(path, false) != null){
               // return zk.setData(path, data, IGNORE_VERSION);
                System.out.println(".... SimpleZookeeper  setData..00." +path +"::"+ data);
                return null;
            }else{
                zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                System.out.println(".... SimpleZookeeper  setData..11."+path +"::"+ data);

                //return new Stat();
                return null;
            }

        } catch (KeeperException e){
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return null;
    }


    /**
     * Retrieve data from a zookeeper node
     *
     * @param path
     * @return
     */

//    public byte[] getData(String path){
//
//        Stat zkStat = new Stat();
//        byte[] returnedValue = null;
//
//        try {
//            returnedValue = zk.getData(path, false, zkStat);
//            //int a=0;
//        } catch (KeeperException e) {
//            e.printStackTrace();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//
//        return returnedValue;
//    }


    /**
     * Retrieve children of a zookeeper path
     *
     * @param path
     * @return
     */
    public List<String> getChildren(String path){

        List<String> returnedValue = null;
        try {
            returnedValue = zk.getChildren(path, false);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return returnedValue;
    }



    public ZooKeeper getZK(){
        return zk;
    }

}
