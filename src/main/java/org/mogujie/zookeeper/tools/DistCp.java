package org.mogujie.zookeeper.tools;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Zookeeper distCopy.
 *
 */
public class DistCp {
    private static final Logger LOG = LoggerFactory.getLogger(DistCp.class);

    protected MyCommandOptions cl = new MyCommandOptions();
    protected boolean printWatches = true;

    protected ZooKeeper myZk;
    protected ZooKeeper sourceZk;

    public boolean getPrintWatches( ) {
        return printWatches;
    }

    static void usage() {
        DistCp.printMessage("DistCp -server host:port -from host:port -path path [watch]");
    }

    private class MyWatcher implements Watcher {
        public void process(WatchedEvent event) {
            if (getPrintWatches()) {
                DistCp.printMessage("WATCHER::");
                DistCp.printMessage(event.toString());
            }
        }
    }

    /**
     * A storage class for both command line options and shell commands.
     *
     */
    static class MyCommandOptions {

        private Map<String,String> options = new HashMap<String,String>();
        private List<String> cmdArgs = null;
        private String command = null;

        public MyCommandOptions() {
          options.put("server", "localhost:2181");
          options.put("timeout", "30000");
          options.put("watch", String.valueOf(false));
        }

        public String getOption(String opt) {
            return options.get(opt);
        }

        public String getCommand( ) {
            return command;
        }

        public String getCmdArgument( int index ) {
            return cmdArgs.get(index);
        }

        public int getNumArguments( ) {
            return cmdArgs.size();
        }

        public String[] getArgArray() {
            return cmdArgs.toArray(new String[0]);
        }

        /**
         * Parses a command line that may contain one or more flags
         * before an optional command string
         * @param args command line arguments
         * @return true if parsing succeeded, false otherwise.
         */
        public boolean parseOptions(String[] args) {
            List<String> argList = Arrays.asList(args);
            Iterator<String> it = argList.iterator();

            while (it.hasNext()) {
                String opt = it.next();
                try {
                    if (opt.equals("-server")) {
                        options.put("server", it.next());
                    } else if (opt.equals("-timeout")) {
                        options.put("timeout", it.next());
                    } else if (opt.equals("-from")) {
                        options.put("from", it.next());
                    } else if (opt.equals("-path")) {
                        options.put("path", it.next());
                    } else if (opt.equals("watch")) {
                        options.put("watch", String.valueOf(true));
                    } else if (opt.equals("-h") || opt.equals("-help")) {
                        usage();
                        return false; 
                    }
                } catch (NoSuchElementException e){
                    System.err.println("Error: no argument found for option "
                            + opt);
                    return false;
                }

//                if (!opt.startsWith("-")) {
//                    command = opt;
//                    cmdArgs = new ArrayList<String>( );
//                    cmdArgs.add( command );
//                    while (it.hasNext()) {
//                        cmdArgs.add(it.next());
//                    }
//                    return true;
//                }
            }
            return true;
        }
    }

    public static void printMessage(String msg) {
        System.out.println("\n"+msg);
    }

    /**
     * Connect to zk server via host
     * @param zk
     * @param host
     * @param readOnly
     * @return host
     */
    protected ZooKeeper connectToZK(ZooKeeper zk, String host, boolean readOnly) 
            throws InterruptedException, IOException {
        if (zk != null && zk.getState().isAlive()) {
            zk.close();
        }
        return new ZooKeeper(host,
                 Integer.parseInt(cl.getOption("timeout")),
                 new MyWatcher(), readOnly);
    }
    
    protected ZooKeeper connectToZK(ZooKeeper zk, String host) 
            throws InterruptedException, IOException {
        return connectToZK(zk, host, false);
    }
    
    public static void main(String args[])
        throws KeeperException, IOException, InterruptedException {
        DistCp distCp = new DistCp(args);
        distCp.distCopy();
        distCp.clear();
    }

    public DistCp(String args[]) throws IOException, InterruptedException {
        if (!cl.parseOptions(args)) {
            return;
        }
        myZk = connectToZK(myZk, cl.getOption("server"));
        if (myZk == null) {
            throw new IOException("Error connectting to " + cl.getOption("server"));
        }
        DistCp.printMessage("Connecting to " + cl.getOption("server"));
        
        // connect to source zk, we should enable readOnly mode
        sourceZk = connectToZK(sourceZk, cl.getOption("from"), false);
        if (sourceZk == null) {
            throw new IOException("Error connectting to " + cl.getOption("from"));
        }
        DistCp.printMessage("Connecting to " + cl.getOption("from"));
    }

    public void distCopy() throws InterruptedException, KeeperException {
        String path = cl.getOption("path");
        boolean watch = Boolean.getBoolean(cl.getOption("watch"));
        CreateMode createMode = CreateMode.PERSISTENT;
        
        // BFS traverse
        Queue<String> queue = new LinkedList<String>();
        if (sourceZk.exists(path, watch) != null) {
            queue.add(path);
        } 
        while (!queue.isEmpty()) {
            String parent = queue.poll();
            byte[] data = sourceZk.getData(parent, watch, new Stat());
            List<ACL> acls = sourceZk.getACL(parent, new Stat());
            myZk.create(parent, data, acls, createMode);
            
            List<String> childs = sourceZk.getChildren(parent, watch);
            if (childs != null && !childs.isEmpty()) {
                for (String child : childs) {
                    String absoluteChild = parent + "/" + child;
                    queue.add(absoluteChild);
                }
            }
        }
    }
    
    public void clear() throws InterruptedException {
        if (myZk != null && myZk.getState().isAlive()) {
            myZk.close();
        }
        
        if (sourceZk != null && sourceZk.getState().isAlive()) {
            sourceZk.close();
        }
    }
}
