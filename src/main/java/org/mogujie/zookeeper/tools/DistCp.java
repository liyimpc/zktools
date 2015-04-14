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

  protected ZooKeeper sourceZk;
  protected ZooKeeper destinationZk;

  public boolean getPrintWatches() {
    return printWatches;
  }

  static void usage() {
    DistCp.printMessage("DistCp -from host:port -to host:port "
        + "-path path [watch] [-timeout time]");
  }

  private class MyWatcher implements Watcher {
    @Override
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

    private Map<String, String> options = new HashMap<String, String>();
    private List<String> cmdArgs = null;
    private String command = null;

    public MyCommandOptions() {
      options.put("timeout", "30000");
      options.put("path", "/");
      options.put("watch", String.valueOf(false));
    }

    public String getOption(String opt) {
      return options.get(opt);
    }

    public String getCommand() {
      return command;
    }

    public String getCmdArgument(int index) {
      return cmdArgs.get(index);
    }

    public int getNumArguments() {
      return cmdArgs.size();
    }

    public String[] getArgArray() {
      return cmdArgs.toArray(new String[0]);
    }

    /**
     * Parses a command line that may contain one or more flags before an
     * optional command string
     *
     * @param args
     *          command line arguments
     * @return true if parsing succeeded, false otherwise.
     */
    public boolean parseOptions(String[] args) {
      List<String> argList = Arrays.asList(args);
      Iterator<String> it = argList.iterator();

      while (it.hasNext()) {
        String opt = it.next();
        try {
          if (opt.equals("-from")) {
            options.put("source", it.next());
          } else if (opt.equals("-to")) {
            options.put("destination", it.next());
          } else if (opt.equals("-path")) {
            options.put("path", it.next());
          } else if (opt.equals("watch")) {
            options.put("watch", String.valueOf(true));
          } else if (opt.equals("-h") || opt.equals("-help")) {
            return false;
          } else if (opt.equals("-timeout")) {
            options.put("timeout", it.next());
          }
        } catch (NoSuchElementException e) {
          System.err.println("Error: no argument found for option " + opt);
          return false;
        }
      }

      if (options.get("source") == null || options.get("destination") == null) {
        System.err.println("argument -from and -to must be set");
        return false;
      }

      return true;
    }
  }

  public static void printMessage(String msg) {
    System.out.println("\n" + msg);
  }

  /**
   * Connect to zk server via host
   *
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
    return new ZooKeeper(host, Integer.parseInt(cl.getOption("timeout")),
        new MyWatcher(), readOnly);
  }

  protected ZooKeeper connectToZK(ZooKeeper zk, String host)
      throws InterruptedException, IOException {
    return connectToZK(zk, host, false);
  }

  public static void main(String args[]) throws KeeperException, IOException,
      InterruptedException {
    DistCp distCp = new DistCp(args);
    distCp.distCopy();
    distCp.clear();
    DistCp.printMessage("copy fininshed");
  }

  public DistCp(String args[]) throws IOException, InterruptedException {
    if (!cl.parseOptions(args)) {
      usage();
      System.exit(0);
    }

    // connect to source zk, we should enable readOnly mode
    sourceZk = connectToZK(sourceZk, cl.getOption("source"), false);
    if (sourceZk == null) {
      LOG.error("Error connectting to " + cl.getOption("source"));
      throw new IOException("Error connectting to " + cl.getOption("source"));
    }
    LOG.info("Connecting to " + cl.getOption("source"));

    // connect to destination zk
    destinationZk = connectToZK(destinationZk, cl.getOption("destination"));
    if (destinationZk == null) {
      LOG.error("Error connectting to " + cl.getOption("destination"));
      throw new IOException("Error connectting to "
          + cl.getOption("destination"));
    }
    LOG.info("Connecting to " + cl.getOption("destination"));
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
      Stat stat = new Stat();
      byte[] data = sourceZk.getData(parent, watch, stat);
      // if this znode is Ephemeral, we don't need to copy it
      if (stat.getEphemeralOwner() != 0) {
        LOG.info("znode " + stat.toString() + " is Ephemeral");
        continue;
      }
      List<ACL> acls = sourceZk.getACL(parent, new Stat());
      destinationZk.create(parent, data, acls, createMode);
      LOG.info("create path " + parent + "successfully!");

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
    if (sourceZk != null && sourceZk.getState().isAlive()) {
      LOG.info("Close " + cl.getOption("source"));
      sourceZk.close();
    }

    if (destinationZk != null && destinationZk.getState().isAlive()) {
      LOG.info("Close " + cl.getOption("destination"));
      destinationZk.close();
    }
  }
}
