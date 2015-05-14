/*
 * 蘑菇街 Inc.
 * Copyright (c) 2010-2013 All Rights Reserved.
 *
 * Author     :guangming
 * Version    :1.0
 * Create Date:2015年4月15日
 */

package org.mogujie.zookeeper.tools;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.api.transaction.CuratorTransactionBridge;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
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
  public static final int TIMEOUT_DEFAULT = 30000;

  protected MyCommandOptions cl = new MyCommandOptions();
  protected boolean printWatches = true;

  protected CuratorFramework sourceClient;
  protected CuratorFramework destinationClient;

  public boolean getPrintWatches() {
    return printWatches;
  }

  static void usage() {
    LOG.info("DistCp -from host:port -to host:port "
        + "-srcpath srcpath [-despath despath] [watch] [-timeout time]");
  }

  private class MyWatcher implements Watcher {
    @Override
    public void process(WatchedEvent event) {
      if (getPrintWatches()) {
        LOG.info("WATCHER::");
        LOG.info(event.toString());
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
      options.put("timeout", String.valueOf(TIMEOUT_DEFAULT));
      options.put("srcpath", "/");
      options.put("prefix", "");
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
          } else if (opt.equals("-srcpath")) {
            options.put("srcpath", it.next());
          } else if (opt.equals("-despath")) {
            options.put("despath", it.next());
          } else if (opt.equals("-prefix")) {
            options.put("prefix", it.next());
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

  protected CuratorFramework getClient(String connectString, boolean readOnly, int timeout) {
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    CuratorFramework client = CuratorFrameworkFactory.builder()
        .connectString(connectString)
        .sessionTimeoutMs(timeout)
        .connectionTimeoutMs(timeout)
        .canBeReadOnly(readOnly)
        .retryPolicy(retryPolicy)
        .build();
    return client;
  }

  public static void main(String args[]) throws Exception {
    DistCp distCp = new DistCp(args);
    distCp.distCopy();
    LOG.info("copy fininshed");
    System.out.println("copy fininshed");
  }

  public DistCp(String args[]) throws IOException, InterruptedException {
    if (!cl.parseOptions(args)) {
      usage();
      System.exit(0);
    }

    // get source curator client, we should enable readOnly mode
    sourceClient = getClient(cl.getOption("source"), true,
       Integer.valueOf(cl.getOption("timeout")));
    if (sourceClient == null) {
      LOG.error("Error connectting to " + cl.getOption("source"));
      throw new IOException("Error connectting to " + cl.getOption("source"));
    }
    LOG.info("Connecting to " + cl.getOption("source"));

    // get destination curator client
    destinationClient = getClient(cl.getOption("destination"), false,
        Integer.valueOf(cl.getOption("timeout")));
    if (destinationClient == null) {
      LOG.error("Error connectting to " + cl.getOption("destination"));
      throw new IOException("Error connectting to "
          + cl.getOption("destination"));
    }
    LOG.info("Connecting to " + cl.getOption("destination"));
  }

  /**
   * copy from client 'from' to client 'to'
   *
   * @param from
   * @param to
   * @param path
   * @throws Exception
   */
  public static void copy(CuratorFramework from, CuratorFramework to,
      String path) throws Exception {
    copy(from, to, path, path);
  }

  /**
   * copy from client 'from' to client 'to'
   *
   * @param from
   * @param to
   * @param srcpath
   * @param despath
   * @throws Exception
   */
  public static void copy(CuratorFramework from, CuratorFramework to,
      String srcpath, String despath)
      throws Exception {
    CreateMode createMode = CreateMode.PERSISTENT;
    boolean firstCreate = true;

    // if parent of destination path is not existed,
    // first create parent path
    String prefix = despath.substring(0, despath.lastIndexOf("/"));
    if (prefix != null && !prefix.isEmpty()
        && to.checkExists().forPath(prefix) == null ) {
      to.create().creatingParentsIfNeeded().forPath(prefix);
    }

    // start transaction
    CuratorTransaction transaction = to.inTransaction();
    CuratorTransactionBridge bridge = null;

    LOG.info("start discopy...");
    System.out.println("start distCopy...");

    // BFS traverse
    Queue<MyPair> queue = new LinkedList<MyPair>();
    if (from.checkExists().forPath(srcpath) != null) {
      MyPair pairParent = new MyPair();
      pairParent.setSrcParent(srcpath);
      pairParent.setDesParent(despath);
      queue.add(pairParent);
    }
    while (!queue.isEmpty()) {
      MyPair pair = queue.poll();
      String srcparent = pair.getSrcParent();
      String desparent = pair.getDesParent();

      Stat stat = from.checkExists().forPath(srcparent);
      byte[] data = from.getData().forPath(srcparent);
      // if this znode is Ephemeral, we don't need to copy it
      if (stat.getEphemeralOwner() != 0) {
        LOG.info("znode " + stat.toString() + " is Ephemeral");
        continue;
      }
      List<ACL> acls = from.getACL().forPath(srcparent);

      // create znode to destination zk
      if (firstCreate) {
        bridge = transaction
          .create()
          .withMode(createMode)
          .withACL(acls)
          .forPath(desparent, data);
        firstCreate = false;
      } else {
        bridge = bridge.and()
          .create()
          .withMode(createMode)
          .withACL(acls)
          .forPath(desparent, data);
      }

      List<String> childs = from.getChildren().forPath(srcparent);
      if (childs != null && !childs.isEmpty()) {
        for (String child : childs) {
          MyPair pairChild = new MyPair();
          pairChild.setSrcParent(srcparent + "/" + child);
          pairChild.setDesParent(desparent + "/" + child);
          queue.add(pairChild);
        }
      }
    }

    // commit transaction
    Collection<CuratorTransactionResult> results = bridge.and().commit();
    for (CuratorTransactionResult result : results) {
      LOG.info(result.getForPath() + " - " + result.getType());
    }
  }

  public void distCopy() throws Exception {
    String srcpath = cl.getOption("srcpath");
    String despath = cl.getOption("despath");
//    boolean watch = Boolean.getBoolean(cl.getOption("watch"));
    try {
      // 1. open client
      if (sourceClient.getState() != CuratorFrameworkState.STARTED) {
        sourceClient.start();
      }
      if (destinationClient.getState() != CuratorFrameworkState.STARTED) {
        destinationClient.start();
      }

      // 2. copy from source to destination
      if (despath != null && !despath.isEmpty()){
        copy(sourceClient, destinationClient, srcpath, despath);
      } else {
        copy(sourceClient, destinationClient, srcpath);
      }

    } finally {
      // 3. close client
      CloseableUtils.closeQuietly(sourceClient);
      sourceClient = null;
      LOG.info("Close " + cl.getOption("source"));

      CloseableUtils.closeQuietly(destinationClient);
      destinationClient = null;
      LOG.info("Close " + cl.getOption("destination"));
    }
  }
}
