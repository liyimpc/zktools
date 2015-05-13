/*
 * 蘑菇街 Inc.
 * Copyright (c) 2010-2013 All Rights Reserved.
 *
 * Author     :guangming
 * Version    :1.0
 * Create Date:2015年4月15日
 */

package org.mogujie.zookeeper.tools;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.utils.CloseableUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestDistCp {
  protected TestingCluster cluster1;
  protected TestingCluster cluster2;

  @Before
  public void setup() throws Exception {
    cluster1 = new TestingCluster(2);
    cluster1.start();

    cluster2 = new TestingCluster(2);
    cluster2.start();
  }

  @After
  public void teardown() throws Exception {
    CloseableUtils.closeQuietly(cluster1);
    CloseableUtils.closeQuietly(cluster2);
  }

  @Test
  public void testDistCopy() throws Exception {
    CuratorFramework client1 = CuratorFrameworkFactory.builder()
        .connectString(cluster1.getConnectString())
        .canBeReadOnly(true)
        .retryPolicy(new ExponentialBackoffRetry(100, 3))
        .build();

    CuratorFramework client2 = CuratorFrameworkFactory.builder()
        .connectString(cluster2.getConnectString())
        .canBeReadOnly(true)
        .retryPolicy(new ExponentialBackoffRetry(100, 3))
        .build();
    try {
      client1.start();
      client2.start();

      client1.create().forPath("/test", "test".getBytes());
      client1.create().forPath("/test/test1", "test1".getBytes());
      client1.create().forPath("/test/test2", "test2".getBytes());

      DistCp.copy(client1, client2, "/test");

      Assert.assertEquals("test", new String(client2.getData().forPath("/test")));
      Assert.assertEquals("test1", new String(client2.getData().forPath("/test/test1")));
      Assert.assertEquals("test2", new String(client2.getData().forPath("/test/test2")));

    } finally {
      CloseableUtils.closeQuietly(client1);
      CloseableUtils.closeQuietly(client2);
    }
  }

  @Test
  public void testDistCopyWithPrefix() throws Exception {
    CuratorFramework client1 = CuratorFrameworkFactory.builder()
        .connectString(cluster1.getConnectString())
        .canBeReadOnly(true)
        .retryPolicy(new ExponentialBackoffRetry(100, 3))
        .build();

    CuratorFramework client2 = CuratorFrameworkFactory.builder()
        .connectString(cluster2.getConnectString())
        .canBeReadOnly(true)
        .retryPolicy(new ExponentialBackoffRetry(100, 3))
        .build();
    try {
      client1.start();
      client2.start();

      client1.create().forPath("/test", "test".getBytes());
      client1.create().forPath("/test/test1", "test1".getBytes());
      client1.create().forPath("/test/test2", "test2".getBytes());

      DistCp.copy(client1, client2, "/test", "/bda");

      Assert.assertEquals("test", new String(client2.getData().forPath("/bda/test")));
      Assert.assertEquals("test1", new String(client2.getData().forPath("/bda/test/test1")));
      Assert.assertEquals("test2", new String(client2.getData().forPath("/bda/test/test2")));

    } finally {
      CloseableUtils.closeQuietly(client1);
      CloseableUtils.closeQuietly(client2);
    }
  }
}
