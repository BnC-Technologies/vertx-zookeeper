package io.vertx.spi.cluster.zookeeper.impl;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.zookeeper.CreateMode;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;

public class ZKEphemeralNodeCache implements PathChildrenCacheListener {
  private PathChildrenCache cache;
  private Queue<String> keys = new ConcurrentLinkedDeque<>();
  private AtomicBoolean wasReconnecting = new AtomicBoolean(false);

  public ZKEphemeralNodeCache(String basePath, CuratorFramework curator) {
    this.cache = new PathChildrenCache(curator, basePath, true);
    this.cache.getListenable().addListener(this);
  }

  public void start() throws Exception {
    cache.start();
  }

  public void add(String keyPath) throws Exception {
    keys.add(keyPath);
  }

  @Override
  public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
    ChildData eventData = event.getData();
    switch (event.getType()) {
      case CHILD_ADDED:
        wasReconnecting.set(false);
        break;
      case CHILD_UPDATED:
        wasReconnecting.set(false);
        break;
      case CHILD_REMOVED:
        if (!wasReconnecting.get()) {
          keys.removeIf(key -> key.equals(eventData.getPath()));
        }
        wasReconnecting.set(false);
        break;
      case CONNECTION_SUSPENDED:
        wasReconnecting.set(false);
        break;
      case CONNECTION_RECONNECTED:
        wasReconnecting.set(true);
        keys.stream().map(cache::getCurrentData).forEach(childData -> {
          try {
            String path = childData.getPath();
            byte[] data = childData.getData();
            client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
              .inBackground((ignoreClient, ignoreElement) -> {
              }).forPath(path, data);
          } catch (Exception e) {
          }
        });
        break;
      case CONNECTION_LOST:
        wasReconnecting.set(false);
        break;
      case INITIALIZED:
        wasReconnecting.set(false);
        break;
    }
  }
}
