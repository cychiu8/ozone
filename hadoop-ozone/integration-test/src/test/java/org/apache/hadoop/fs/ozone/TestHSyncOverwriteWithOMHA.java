/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.ozone;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.client.OzoneQuota;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.DataTestUtil;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * HDDS-16765: a rejected overwrite commit leaves OVERWRITTEN_HSYNC_KEY on another writer's open
 * entry in the open file table cache until the double buffer flushes and evicts that epoch.
 *
 * Client sequence (FileSystem API only), bucket namespace quota = 2:
 *   A: create f, write, hsync          (used namespace 1)
 *   B: create f (overwrite), write     (create passes: 1 + 1 <= 2)
 *   C: create g, close                 (used namespace 2)
 *   B: close                           -> QUOTA_EXCEEDED, but A's open entry is already flagged
 *   A: write, hsync, close
 *
 * Each test only assumes what a real OM ring does on its own: every OM flushes its double
 * buffer at its own pace.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestHSyncOverwriteWithOMHA {
  private static final OzoneConfiguration CONF = new OzoneConfiguration();
  private static final String OM_SERVICE_ID = "omservice-hsync-overwrite";
  private static MiniOzoneHAClusterImpl cluster;
  private static OzoneClient client;
  private static FileSystem fs;
  private static int seq;

  @BeforeAll
  public static void init() throws Exception {
    CONF.setBoolean(OzoneConfigKeys.OZONE_HBASE_ENHANCEMENTS_ALLOWED, true);
    CONF.setBoolean("ozone.client.hbase.enhancements.allowed", true);
    CONF.setBoolean(OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED, true);
    cluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newHABuilder(CONF)
        .setOMServiceId(OM_SERVICE_ID).setNumOfOzoneManagers(3).setNumDatanodes(3).build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
    OzoneConfiguration fsConf = new OzoneConfiguration(cluster.getConf());
    fsConf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
        String.format("%s://%s/", OZONE_OFS_URI_SCHEME, OM_SERVICE_ID));
    fs = FileSystem.get(fsConf);
  }

  @AfterAll
  public static void teardown() {
    IOUtils.closeQuietly(fs, client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private static void log(String s) {
    System.out.println("[HSyncOverwriteHA] " + s);
  }

  private static String describe(Throwable t) {
    StringBuilder sb = new StringBuilder();
    for (Throwable c = t; c != null && c != c.getCause(); c = c.getCause()) {
      sb.append(sb.length() == 0 ? "" : " <- ").append(c.getClass().getSimpleName()).append(": ")
          .append(String.valueOf(c.getMessage()).split("\n")[0]);
    }
    return sb.toString();
  }

  private static boolean isOverwrittenRejection(Throwable t) {
    return t != null && describe(t).contains("is already deleted/overwritten");
  }

  private static Table<String, OmKeyInfo> openTable(OzoneManager om) {
    return om.getMetadataManager().getOpenKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED);
  }

  private static Table<String, OmKeyInfo> fileTable(OzoneManager om) {
    return om.getMetadataManager().getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED);
  }

  /** Entries of the open file table cache whose key names the file. */
  private static List<String> openCache(OzoneManager om, String name) {
    List<String> rows = new ArrayList<>();
    Iterator<Map.Entry<CacheKey<String>, CacheValue<OmKeyInfo>>> it = openTable(om).cacheIterator();
    while (it.hasNext()) {
      Map.Entry<CacheKey<String>, CacheValue<OmKeyInfo>> e = it.next();
      if (e.getKey().getCacheKey().contains("/" + name + "/")) {
        OmKeyInfo v = e.getValue().getCacheValue();
        rows.add("epoch=" + e.getValue().getEpoch() + " " + (v == null ? "tombstone"
            : OzoneConsts.OVERWRITTEN_HSYNC_KEY + "=" + v.getMetadata().get(OzoneConsts.OVERWRITTEN_HSYNC_KEY)));
      }
    }
    return rows;
  }

  /** What RocksDB holds for the file on one OM (table iterators bypass the table cache). */
  private static String dumpDb(OzoneManager om, String name) throws Exception {
    Map<String, String> rows = new TreeMap<>();
    try (Table.KeyValueIterator<String, OmKeyInfo> it = fileTable(om).iterator()) {
      while (it.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> kv = it.next();
        if (kv.getKey().endsWith("/" + name)) {
          OmKeyInfo v = kv.getValue();
          rows.put("fileTable", "{dataSize=" + v.getDataSize() + ", updateID=" + v.getUpdateID()
              + ", hsyncClientId=" + v.getMetadata().get(OzoneConsts.HSYNC_CLIENT_ID) + "}");
        }
      }
    }
    List<String> open = new ArrayList<>();
    try (Table.KeyValueIterator<String, OmKeyInfo> it = openTable(om).iterator()) {
      while (it.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> kv = it.next();
        if (kv.getKey().contains("/" + name + "/")) {
          open.add("{client=" + kv.getKey().substring(kv.getKey().lastIndexOf('/') + 1)
              + ", dataSize=" + kv.getValue().getDataSize() + ", updateID=" + kv.getValue().getUpdateID() + "}");
        }
      }
    }
    rows.put("openFileTable", open.toString());
    return rows.toString();
  }

  private static long readLen(OzoneManager om, OzoneBucket bucket, String name) {
    try {
      return om.getFileStatus(new OmKeyArgs.Builder().setVolumeName(bucket.getVolumeName())
          .setBucketName(bucket.getName()).setKeyName(name).build()).getKeyInfo().getDataSize();
    } catch (Exception e) {
      return -1;
    }
  }

  private static void awaitFlushed(OzoneManager om, String name) throws Exception {
    GenericTestUtils.waitFor(() -> {
      try {
        om.awaitDoubleBufferFlush();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return openCache(om, name).isEmpty();
    }, 100, 30000);
  }

  /** Outcome of one run. */
  private static final class Outcome {
    private Throwable bClose;
    private Throwable aHsync;
    private Throwable aClose;
    private final Map<String, String> db = new TreeMap<>();
    private final Map<String, Long> len = new TreeMap<>();

    boolean replicasAgree() {
      return db.values().stream().distinct().count() == 1;
    }
  }

  private OzoneBucket newBucket() throws Exception {
    OzoneBucket bucket = DataTestUtil.createVolumeAndBucket(client, BucketLayout.FILE_SYSTEM_OPTIMIZED);
    bucket.setQuota(OzoneQuota.parseQuota("10GB", "2"));
    return bucket;
  }

  /**
   * @param lagging OM whose double buffer flush lags behind (null: none)
   * @param race true: no waiting at all, A closes right behind B's rejected close
   */
  private Outcome run(String label, OzoneManager lagging, boolean race) throws Exception {
    OzoneBucket bucket = newBucket();
    String name = "f" + (++seq);
    Path dir = new Path("/" + bucket.getVolumeName() + "/" + bucket.getName());
    Path file = new Path(dir, name);
    Outcome o = new Outcome();
    if (!race) {
      log("---- " + label + " file=" + file + " laggingOM=" + (lagging == null ? "none" : lagging.getOMNodeId())
          + " leader=" + cluster.getOMLeader().getOMNodeId());
    }

    FSDataOutputStream a = fs.create(file, true);
    a.write(new byte[100]);
    a.hsync();
    FSDataOutputStream b = fs.create(file, true);
    b.write(new byte[7]);
    fs.create(new Path(dir, "g" + seq), true).close();
    if (!race) {
      for (OzoneManager om : cluster.getOzoneManagersList()) {
        awaitFlushed(om, name);
      }
      log("1. A: create+write(100)+hsync; B: create(overwrite)+write(7); C: create+close g. namespace used="
          + client.getObjectStore().getVolume(bucket.getVolumeName()).getBucket(bucket.getName()).getUsedNamespace()
          + " quota=2");
    }

    if (race) {
      // No wait and no hook: A closes right behind B's rejected close.
      try {
        b.close();
      } catch (Throwable e) {
        o.bClose = e;
      }
    } else {
      if (lagging != null) {
        lagging.getOmRatisServer().getOmStateMachine().getOzoneManagerDoubleBuffer().pause();
      }
      try {
        b.close();
      } catch (Throwable e) {
        o.bClose = e;
      }
      log("2. B: close -> " + (o.bClose == null ? "OK" : describe(o.bClose)));
      Thread.sleep(2000);
      for (OzoneManager om : cluster.getOzoneManagersList()) {
        if (om != lagging) {
          awaitFlushed(om, name);
        }
        log("   " + om.getOMNodeId() + " open file table cache entries for " + name + ": " + openCache(om, name));
      }
      try {
        a.write(new byte[50]);
        a.hsync();
      } catch (Throwable e) {
        o.aHsync = e;
      }
      log("3. A: write(50)+hsync -> " + (o.aHsync == null ? "OK" : describe(o.aHsync)));
    }
    try {
      a.close();
    } catch (Throwable e) {
      o.aClose = e;
    }
    if (!race) {
      log("4. A: close -> " + (o.aClose == null ? "OK" : describe(o.aClose)));
      Thread.sleep(2000);
    }
    if (lagging != null) {
      lagging.getOmRatisServer().getOmStateMachine().getOzoneManagerDoubleBuffer().unpause();
    }
    if (race && !isOverwrittenRejection(o.aHsync) && !isOverwrittenRejection(o.aClose)) {
      return o;
    }
    Thread.sleep(1000);
    for (OzoneManager om : cluster.getOzoneManagersList()) {
      awaitFlushed(om, name);
      o.db.put(om.getOMNodeId(), dumpDb(om, name));
      o.len.put(om.getOMNodeId(), readLen(om, bucket, name));
    }
    if (race) {
      log("---- " + label + " file=" + file + " leader=" + cluster.getOMLeader().getOMNodeId());
      log("B: close -> " + (o.bClose == null ? "OK" : describe(o.bClose)));
      log("A: hsync -> " + (o.aHsync == null ? "OK" : describe(o.aHsync)));
      log("A: close -> " + (o.aClose == null ? "OK" : describe(o.aClose)));
    }
    log("5. all OMs flushed and evicted. RocksDB per OM:");
    o.db.forEach((id, rows) -> log("   " + id + " " + rows + " getFileStatus.dataSize=" + o.len.get(id)));
    log("   replicas agree: " + o.replicasAgree());
    return o;
  }

  private OzoneManager follower() throws Exception {
    OzoneManager leader = cluster.getOMLeader();
    for (OzoneManager om : cluster.getOzoneManagersList()) {
      if (om != leader) {
        return om;
      }
    }
    throw new IllegalStateException("no follower");
  }

  /** Level 0: no hook, no waiting. B's close is rejected, then A closes at once. */
  @Test
  @Order(1)
  public void level0Race() throws Exception {
    int rounds = Integer.getInteger("hsync.overwrite.rounds", 40);
    int hit = 0;
    int diverged = 0;
    int bRejected = 0;
    for (int i = 0; i < rounds; i++) {
      Outcome o = run("LEVEL 0 race round " + i, null, true);
      if (o.bClose != null && describe(o.bClose).contains("namespace quota")) {
        bRejected++;
      }
      if (isOverwrittenRejection(o.aHsync) || isOverwrittenRejection(o.aClose)) {
        hit++;
        if (!o.replicasAgree()) {
          diverged++;
        }
      }
    }
    log("LEVEL 0 SUMMARY: rounds=" + rounds + " B rejected with QUOTA_EXCEEDED=" + bRejected
        + " A rejected as 'already deleted/overwritten'=" + hit + " rounds with diverged replicas=" + diverged);
    if (hit > 0) {
      log("BUG HDDS-16765 TRIGGERED (level 0): writer A was rejected as overwritten although the overwrite"
          + " was rejected");
    }
  }

  /** Control: every OM has flushed and evicted before A continues. */
  @Test
  @Order(2)
  public void controlAllFlushed() throws Exception {
    Outcome o = run("CONTROL (all OMs flushed before A continues)", null, false);
    log("CONTROL: B rejected=" + (o.bClose != null) + " A hsync ok=" + (o.aHsync == null) + " A close ok="
        + (o.aClose == null) + " replicas agree=" + o.replicasAgree());
  }

  /** Level 1: one follower's flush lags (existing test hook OzoneManagerDoubleBuffer#pause). */
  @Test
  @Order(3)
  public void level1FollowerFlushLags() throws Exception {
    Outcome o = run("LEVEL 1 follower flush lags", follower(), false);
    if (o.bClose != null && o.aHsync == null && o.aClose == null && !o.replicasAgree()) {
      log("BUG HDDS-16765 TRIGGERED (level 1, follower): client A saw success, but the OM RocksDBs differ");
    }
  }

  /** Level 1: the leader's flush lags. */
  @Test
  @Order(4)
  public void level1LeaderFlushLags() throws Exception {
    Outcome o = run("LEVEL 1 leader flush lags", cluster.getOMLeader(), false);
    if (o.bClose != null && isOverwrittenRejection(o.aClose) && !o.replicasAgree()) {
      log("BUG HDDS-16765 TRIGGERED (level 1, leader): client A was rejected as overwritten, followers applied"
          + " its commit, and the OM RocksDBs differ");
    }
  }
}
