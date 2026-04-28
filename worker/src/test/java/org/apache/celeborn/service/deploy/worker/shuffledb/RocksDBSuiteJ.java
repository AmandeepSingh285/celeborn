/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.celeborn.service.deploy.worker.shuffledb;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.util.JavaUtils;
import org.apache.celeborn.service.deploy.worker.WorkerSource;

/**
 * Unit tests for {@link RocksDB} wrapper: failure handling, {@code recreateDBInstance}, and
 * continued operation after a new native RocksDB handle is opened.
 */
public class RocksDBSuiteJ {

    private static final String RESTART_FAILURE_MODE = "RESTART";
    private static final String METADATA_DB_FAIL_MODE_KEY = "celeborn.worker.metadata.db.fail.mode";

    private File dbDir;
    private StoreVersion version;
    private WorkerSource restartSource;
    private WorkerSource ignoreSource;

    @Before
    public void setUp() throws IOException {
        dbDir = new File(System.getProperty("java.io.tmpdir"), "rocksdb-wrapper-" + UUID.randomUUID());
        FileUtils.forceMkdir(dbDir);
        version = new StoreVersion(1, 0);
        CelebornConf restartConf = new CelebornConf();
        restartConf.set(METADATA_DB_FAIL_MODE_KEY, RESTART_FAILURE_MODE);
        restartSource = new WorkerSource(restartConf);
        ignoreSource = new WorkerSource(new CelebornConf());
    }

    @After
    public void tearDown() {
        if (dbDir != null) {
            try {
                JavaUtils.deleteRecursively(dbDir);
            } catch (IOException ignored) {
            }
        }
    }

    private void prepareEmptyDbOnDisk() throws IOException {
        org.rocksdb.RocksDB bootstrap = RocksDBProvider.initRockDB(dbDir, version);
        bootstrap.close();
    }

    @Test
    public void testPutRecreatesDbOnRocksDBExceptionWhenRestartMode() throws Exception {
        prepareEmptyDbOnDisk();
        org.rocksdb.RocksDB mockDb = mock(org.rocksdb.RocksDB.class);
        RocksDBException fault = new RocksDBException("simulated write failure");
        Mockito.doThrow(fault).when(mockDb).put(any(byte[].class), any(byte[].class));

        RocksDB rocks = new RocksDB(mockDb, dbDir, version, restartSource);
        byte[] key = "k1".getBytes(StandardCharsets.UTF_8);
        byte[] val = "v1".getBytes(StandardCharsets.UTF_8);
        // First put hits the mock, fails, triggers recreate; the write is not retried by RocksDB.put.
        rocks.put(key, val);
        rocks.put(key, val);

        verify(mockDb, times(1)).close();
        assertArrayEquals(val, rocks.get(key));
        rocks.close();
    }

    @Test
    public void testPutWithSyncRecreatesDbOnRocksDBExceptionWhenRestartMode() throws Exception {
        prepareEmptyDbOnDisk();
        org.rocksdb.RocksDB mockDb = mock(org.rocksdb.RocksDB.class);
        Mockito.doThrow(new RocksDBException("sync write fault"))
                .when(mockDb)
                .put(any(WriteOptions.class), any(byte[].class), any(byte[].class));

        RocksDB rocks = new RocksDB(mockDb, dbDir, version, restartSource);
        byte[] key = "syncKey".getBytes(StandardCharsets.UTF_8);
        byte[] val = "syncVal".getBytes(StandardCharsets.UTF_8);
        rocks.put(key, val, true);
        rocks.put(key, val, true);

        verify(mockDb, times(1)).close();
        assertArrayEquals(val, rocks.get(key));
        rocks.close();
    }

    @Test
    public void testSubsequentOperationsWorkAfterRecreateUpstreamUnaffected() throws Exception {
        prepareEmptyDbOnDisk();
        org.rocksdb.RocksDB mockDb = mock(org.rocksdb.RocksDB.class);
        Mockito.doThrow(new RocksDBException("first put fails"))
                .when(mockDb)
                .put(any(byte[].class), any(byte[].class));

        RocksDB rocks = new RocksDB(mockDb, dbDir, version, restartSource);
        byte[] k1 = "a".getBytes(StandardCharsets.UTF_8);
        byte[] v1 = "1".getBytes(StandardCharsets.UTF_8);
        rocks.put(k1, v1);
        rocks.put(k1, v1);

        byte[] k2 = "b".getBytes(StandardCharsets.UTF_8);
        byte[] v2 = "2".getBytes(StandardCharsets.UTF_8);
        rocks.put(k2, v2);
        rocks.put(k2, v2, false);

        assertArrayEquals(v1, rocks.get(k1));
        assertArrayEquals(v2, rocks.get(k2));
        rocks.delete(k1);
        assertNull(rocks.get(k1));
        assertArrayEquals(v2, rocks.get(k2));
        rocks.close();
    }

    @Test
    public void testPutPropagatesWhenIgnoreModeAndDoesNotCloseMockDb() throws Exception {
        prepareEmptyDbOnDisk();
        org.rocksdb.RocksDB mockDb = mock(org.rocksdb.RocksDB.class);
        RocksDBException fault = new RocksDBException("ignored path");
        Mockito.doThrow(fault).when(mockDb).put(any(byte[].class), any(byte[].class));

        RocksDB rocks = new RocksDB(mockDb, dbDir, version, ignoreSource);
        RuntimeException thrown =
                assertThrows(
                        RuntimeException.class,
                        () ->
                                rocks.put(
                                        "x".getBytes(StandardCharsets.UTF_8), "y".getBytes(StandardCharsets.UTF_8)));
        assertTrue(thrown.getCause() instanceof RocksDBException);
        verify(mockDb, times(0)).close();
        rocks.close();
    }

    @Test
    public void testGetReturnsNullAndRecreatesOnReadFailureWhenRestartMode() throws Exception {
        prepareEmptyDbOnDisk();
        org.rocksdb.RocksDB mockDb = mock(org.rocksdb.RocksDB.class);
        Mockito.doThrow(new RocksDBException("read fault")).when(mockDb).get(any(byte[].class));

        RocksDB rocks = new RocksDB(mockDb, dbDir, version, restartSource);
        assertNull(rocks.get("missing".getBytes(StandardCharsets.UTF_8)));
        verify(mockDb, times(1)).close();

        byte[] key = "recovered".getBytes(StandardCharsets.UTF_8);
        byte[] val = "data".getBytes(StandardCharsets.UTF_8);
        rocks.put(key, val);
        assertArrayEquals(val, rocks.get(key));
        rocks.close();
    }

    @Test
    public void testDeleteRecreatesOnFailureWhenRestartMode() throws Exception {
        prepareEmptyDbOnDisk();
        org.rocksdb.RocksDB mockDb = mock(org.rocksdb.RocksDB.class);
        Mockito.doThrow(new RocksDBException("delete fault")).when(mockDb).delete(any(byte[].class));

        RocksDB rocks = new RocksDB(mockDb, dbDir, version, restartSource);
        byte[] key = "delKey".getBytes(StandardCharsets.UTF_8);
        byte[] val = "delVal".getBytes(StandardCharsets.UTF_8);
        rocks.delete(key);

        verify(mockDb, times(1)).close();
        rocks.put(key, val);
        assertArrayEquals(val, rocks.get(key));
        rocks.close();
    }

    @Test
    public void testRealDbPutGetDeleteRoundTrip() throws Exception {
        org.rocksdb.RocksDB nativeDb = RocksDBProvider.initRockDB(dbDir, version);
        RocksDB rocks = new RocksDB(nativeDb, dbDir, version, restartSource);
        byte[] key = "round".getBytes(StandardCharsets.UTF_8);
        byte[] val = "trip".getBytes(StandardCharsets.UTF_8);
        rocks.put(key, val);
        assertArrayEquals(val, rocks.get(key));
        rocks.delete(key);
        assertNull(rocks.get(key));
        rocks.close();
    }

    @Test
    public void testRealDbPutWithSync() throws Exception {
        org.rocksdb.RocksDB nativeDb = RocksDBProvider.initRockDB(dbDir, version);
        RocksDB rocks = new RocksDB(nativeDb, dbDir, version, restartSource);
        byte[] key = "sync".getBytes(StandardCharsets.UTF_8);
        byte[] val = "on".getBytes(StandardCharsets.UTF_8);
        rocks.put(key, val, true);
        assertArrayEquals(val, rocks.get(key));
        rocks.close();
    }

    @Test
    public void testGetPropagatesWhenIgnoreMode() throws Exception {
        prepareEmptyDbOnDisk();
        org.rocksdb.RocksDB mockDb = mock(org.rocksdb.RocksDB.class);
        Mockito.doThrow(new RocksDBException("read fail")).when(mockDb).get(any(byte[].class));

        RocksDB rocks = new RocksDB(mockDb, dbDir, version, ignoreSource);
        assertThrows(RuntimeException.class, () -> rocks.get("any".getBytes(StandardCharsets.UTF_8)));
        verify(mockDb, times(0)).close();
        rocks.close();
    }

    @Test
    public void testDeletePropagatesWhenIgnoreMode() throws Exception {
        prepareEmptyDbOnDisk();
        org.rocksdb.RocksDB mockDb = mock(org.rocksdb.RocksDB.class);
        Mockito.doThrow(new RocksDBException("del fail")).when(mockDb).delete(any(byte[].class));

        RocksDB rocks = new RocksDB(mockDb, dbDir, version, ignoreSource);
        assertThrows(RuntimeException.class, () -> rocks.delete("z".getBytes(StandardCharsets.UTF_8)));
        verify(mockDb, times(0)).close();
        rocks.close();
    }

    @Test
    public void testMultipleKeysReadableAfterRecreate() throws Exception {
        prepareEmptyDbOnDisk();
        org.rocksdb.RocksDB mockDb = mock(org.rocksdb.RocksDB.class);
        Mockito.doThrow(new RocksDBException("fail"))
                .when(mockDb)
                .put(any(byte[].class), any(byte[].class));

        RocksDB rocks = new RocksDB(mockDb, dbDir, version, restartSource);
        byte[] p1 = "p1".getBytes(StandardCharsets.UTF_8);
        byte[] q1 = "q1".getBytes(StandardCharsets.UTF_8);
        byte[] p2 = "p2".getBytes(StandardCharsets.UTF_8);
        byte[] q2 = "q2".getBytes(StandardCharsets.UTF_8);
        rocks.put(p1, q1);
        rocks.put(p1, q1);
        rocks.put(p2, q2);

        verify(mockDb, times(1)).close();
        assertArrayEquals(q1, rocks.get(p1));
        assertArrayEquals(q2, rocks.get(p2));
        rocks.close();
    }
}
