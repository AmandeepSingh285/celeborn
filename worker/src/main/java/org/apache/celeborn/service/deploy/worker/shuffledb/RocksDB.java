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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.metrics.source.AbstractSource;
import org.apache.celeborn.service.deploy.worker.WorkerSource;

/**
 * RocksDB implementation of the local KV storage used to persist the shuffle state.
 *
 * <p>Note: code copied from Apache Spark.
 */
public class RocksDB implements DB {
  private volatile org.rocksdb.RocksDB db;
  private final WriteOptions SYNC_WRITE_OPTIONS = new WriteOptions().setSync(true);
  private final File dbFile;
  private final StoreVersion version;
  private final AbstractSource source;
  private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

  private static final Logger logger = LoggerFactory.getLogger(RocksDB.class);

  public RocksDB(org.rocksdb.RocksDB db, File dbFile, StoreVersion version, AbstractSource source) {
    this.db = db;
    this.dbFile = dbFile;
    this.version = version;
    this.source = source;
  }

  private void recreateDBInstance() {
    rwLock.writeLock().lock();
    try {
      try {
        if (db != null) {
          db.close();
        }
      } catch (Exception e) {
        logger.warn("Failed to close RocksDB instance", e);
      }

      db = RocksDBProvider.initRockDB(dbFile, version);
    } catch (IOException e) {
      logger.warn("Failed to recreate RocksDB instance", e);
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  @Override
  public void put(byte[] key, byte[] value) {
    try {
      rwLock.readLock().lock();
      try {
        db.put(key, value);
      } finally {
        rwLock.readLock().unlock();
      }
    } catch (RocksDBException e) {
      if (source instanceof WorkerSource
              && "THROW".equals(((WorkerSource) source).workerMetadataFailureMode())) {
        throw new RuntimeException(e);
      }
      recreateDBInstance();
    }
  }

  @Override
  public void put(byte[] key, byte[] value, boolean sync) {
    try {
      rwLock.readLock().lock();
      try {
        if (sync) {
          db.put(SYNC_WRITE_OPTIONS, key, value);
        } else {
          db.put(key, value);
        }
      } finally {
        rwLock.readLock().unlock();
      }
    } catch (RocksDBException e) {
      if (source instanceof WorkerSource
              && "THROW".equals(((WorkerSource) source).workerMetadataFailureMode())) {
        throw new RuntimeException(e);
      }
      recreateDBInstance();
    }
  }

  @Override
  public byte[] get(byte[] key) {
    try {
      return db.get(key);
    } catch (RocksDBException e) {
      if (source instanceof WorkerSource
              && "THROW".equals(((WorkerSource) source).workerMetadataFailureMode())) {
        throw new RuntimeException(e);
      }
      recreateDBInstance();
      try {
        return db.get(key);
      } catch (RocksDBException e2) {
        throw new RuntimeException(e2);
      }
    }
  }

  @Override
  public void delete(byte[] key) {
    try {
      rwLock.readLock().lock();
      try {
        db.delete(key);
      } finally {
        rwLock.readLock().unlock();
      }
    } catch (RocksDBException e) {
      if (source instanceof WorkerSource
              && "THROW".equals(((WorkerSource) source).workerMetadataFailureMode())) {
        throw new RuntimeException(e);
      }
      recreateDBInstance();
    }
  }

  @Override
  public DBIterator iterator() {
    return new RocksDBIterator(db.newIterator());
  }

  @Override
  public void close() throws IOException {
    db.close();
  }
}
