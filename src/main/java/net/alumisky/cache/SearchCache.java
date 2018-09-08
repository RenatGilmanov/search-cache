/*
 * Copyright 2017 AlumiSky (http://alumisky.net). All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
 */
package net.alumisky.cache;

import net.alumisky.cache.segment.MapSegment;
import net.alumisky.cache.segment.MapSegment.Entry;

import java.util.Map;

/**
 * <p>This is an experimental cache implementation, based on Javolution FastMap.
 * The only intention behind this implementation is to verify several cache strategies,
 * related to several usage scenarios, so it is not intended to be used in production.</p> 
 * 
 * <p>The main focus of the suggested approach covers multiple eviction
 * strategies and ability to hold large amount of data.</p>
 * 
 * <p>Eviction strategies are extremely important for any real-life usage.
 * Timed strategies, based on access time or creation time helps to ensure cache 
 * always holds an up to date values.</p>
 * 
 * <p>Strategies based on size are crucial, because there definitely will not be enough
 * memory to cache everything. Also, fixed memory footprint is a key to guarantee 
 * application memory limit will not be reached.</p>
 * 
 * <ul>
 *   <li> <b>expireAfterWrite</b> strategy is required to avoid keeping stale data
 *   <li> <b>expireAfterAccess</b> strategy helps to avoid keeping data that isn't likely to 
 *        be used again soon
 * </ul>
 * 
 * <p>
 * Those strategies work independently in order to make sure constantly accessed entries are
 * still being removed and refreshed. <b>expireAfterWrite</b> guarantees entry will be 
 * refreshed if validity period passed, no matter how frequently you access it.
 * </p>
 * 
 */
public class SearchCache<K, V> {

    /**
     * Initial {@see CacheSegement} capacity.
     */    
    public static final int CAPACITY = 1000;
    
    /**
     * Initial TTL value for <b>expireAfterAccess</b> eviction strategy.
     */
    public static final int ACCESS_TTL = 200;
    
    /**
     * Initial TTL value for <b>expireAfterWrite</b> eviction strategy.
     */
    public static final int CREATE_TTL = 600;

    /**
     * This threshold is related to both access and creation time invalidation
     * strategies, so let's keep it big enough.
     */
    private final int REMOVE_DEPTH = 100;

    /** 
     * Defines a threshold to perform a cleanup procedure.
     */
    public static final int CLEANUP_TIME_THRESHOLD = 50;          
    
    /**
     * Holds access time TTL value.
     */
    private int accessTTL = ACCESS_TTL;    
    
    /**
     * Holds creation time TTL value.
     */
    private int createTTL = CREATE_TTL;
    
    /**
     * {@see Ticker} holds latest timestamp in order to make it reusable.
     */
    private final Ticker ticker;
    
    /**
     * The {@link Map} segment, used to store cached data.
     */
    private final MapSegment<K, V> segment;

    /**
     * Constructs cache instance with current time and default capacity.
     */
    public SearchCache() {
        this(CAPACITY, new Ticker());
    }

    /**
     * 
     * @param ticker
     * @param capacity 
     */
    public SearchCache(int capacity, Ticker ticker) {
        this(capacity, ACCESS_TTL, CREATE_TTL, ticker);
    }    
    
    /**
     * 
     * @param ticker
     * @param capacity 
     */
    public SearchCache(int capacity, int accessTTL, int createTTL) {
        this(capacity, accessTTL, createTTL, new Ticker());
    }      

    /**
     * 
     * @param capacity
     * @param accessTTL
     * @param createTTL
     * @param ticker 
     */
    public SearchCache(int capacity, int accessTTL, int createTTL, Ticker ticker) {        
        this.segment = new MapSegment<>(capacity);
        this.accessTTL = accessTTL;
        this.createTTL = createTTL;
        this.ticker = ticker;        
    }
    
    /**
     * <p>
     * Get is the most critical operation, so it is better to keep it as simple
     * as possible.</p>
     * 
     * <p>
     * Basically, the idea is to limit this method to segment resolution and get
     * call. No other logic is allowed here in order to save the performance.</p>
     *
     * @param key
     * @return
     */
    public V getIfPresent(Object key) {
        V result = null;
        MapSegment.Entry<K, V> entry = segment.getEntry(key);

        if (null != entry) {
            entry.setAccessTime(ticker.nextTick());
            result = entry.getValue();
        }
        return result;
    }
    
    /**
     * Put performance is not that critical, so all cleanup operations will be
     * handled here.
     *
     * @param key
     * @param value
     */
    public void put(K key, V value) {
        Entry<K, V> entry = segment.put(key, value);

        // Time-based eviction strategy is highly important, so creation time
        // tracking is a must. 
        long time = System.currentTimeMillis();

        //{@see Ticker} is used to reuse timestamp
        // and save some CPU cycles for read opetion.        
        ticker.setNextTick(time);

        // Update creation time in order to avoid entry removal
        entry.setCreationTime(time);

        // Perform a bit of a clean-up. Write performance is critical, so no 
        // heavy lifting is supposed to happen here. 
        cleanUp();
    }

    /**
     * Removes the entry for the specified key if present.
     * 
     * @param key the key for the entry to be removed
     */
    protected void remove(K key) {
        segment.remove(key);
    }

    /**
     * Returns number of entries in the cache.
     * 
     * @return cache size
     */
    public long size() {
        return segment.size();
    }

    /**
     * Performs a cleanup procedure. 
     */
    public void cleanUp() {
        if(ticker.skipCleanup()) {
            return;
        }               
        
        // At this point cleanup is inevitable, so linked list traversal should
        // be performed.        
        Entry<K, V> e = segment.head();
        Entry<K, V> end = segment.tail();
        
        int processed = 0;
        long createThreshold = ticker.nextTick() - createTTL;
        long accessThreshold = ticker.nextTick() - accessTTL;

        Object removeKey = null;
        while ((e = e.getNext()) != end && processed++ < REMOVE_DEPTH) {
            if (null != removeKey) {
                segment.remove(removeKey);
                removeKey = null;
            }

            // both, access and creation thresholds might trigger entry removal      
            if (e.getCreationTime() <= createThreshold || e.getAccessTime() <= accessThreshold) {
                removeKey = e.getKey();
            }
        }

        if (null != removeKey) {
            segment.remove(removeKey);
        }        
        ticker.markCleanup();
    }

    /**
     * 
     * @param map 
     */
    public void putAll(Map<? extends K, ? extends V> map) {
        segment.putAll(map);
    }

    /**
     * Removes all map's entries. 
     */
    public void invalidateAll() {
        segment.clear();
    }

    /**
     * 
     * @param accessTTL  new TTL value 
     */
    public void setAccessTTL(int accessTTL) {
        this.accessTTL = accessTTL;
    }

    /**
     * 
     * @param createTTL new TTL value
     */
    public void setCreateTTL(int createTTL) {
        this.createTTL = createTTL;
    }   
    
    
    /**
     * Timestamps placeholder.
     */
    public static class Ticker {

        private long prevTick;
        private long nextTick;

        public Ticker() {
            this(System.currentTimeMillis());
        }
        
        public Ticker(long tick) {
            this.nextTick = tick;
            this.prevTick = tick;
        }

        public long nextTick() {
            return nextTick;
        }

        public void setNextTick(long tick) {
            this.nextTick = tick;
        }
        
        public boolean skipCleanup() {
            return (nextTick - prevTick < CLEANUP_TIME_THRESHOLD);
        }
        
        public void markCleanup() {
            prevTick = nextTick;
        }
    }    
    
    public static Builder newBuilder() {
        return new Builder();
    }
    
    // Builder interface
    
    public static class Builder {
        private int capacity  = CAPACITY;    
        private int accessTTL = ACCESS_TTL;    
        private int createTTL  = CREATE_TTL;                    
        
        public Builder expireAfterAccess(int accessTTL) {
            this.accessTTL = accessTTL;
            return this;
        }

        public Builder initialCapacity(int capacity) {
            this.capacity = capacity;
            return this;
        }
        
        public Builder expireAfterWrite(int writeTTL) {
            this.createTTL = writeTTL;
            return this;
        }
                
        public SearchCache build() {
            return new SearchCache(capacity, accessTTL, createTTL);
        }
    }    
}
