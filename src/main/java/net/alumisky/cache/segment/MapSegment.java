/*
 * Javolution - Java(TM) Solution for Real-Time and Embedded Systems
 * Copyright (C) 2006 - Javolution (http://javolution.org/)
 * All rights reserved.
 * 
 * Permission to use, copy, modify, and distribute this software is
 * freely granted, provided that this notice is preserved.
 */
package net.alumisky.cache.segment;

import java.util.Map;
import javax.realtime.MemoryArea;
import java.io.PrintStream;
import java.util.Iterator;

import javolution.context.ArrayFactory;
import javolution.context.LogContext;
import javolution.lang.MathLib;
import javolution.lang.Reusable;
import javolution.util.FastCollection.Record;
import javolution.util.FastComparator;
import javolution.util.FastMap;

/**
 * <p>
 * This class represents a hash map with real-time behavior; smooth capacity
 * increase and <i>thread-safe</i> without external synchronization when
 * {@link #isShared shared}.</p>
 *
 * <p>
 * {@link MapSegment} has a predictable iteration order, which is the order in
 * which keys are inserted into the map (similar to
 * <code>java.util.LinkedHashMap</code> collection class). If the map is marked
 * {@link #setShared(boolean) shared} then all operations are thread-safe
 * including iterations over the map's collections. Unlike
 * <code>ConcurrentHashMap</code>, {@link #get(Object) access} never blocks;
 * retrieval reflects the map state not older than the last time the accessing
 * threads have been synchronized (for multi-processors systems synchronizing
 * ensures that the CPU internal cache is not stale).</p>
 *
 * <p>
 * {@link MapSegment} may use custom key comparators; the default comparator is
 * either {@link FastComparator#DIRECT DIRECT} or
 * {@link FastComparator#REHASH REHASH} based upon the current <a href=
 *     "{@docRoot}/overview-summary.html#configuration">Javolution
 * Configuration</a>. Users may explicitly set the key comparator to
 * {@link FastComparator#DIRECT DIRECT} for optimum performance when the hash
 * codes are well distributed for all run-time platforms (e.g. calculated hash
 * codes).</p>
 *
 * <p>
 * Custom key comparators are extremely useful for value retrieval when map's
 * keys and argument keys are not of the same class (such as {@link String} and {@link javolution.text.Text Text}
 *     ({@link FastComparator#LEXICAL LEXICAL})), to substitute more efficient hash
 * code calculations ({@link FastComparator#STRING STRING}) or for identity maps
 * ({@link FastComparator#IDENTITY IDENTITY}):[code] MapSegment identityMap =
 new MapSegment().setKeyComparator(FastComparator.IDENTITY); [/code]</p>
 *
 * <p>
 * {@link FastMap.Entry} can quickly be iterated over (forward or backward)
 without using iterators. For example:[code] MapSegment<String, Thread> map =
 ...; for (MapSegment.Entry<String, Thread> e = map.head(), end = map.tail();
 * (e = e.getNext()) != end;) { String key = e.getKey(); // No typecast
 * necessary. Thread value = e.getValue(); // No typecast necessary.
 * }[/code]</p>
 *
 * <p>
 * Custom map implementations may override the {@link #newEntry} method in order
 * to return their own {@link Entry} implementation (with additional fields for
 * example).</p>
 *
 * <p>
 * {@link MapSegment} are {@link Reusable reusable}; they maintain an internal
 * pool of <code>Map.Entry</code> objects. When an entry is removed from a map,
 * it is automatically restored to its pool (unless the map is shared in which
 * case the removed entry is candidate for garbage collection as it cannot be
 * safely recycled).</p>
 *
 * <p>
 Shared maps do not use internal synchronization, except in case of concurrent
 modifications of the map structure (entry added/deleted). Reads and
 iterations are never synchronized and never blocking. With regards to the
 memory model, shared maps are equivalent to shared non-volatile variables (no
 "happen before" guarantee). There are typically used as lookup tables. For
 example:[code] public class Unit { static MapSegment<Unit, String> labels =
 new MapSegment().setShared(true); ... public String toString() { String
 label = labels.get(this); // No synchronization. return label != null ? label
 : makeLabel(); } }[/code]</p>
 *
 * <p>
 * <b>Implementation Note:</b> To maintain time-determinism, rehash/resize is
 * performed only when the map's size is small (see chart). For large maps (size
 * > 512), the collection is divided recursively into (64) smaller sub-maps. The
 * cost of the dispatching (based upon hashcode value) has been measured to be
 * at most 20% of the access time (and most often way less).</p>
 *
 * @author <a href="mailto:jean-marie@dautelle.com">Jean-Marie Dautelle </a>
 * @version 5.2, September 11, 2007
 */
public class MapSegment<K, V> {

    // We do a full resize (and rehash) only when the capacity is less than C1.
    // For large maps we dispatch to sub-maps.
    private static final int C0 = MapConstants.C0;
    private static final int C1 = MapConstants.C1;
    private static final int B2 = MapConstants.B2;
    private static final int C2 = MapConstants.C2;

    /**
     * Holds the head entry to which the first entry attaches. The head entry
     * never changes (entries always added last).
     */
    private transient Entry<K, V> _head;

    /**
     * Holds the tail entry to which the last entry attaches. The tail entry
     * changes as entries are added/removed.
     */
    private transient Entry<K, V> _tail;

    /**
     * Holds the map's entries.
     */
    private transient Entry<K, V>[] _entries;

    /**
     * Holds the number of user entry in the entry table.
     */
    private transient int _entryCount;

    /**
     * Holds the number of NULL (when entry removed). The number has to be taken
     * into account to clean-up the table if too many NULL are present.
     */
    private transient int _nullCount;

    /**
     * Holds sub-maps (for large collection).
     */
    private transient MapSegment[] _subMaps;

    /**
     * Indicates if sub-maps are active.
     */
    private transient boolean _useSubMaps;

    /**
     * The hash shift (for sub-maps to discard bits already taken into account).
     */
    private transient int _keyShift;

    /**
     * Holds the key comparator.
     */
    private transient FastComparator _keyComparator;

    /**
     * Indicates if key comparator is direct.
     */
    private transient boolean _isDirectKeyComparator;

    /**
     * Holds the value comparator.
     */
    private transient FastComparator _valueComparator;

    /**
     * Indicates if this map is shared (thread-safe).
     */
    private transient boolean _isShared;

    /**
     * Creates a map whose capacity increment smoothly without large resize
     * operations.
     */
    public MapSegment() {
        this(4);
    }

    /**
     * Creates a map of specified maximum size (a full resize may occur if the
     * specified capacity is exceeded).
     *
     * @param capacity the maximum capacity.
     */
    public MapSegment(int capacity) {
        setKeyComparator(FastComparator.DEFAULT);
        setValueComparator(FastComparator.DEFAULT);
        setup(capacity);
    }

    private void setup(int capacity) {
        int tableLength = C0;
        while (tableLength < capacity) {
            tableLength <<= 1;
        }
        _entries = (Entry<K, V>[]) new Entry[tableLength << 1];
        _head = newEntry();
        _tail = newEntry();
        _head._next = _tail;
        _tail._previous = _head;
        Entry previous = _tail;
        for (int i = 0; i++ < capacity;) {
            Entry newEntry = newEntry();
            newEntry._previous = previous;
            previous._next = newEntry;
            previous = newEntry;
        }
    }

    /**
     * Used solely for sub-maps (we don't need head or tail just the table).
     */
    private MapSegment(Entry[] entries) {
        _entries = entries;
    }

    /**
     * Recycles the specified map instance.
     *
     * @param instance the map instance to recycle.
     */
    public static void recycle(MapSegment instance) {
       instance.reset();
    }

    /**
     * Returns the head entry of this map.
     *
     * @return the entry such as <code>head().getNext()</code> holds the first
     * map entry.
     */
    public final Entry<K, V> head() {
        return _head;
    }

    /**
     * Returns the tail entry of this map.
     *
     * @return the entry such as <code>tail().getPrevious()</code> holds the
     * last map entry.
     */
    public final Entry<K, V> tail() {
        return _tail;
    }

    /**
     * Returns the number of key-value mappings in this {@link MapSegment}.
     *
     * <p>
     * Note: If concurrent updates are performed, application should not rely
     * upon the size during iterations.</p>
     *
     * @return this map's size.
     */
    public final int size() {
        if (!_useSubMaps) {
            return _entryCount;
        }
        int sum = 0;
        for (int i = 0; i < _subMaps.length;) {
            sum += _subMaps[i++].size();
        }
        return sum;
    }

    /**
     * Indicates if this map contains no key-value mappings.
     *
     * @return <code>true</code> if this map contains no key-value mappings;
     * <code>false</code> otherwise.
     */
    public final boolean isEmpty() {
        return _head._next == _tail;
    }

    /**
     * Indicates if this map contains a mapping for the specified key.
     *
     * @param key the key whose presence in this map is to be tested.
     * @return <code>true</code> if this map contains a mapping for the
     * specified key; <code>false</code> otherwise.
     * @throws NullPointerException if the key is <code>null</code>.
     */
    public final boolean containsKey(Object key) {
        return getEntry(key) != null;
    }

    /**
     * Returns the value to which this map associates the specified key. This
     * method is always thread-safe regardless whether or not the map is marked
     * {@link #isShared() shared}.
     *
     * @param key the key whose associated value is to be returned.
     * @return the value to which this map maps the specified key, or
     * <code>null</code> if there is no mapping for the key.
     * @throws NullPointerException if key is <code>null</code>.
     */
    public final V get(Object key) {
        Entry<K, V> entry = getEntry(key);
        return (entry != null) ? entry._value : null;
    }

    /**
     * Returns the entry with the specified key. This method is always
     * thread-safe without synchronization.
     *
     * @param key the key whose associated entry is to be returned.
     * @return the entry for the specified key or <code>null</code> if none.
     */
    public final Entry<K, V> getEntry(Object key) {
        return getEntry(key, _isDirectKeyComparator ? key.hashCode()
                : _keyComparator.hashCodeOf(key));
    }

    private final Entry getEntry(Object key, int keyHash) {
        final MapSegment map = getSubMap(keyHash);
        final Entry[] entries = map._entries; // Atomic.
        final int mask = entries.length - 1;
        
        for (int i = keyHash >> map._keyShift;; i++) {
            Entry entry = entries[i & mask];
            if (entry == null) {
                return null;
            }
            if ((key == entry._key)
                    || ((keyHash == entry._keyHash) && (_isDirectKeyComparator ? key
                                    .equals(entry._key)
                            : _keyComparator.areEqual(key, entry._key)))) {
                return entry;
            }
        }
    }

    private MapSegment getSubMap(int keyHash) {
        return _useSubMaps ? _subMaps[keyHash & (C2 - 1)]
                .getSubMap(keyHash >> B2) : this;
    }

    /**
     * Associates the specified value with the specified key in this map. If
     * this map previously contained a mapping for this key, the old value is
     * replaced. For {@link #isShared() shared} map, internal synchronization is
     * performed only when new entries are created.
     *
     * @param key the key with which the specified value is to be associated.
     * @param value the value to be associated with the specified key.
     * @return the previous value associated with specified key, or
     * <code>null</code> if there was no mapping for key. A <code>null</code>
     * return can also indicate that the map previously associated
     * <code>null</code> with the specified key.
     * @throws NullPointerException if the key is <code>null</code>.
     */
    public final Entry<K,V> put(K key, V value) {
        return put(key, value, _isDirectKeyComparator ? key
                .hashCode() : _keyComparator.hashCodeOf(key), _isShared);
    }

    private Entry<K,V> put(Object key, Object value, int keyHash, boolean concurrent) {
        final MapSegment map = getSubMap(keyHash);
        final Entry[] entries = map._entries; // Atomic.
        final int mask = entries.length - 1;
        int slot = -1;
        
        for (int i = keyHash >> map._keyShift;; i++) {
            Entry entry = entries[i & mask];
            if (entry == null) {
                slot = slot < 0 ? i & mask : slot;
                break;
            } else if (entry == Entry.NULL) {
                slot = slot < 0 ? i & mask : slot;
            } else if ((key == entry._key)
                    || ((keyHash == entry._keyHash) && (_isDirectKeyComparator ? key
                                    .equals(entry._key)
                            : _keyComparator.areEqual(key, entry._key)))) {

                entry._value = value;
                return entry;
            }
        }

        // Add new entry (synchronize if concurrent).
        if (concurrent) {
            synchronized (this) {
                return put(key, value, keyHash, false);
            }
        }

        // Setup entry.
        final Entry entry = _tail;
        entry._key = key;
        entry._value = value;
        entry._keyHash = keyHash;
        if (entry._next == null) {
            createNewEntries();
        }
        entries[slot] = entry;
        map._entryCount += ONE_VOLATILE; // Prevents reordering.
        _tail = _tail._next;

        if (map._entryCount + map._nullCount > (entries.length >> 1)) { // Table more than half empty.
            map.resizeTable(_isShared);
        }
        return entry;
    }

    /**
     * Copies all of the mappings from the specified map to this map.
     * 
     * @param map the mappings to be stored in this map.
     * @throws NullPointerException the specified map is <code>null</code>,
     *         or the specified map contains <code>null</code> keys.
     */
    public final void putAll(Map<? extends K, ? extends V> map) {
        for (Iterator i = map.entrySet().iterator(); i.hasNext();) {
            Map.Entry<K,V> e = (Map.Entry<K,V>) i.next();
            put(e.getKey(), e.getValue());
        }
    }    
    
    private void createNewEntries() { // Increase the number of entries.
        MemoryArea.getMemoryArea(this).executeInArea(new Runnable() {
            public void run() {
                Entry previous = _tail;
                for (int i = 0; i < 8; i++) { // Creates 8 entries at a time.
                    Entry<K, V> newEntry = newEntry();
                    newEntry._previous = previous;
                    previous._next = newEntry;
                    previous = newEntry;
                }
            }
        });
    }

    // This method is called only on final sub-maps.
    private void resizeTable(final boolean isShared) {
        MemoryArea.getMemoryArea(this).executeInArea(new Runnable() {
            public void run() {

                // Reset the NULL count (we don't copy Entry.NULL).
                final int nullCount = _nullCount;
                _nullCount = 0;

                // Check if we can just cleanup (remove NULL entries).
                if (nullCount > _entryCount) { // Yes.
                    if (isShared) { // Replaces with a new table.
                        Entry[] tmp = new Entry[_entries.length];
                        copyEntries(_entries, tmp, _entries.length);
                        _entries = tmp;
                    } else { // We need a temporary buffer.
                        Object[] tmp = (Object[]) ArrayFactory.OBJECTS_FACTORY
                                .array(_entries.length);
                        System.arraycopy(_entries, 0, tmp, 0, _entries.length);
                        MapSegment.reset(_entries); // Ok not shared. 
                        copyEntries(tmp, _entries, _entries.length);
                        MapSegment.reset(tmp); // Clear references.
                        ArrayFactory.OBJECTS_FACTORY.recycle(tmp);
                    }
                    return;
                }

                // Resize if size is small.
                int tableLength = _entries.length << 1;
                if (tableLength <= C1) { // Ok to resize.
                    Entry[] tmp = new Entry[tableLength];
                    copyEntries(_entries, tmp, _entries.length);
                    _entries = tmp;
                    return;
                }

                // No choice but to use sub-maps.
                if (_subMaps == null) { // Creates sub-maps.
                    _subMaps = newSubMaps(tableLength >> (B2 - 1));
                }

                // Copies the current entries to sub-maps. 
                for (int i = 0; i < _entries.length;) {
                    Entry entry = _entries[i++];
                    if ((entry == null) || (entry == Entry.NULL)) {
                        continue;
                    }
                    MapSegment subMap = _subMaps[(entry._keyHash >> _keyShift)
                            & (C2 - 1)];
                    subMap.mapEntry(entry);
                    if (((subMap._entryCount + subMap._nullCount) << 1) >= subMap._entries.length) {
                        // Serious problem submap already full, don't use submap just resize.
                        LogContext.warning("Unevenly distributed hash code - Degraded Preformance");
                        Entry[] tmp = new Entry[tableLength];
                        copyEntries(_entries, tmp, _entries.length);
                        _entries = tmp;
                        _subMaps = null; // Discards sub-maps.
                        return;
                    }
                }
                _useSubMaps = (ONE_VOLATILE == 1); // Prevents reordering.   
            }
        });
    }

    private MapSegment[] newSubMaps(int capacity) {
        MapSegment[] subMaps = new MapSegment[C2];
        for (int i = 0; i < C2; i++) {
            MapSegment subMap = new MapSegment(new Entry[capacity]);
            subMap._keyShift = B2 + _keyShift;
            subMaps[i] = subMap;
        }
        return subMaps;
    }

    // Adds the specified entry to this map table.
    private void mapEntry(Entry entry) {
        final int mask = _entries.length - 1;
        for (int i = entry._keyHash >> _keyShift;; i++) {
            Entry e = _entries[i & mask];
            if (e == null) {
                _entries[i & mask] = entry;
                break;
            }
        }
        _entryCount++;
    }

    // The destination table must be empty.
    private void copyEntries(Object[] from, Entry[] to, int count) {
        final int mask = to.length - 1;
        for (int i = 0; i < count;) {
            Entry entry = (Entry) from[i++];
            if ((entry == null) || (entry == Entry.NULL)) {
                continue;
            }
            for (int j = entry._keyHash >> _keyShift;; j++) {
                Entry tmp = to[j & mask];
                if (tmp == null) {
                    to[j & mask] = entry;
                    break;
                }
            }
        }
    }

    /**
     * Removes the entry for the specified key if present. The entry is recycled
     * if the map is not marked as {@link #isShared shared}; otherwise the entry
     * is candidate for garbage collection.
     *
     * <p>
     * Note: Shared maps in ImmortalMemory (e.g. static) should not remove their
     * entries as it could cause a memory leak (ImmortalMemory is never garbage
     * collected), instead they should set their entry values to
     * <code>null</code>.</p>
     *
     * @param key the key whose mapping is to be removed from the map.
     * @return previous value associated with specified key, or
     * <code>null</code> if there was no mapping for key. A <code>null</code>
     * return can also indicate that the map previously associated
     * <code>null</code> with the specified key.
     * @throws NullPointerException if the key is <code>null</code>.
     */
    public final V remove(Object key) {
        return (V) remove(key, _isDirectKeyComparator ? key
                .hashCode() : _keyComparator.hashCodeOf(key), _isShared);
    }

    private final Object remove(Object key, int keyHash, boolean concurrent) {
        final MapSegment map = getSubMap(keyHash);
        final Entry[] entries = map._entries; // Atomic.
        final int mask = entries.length - 1;
        for (int i = keyHash >> map._keyShift;; i++) {
            Entry entry = entries[i & mask];
            if (entry == null) {
                return null; // No entry.
            }
            if ((key == entry._key)
                    || ((keyHash == entry._keyHash) && (_isDirectKeyComparator ? key
                                    .equals(entry._key)
                            : _keyComparator.areEqual(key, entry._key)))) {
                // Found the entry.
                if (concurrent) {
                    synchronized (this) {
                        return remove(key, keyHash, false);
                    }
                }

                // Detaches entry from list.
                entry._previous._next = entry._next;
                entry._next._previous = entry._previous;

                // Removes from table.
                entries[i & mask] = Entry.NULL;
                map._nullCount++;
                map._entryCount--;

                Object prevValue = entry._value;
                if (!_isShared) { // Clears key/value and recycle.
                    entry._key = null;
                    entry._value = null;
                    entry.setCreationTime(0);
                    entry.setAccessTime(Long.MAX_VALUE);

                    final Entry next = _tail._next;
                    entry._previous = _tail;
                    entry._next = next;
                    _tail._next = entry;
                    if (next != null) {
                        next._previous = entry;
                    }
                }
                return prevValue;
            }
        }
    }

    /**
     * <p>
     * Sets the shared status of this map (whether the map is thread-safe or
     * not). Shared maps are typically used for lookup table (e.g. static
     * instances in ImmortalMemory). They support concurrent access (e.g.
     * iterations) without synchronization, the maps updates themselves are
     * synchronized internally.</p>
     * <p>
     * Unlike <code>ConcurrentHashMap</code> access to a shared map never
     * blocks. Retrieval reflects the map state not older than the last time the
     * accessing thread has been synchronized (for multi-processors systems
     * synchronizing ensures that the CPU internal cache is not stale).</p>
     *
     * @param isShared <code>true</code> if this map is shared and thread-safe;
     * <code>false</code> otherwise.
     * @return <code>this</code>
     */
    public MapSegment<K, V> setShared(boolean isShared) {
        _isShared = isShared;
        return this;
    }

    /**
     * Indicates if this map supports concurrent operations without
     * synchronization (default unshared).
     *
     * @return <code>true</code> if this map is thread-safe; <code>false</code>
     * otherwise.
     */
    public boolean isShared() {
        return _isShared;
    }

    /**
     * Sets the key comparator for this fast map.
     *
     * @param keyComparator the key comparator.
     * @return <code>this</code>
     */
    public final MapSegment<K, V> setKeyComparator(
            FastComparator<? super K> keyComparator) {
        _keyComparator = keyComparator;
        _isDirectKeyComparator = true;
//        _isDirectKeyComparator = (keyComparator instanceof FastComparator.Direct)
//                || ((_keyComparator instanceof FastComparator.Default) && ! FastComparator._Rehash);
        return this;
    }

    /**
     * Returns the key comparator for this fast map.
     *
     * @return the key comparator.
     */
    public FastComparator<? super K> getKeyComparator() {
        return _keyComparator;
    }

    /**
     * Sets the value comparator for this map.
     *
     * @param valueComparator the value comparator.
     * @return <code>this</code>
     */
    public final MapSegment<K, V> setValueComparator(
            FastComparator<? super V> valueComparator) {
        _valueComparator = valueComparator;
        return this;
    }

    /**
     * Returns the value comparator for this fast map.
     *
     * @return the value comparator.
     */
    public FastComparator<? super V> getValueComparator() {
        return _valueComparator;
    }

    /**
     * Removes all map's entries. The entries are removed and recycled; unless
     * this map is {@link #isShared shared} in which case the entries are
     * candidate for garbage collection.
     *
     * <p>
     * Note: Shared maps in ImmortalMemory (e.g. static) should not remove their
     * entries as it could cause a memory leak (ImmortalMemory is never garbage
     * collected), instead they should set their entry values to
     * <code>null</code>.</p>
     */
    public final void clear() {
        if (_isShared) {
            clearShared();
            return;
        }
        // Clears keys, values and recycle entries.
        for (Entry e = _head, end = _tail; (e = e._next) != end;) {
            e._key = null;
            e._value = null;
        }
        _tail = _head._next; // Reuse linked list of entries.       
        clearTables();
    }

    private void clearTables() {
        if (_useSubMaps) {
            for (int i = 0; i < C2;) {
                _subMaps[i++].clearTables();
            }
            _useSubMaps = false;
        }
        MapSegment.reset(_entries);
        _nullCount = 0;
        _entryCount = 0;
    }

    private synchronized void clearShared() {
        // We do not modify the linked list of entries (e.g. key, values) 
        // Concurrent iterations can still proceed unaffected.
        // The linked list fragment is detached from the map and will be 
        // garbage collected once all concurrent iterations are completed.
        _head._next = _tail;
        _tail._previous = _head;

        // We also detach the main entry table and sub-maps.
        MemoryArea.getMemoryArea(this).executeInArea(new Runnable() {
            public void run() {
                _entries = (Entry<K, V>[]) new Entry[C0];
                if (_useSubMaps) {
                    _useSubMaps = false;
                    _subMaps = newSubMaps(C0);
                }
                _entryCount = 0;
                _nullCount = 0;
            }
        });
    }

    /**
     * Compares the specified object with this map for equality. Returns
     * <code>true</code> if the given object is also a map and the two maps
     * represent the same mappings (regardless of collection iteration order).
     *
     * @param obj the object to be compared for equality with this map.
     * @return <code>true</code> if the specified object is equal to this map;
     * <code>false</code> otherwise.
     */
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj instanceof Map) {
            Map<?, ?> that = (Map) obj;
            return obj == this;
        } else {
            return false;
        }
    }

    /**
     * Returns the hash code value for this map.
     *
     * @return the hash code value for this map.
     */
    public int hashCode() {
        int code = 0;
        for (Entry e = _head, end = _tail; (e = e._next) != end;) {
            code += e.hashCode();
        }
        return code;
    }

    /**
     * Returns a new entry for this map; this method can be overriden by custom
     * map implementations.
     *
     * @return a new entry.
     */
    protected Entry<K, V> newEntry() {
        return new Entry();
    }

    private int getTableLength() {
        if (_useSubMaps) {
            int sum = 0;
            for (int i = 0; i < C2; i++) {
                sum += _subMaps[i].getTableLength();
            }
            return sum;
        } else {
            return _entries.length;
        }
    }

    private int getCapacity() {
        int capacity = 0;
        for (Entry e = _head; (e = e._next) != null;) {
            capacity++;
        }
        return capacity - 1;
    }

    private int getMaximumDistance() {
        int max = 0;
        if (_useSubMaps) {
            for (int i = 0; i < C2; i++) {
                int subMapMax = _subMaps[i].getMaximumDistance();
                max = MathLib.max(max, subMapMax);
            }
            return max;
        }
        for (int i = 0; i < _entries.length; i++) {
            Entry entry = _entries[i];
            if ((entry != null) && (entry != Entry.NULL)) {
                int slot = (entry._keyHash >> _keyShift)
                        & (_entries.length - 1);
                int distanceToSlot = i - slot;
                if (distanceToSlot < 0) {
                    distanceToSlot += _entries.length;
                }
                if (distanceToSlot > max) {
                    max = distanceToSlot;
                }
            }
        }
        return max;
    }

    private long getSumDistance() {
        long sum = 0;
        if (_useSubMaps) {
            for (int i = 0; i < C2; i++) {
                sum += _subMaps[i].getSumDistance();
            }
            return sum;
        }
        for (int i = 0; i < _entries.length; i++) {
            Entry entry = _entries[i];
            if ((entry != null) && (entry != Entry.NULL)) {
                int slot = (entry._keyHash >> _keyShift)
                        & (_entries.length - 1);
                int distanceToSlot = i - slot;
                if (distanceToSlot < 0) {
                    distanceToSlot += _entries.length;
                }
                sum += distanceToSlot;
            }
        }
        return sum;
    }

    private int getSubMapDepth() {
        if (_useSubMaps) {
            int depth = 0;
            for (int i = 0; i < C2; i++) {
                int subMapDepth = _subMaps[i].getSubMapDepth();
                depth = MathLib.max(depth, subMapDepth);
            }
            return depth + 1;
        } else {
            return 0;
        }
    }

    // Implements Reusable.
    public void reset() {
        setShared(false); // A shared map can only be reset if no thread use it.
        clear(); // In which case, it is safe to recycle the entries.
        setKeyComparator(FastComparator.DEFAULT);
        setValueComparator(FastComparator.DEFAULT);
    }

    /**
     * Prints the current statistics on this map. This method may help identify
     * poorly defined hash functions. The average distance should be less than
     * 20% (most of the entries are in their slots or close).
     *
     * @param out the stream to use for output (e.g. <code>System.out</code>)
     */
    public void printStatistics(PrintStream out) {
        long sum = this.getSumDistance();
        int size = this.size();
        int avgDistancePercent = size != 0 ? (int) (100 * sum / size) : 0;
        synchronized (out) {
            out.print("SIZE: " + size);
            out.print(", ENTRIES: " + getCapacity());
            out.print(", SLOTS: " + getTableLength());
            out.print(", USE SUB-MAPS: " + _useSubMaps);
            out.print(", SUB-MAPS DEPTH: " + getSubMapDepth());
            out.print(", NULL COUNT: " + _nullCount);
            out.print(", IS SHARED: " + _isShared);
            out.print(", AVG DISTANCE: " + avgDistancePercent + "%");
            out.print(", MAX DISTANCE: " + getMaximumDistance());
            out.println();
        }
    }    
    
    /**
     * This class represents a {@link MapSegment} entry. Custom
     * {@link MapSegment} may use a derived implementation. For example:[code]
     * static class MyMap<K,V,X> extends MapSegment<K,V> { protected MyEntry
     * newEntry() { return new MyEntry(); } class MyEntry extends Entry<K,V> { X
     * xxx; // Additional entry field (e.g. cross references). } }[/code]
     */
    public static class Entry<K, V> implements Map.Entry<K, V>, Record {

        /**
         * Holds NULL entries (to fill empty hole).
         */
        public static final Entry NULL = new Entry();

        /**
         * Holds the next node.
         */
        private Entry<K, V> _next;

        /**
         * Holds the previous node.
         */
        private Entry<K, V> _previous;

        /**
         * Holds the entry key.
         */
        private K _key;

        /**
         * Holds the entry value.
         */
        private V _value;

        /**
         * Holds the key hash code.
         */
        private int _keyHash;

        /**
         * Holds entry access time
         */
        private long accessTime = Long.MAX_VALUE;
        
        /**
         * Holds entry creation time
         */
        private long creationTime = 0;        

        /**
         * Default constructor.
         */
        protected Entry() {
        }

        /**
         * Returns the entry after this one.
         *
         * @return the next entry.
         */
        public final Entry<K, V> getNext() {
            return _next;
        }

        /**
         * Returns the entry before this one.
         *
         * @return the previous entry.
         */
        public final Entry<K, V> getPrevious() {
            return _previous;
        }
        
        /**
         * Returns the key for this entry.
         *
         * @return the entry key.
         */
        public final K getKey() {
            return _key;
        }

        /**
         * Returns the value for this entry.
         *
         * @return the entry value.
         */
        public final V getValue() {
            return _value;
        }

        public long getAccessTime() {
            return accessTime;
        }

        public void setAccessTime(long accessTime) {
            this.accessTime = accessTime;
        }        

        public boolean hasAccessTime() {
            return (accessTime < Long.MAX_VALUE);
        }        
        
        public boolean hasCreationTime() {
            return (creationTime > 0);
        }
        
        public long getCreationTime() {
            return creationTime;
        }        
        
        public void setCreationTime(long creationTime) {
            this.creationTime = creationTime;
        }
        
        
        /**
         * Sets the value for this entry.
         *
         * @param value the new value.
         * @return the previous value.
         */
        public final V setValue(V value) {
            V old = _value;
            _value = value;
            return old;
        }

        /**
         * Indicates if this entry is considered equals to the specified entry
         * (using default value and key equality comparator to ensure symmetry).
         *
         * @param that the object to test for equality.
         * @return <code>true<code> if both entry have equal keys and values.
         *         <code>false<code> otherwise.
         */
        public boolean equals(Object that) {
            if (that instanceof Map.Entry) {
                Map.Entry entry = (Map.Entry) that;
                return FastComparator.DEFAULT.areEqual(_key, entry.getKey())
                        && FastComparator.DEFAULT.areEqual(_value, entry
                                .getValue());
            } else {
                return false;
            }
        }

        /**
         * Returns the hash code for this entry.
         *
         * @return this entry hash code.
         */
        public int hashCode() {
            return ((_key != null) ? _key.hashCode() : 0)
                    ^ ((_value != null) ? _value.hashCode() : 0);
        }
    }

    /**
     * Reset the specified table.
     */
    private static void reset(Object[] entries) {
        for (int i = 0; i < entries.length;) {
            int count = MathLib.min(entries.length - i, C1);
            System.arraycopy(NULL_ENTRIES, 0, entries, i, count);
            i += count;
        }
    }

    private static final Entry[] NULL_ENTRIES = new Entry[C1];

    static volatile int ONE_VOLATILE = 1; // To prevent reordering.
}
