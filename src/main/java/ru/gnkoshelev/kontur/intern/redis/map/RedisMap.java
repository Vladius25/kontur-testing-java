package ru.gnkoshelev.kontur.intern.redis.map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.*;

public class RedisMap implements Map<String, String> {
    private Jedis jedis;
    private String hash;
    transient Set<String> keySet;
    transient Collection<String> values;
    transient Set<Entry<String, String>> entrySet;

    public RedisMap() {
        this("localhost", 6379, 0);
    }

    public RedisMap(String host, int port) {
        this(host, port, 0);
    }

    public RedisMap(String host, int port, int db) {
        Random rand = new Random();
        jedis = new Jedis(host, port);
        jedis.select(db);
        do
            hash = String.valueOf(rand.nextInt(10000));
        while (jedis.exists(hash));
    }

    public RedisMap(String host, int port, String hash) {
        this(host, port, hash, 0);
    }

    public RedisMap(String host, int port, String hash, int db) {
        jedis = new Jedis(host, port);
        jedis.select(db);
        this.hash = hash;

    }

    @Override
    public int size() {
        return jedis.hlen(hash).intValue();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        return jedis.hexists(hash, (String) key);
    }

    @Override
    public boolean containsValue(Object value) {
        for(String val : values())
            if(val.equals(value))
                return true;
        return false;
    }

    @Override
    public String get(Object key) {
        return jedis.hget(hash, (String) key);
    }

    @Override
    public String put(String key, String value) {
        String oldValue = get(key);
        jedis.hset(hash, key, value);
        return oldValue;
    }

    @Override
    public String remove(Object key) {
        String value = get(key);
        jedis.hdel(hash, (String) key);
        return value;
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> m) {
        for (Entry<? extends String, ? extends String> e : m.entrySet()) {
            String key = e.getKey();
            String value = e.getValue();
            put(key, value);
        }
    }

    @Override
    public void clear() {
        jedis.del(hash);
    }


    @Override
    public Set<String> keySet() {
        Set<String> ks = keySet;
        if (ks == null) {
            ks = new KeySet();
            keySet = ks;
        }
        return ks;
    }

    final class KeySet extends AbstractSet<String> {
        public final int size() {
            return RedisMap.this.size();
        }

        public final void clear() {
            RedisMap.this.clear();
        }

        public final Iterator<String> iterator() {
            return new KeyIterator();
        }

        public final boolean contains(Object o) {
            return containsKey(o);
        }

        public final boolean remove(Object key) {
            return RedisMap.this.remove(key) != null;
        }
    }

    @Override
    public Collection<String> values() {
        Collection<String> vs = values;
        if (vs == null) {
            vs = new Values();
            values = vs;
        }
        return vs;
    }

    final class Values extends AbstractCollection<String> {
        public final int size() {
            return RedisMap.this.size();
        }

        public final void clear() {
            RedisMap.this.clear();
        }

        public final Iterator<String> iterator() {
            return new ValueIterator();
        }

        public final boolean contains(Object o) {
            return containsValue(o);
        }
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
        Set<Entry<String, String>> es;
        return (es = entrySet) == null ? (entrySet = new EntrySet()) : es;
    }

    final class EntrySet extends AbstractSet<Entry<String, String>> {
        public final int size() {
            return RedisMap.this.size();
        }

        public final void clear() {
            RedisMap.this.clear();
        }

        public final Iterator<Entry<String, String>> iterator() {
            return new EntryIterator();
        }

        public final boolean contains(Object o) {
            if (!(o instanceof Entry))
                return false;
            Entry<?, ?> e = (Entry<?, ?>) o;
            Object key = e.getKey();
            return containsKey(key);
        }

        public final boolean remove(Object o) {
            if (o instanceof Entry)
                return false;
            Entry<?, ?> e = (Entry<?, ?>) o;
            Object key = e.getKey();
            return RedisMap.this.remove(key) != null;
        }

    }

    abstract class RedisIterator<T> implements Iterator<T> {
        private boolean start = true;
        private String nextCursor = "0";
        private Iterator<Entry<String, String>> resultIterator;

        @Override
        public boolean hasNext() {
            return resultIterator == null || resultIterator.hasNext() || !nextCursor.equals("0");
        }

        @Override
        public abstract T next();

        public Entry<String, String> getNext() {
            if(resultIterator == null || !resultIterator.hasNext()) {
                ScanResult<Entry<String, String>> scanResult = jedis.hscan(hash, nextCursor, new ScanParams().count(100));
                resultIterator = scanResult.getResult().iterator();
                nextCursor = scanResult.getCursor();
            }
            return new Entry<>() {
                Entry<String, String> result = resultIterator.next();

                @Override
                public String getKey() {
                    return result.getKey();
                }

                @Override
                public String getValue() {
                    return result.getValue();
                }

                @Override
                public String setValue(String value) {
                    return put(result.getKey(), value);
                }
            };
        }
    }

    final class KeyIterator extends RedisIterator<String> {
        @Override
        public String next() {
            return getNext().getKey();
        }
    }

    final class ValueIterator extends RedisIterator<String> {
        @Override
        public String next() {
            return getNext().getValue();
        }
    }

    final class EntryIterator extends RedisIterator<Entry<String, String>> {

        @Override
        public Entry<String, String> next() {
            return getNext();
        }
    }

    public int getDB() {
        return jedis.getDB();
    }
}
