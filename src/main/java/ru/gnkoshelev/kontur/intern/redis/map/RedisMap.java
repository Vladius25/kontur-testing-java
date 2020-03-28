package ru.gnkoshelev.kontur.intern.redis.map;

import jdk.jshell.spi.ExecutionControl;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import javax.swing.text.html.HTMLDocument;
import java.util.*;
import java.util.function.Consumer;

import static java.util.Objects.hash;

public class RedisMap implements Map<String, String> {
    private Jedis jedis;
    transient Set<String> keySet;
    transient Collection<String> values;
    transient Set<Map.Entry<String, String>> entrySet;

    public RedisMap() {
        this("localhost", 6379);
    }

    public RedisMap(String host, int port) {
        jedis = new Jedis(host, port);
        int db = 0;
        do {
            jedis.select(db);
            db++;
        } while (!isEmpty());
    }

    public RedisMap(String host, int port, int db) {
        jedis = new Jedis(host, port);
        jedis.select(db);
    }

    @Override
    public int size() {
        return jedis.dbSize().intValue();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        return jedis.exists((String) key);
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
        return jedis.get((String) key);
    }

    @Override
    public String put(String key, String value) {
        jedis.set(key, value);
        return value;
    }

    @Override
    public String remove(Object key) {
        String value = get(key);
        jedis.del((String) key);
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
        jedis.flushDB();
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
        Set<Map.Entry<String, String>> es;
        return (es = entrySet) == null ? (entrySet = new EntrySet()) : es;
    }

    final class EntrySet extends AbstractSet<Map.Entry<String, String>> {
        public final int size() {
            return RedisMap.this.size();
        }

        public final void clear() {
            RedisMap.this.clear();
        }

        public final Iterator<Map.Entry<String, String>> iterator() {
            return new EntryIterator();
        }

        public final boolean contains(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
            Object key = e.getKey();
            return containsKey(key);
        }

        public final boolean remove(Object o) {
            if (o instanceof Map.Entry)
                return false;
            Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
            Object key = e.getKey();
            return RedisMap.this.remove(key) != null;
        }

    }

    abstract class RedisIterator<T> implements Iterator<T> {
        private boolean start = true;
        private String nextCursor = "0";

        @Override
        public boolean hasNext() {
            return start || !nextCursor.equals("0");
        }

        @Override
        public abstract T next();

        public Entry<String, String> getNext() {
            start = false;
            ScanResult<String> scanResult = jedis.scan(nextCursor, new ScanParams().count(1));
            nextCursor = scanResult.getCursor();
            return new Map.Entry<>() {
                String key = scanResult.getResult().get(0);

                @Override
                public String getKey() {
                    return key;
                }

                @Override
                public String getValue() {
                    return get(key);
                }

                @Override
                public String setValue(String value) {
                    return put(key, value);
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

    final class EntryIterator extends RedisIterator<Map.Entry<String, String>> {

        @Override
        public Map.Entry<String, String> next() {
            return getNext();
        }
    }

    public int getDB() {
        return jedis.getDB();
    }
}
