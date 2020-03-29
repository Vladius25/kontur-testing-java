package ru.gnkoshelev.kontur.intern.redis.map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.lang.ref.Cleaner;
import java.util.*;
import java.util.function.Function;

public class RedisMap implements Map<String, String>, AutoCloseable {
    private final Jedis jedis;
    private final String hash;
    private transient Set<String> keySet;
    private transient Collection<String> values;
    private transient Set<Entry<String, String>> entrySet;

    private static class State implements Runnable {
        private Jedis jedis;
        private String hash;

        State(Jedis jedis, String hash) {
            this.jedis = jedis;
            this.hash = hash;
        }

        public void run() {
            jedis.del(hash);
            jedis.close();
        }
    }

    private static final Cleaner cleaner = Cleaner.create();
    private final Cleaner.Cleanable cleanable;

    public RedisMap() {
        this("localhost", 6379, 0);
    }

    public RedisMap(String hash) {
        this("localhost", 6379, hash,0);
    }

    public RedisMap(String host, int port) {
        this(host, port, 0);
    }

    public RedisMap(String host, int port, int db) {
        this(host, port, (jedis) -> {
            Random rand = new Random();
            String _hash;
            do
                _hash = String.valueOf(rand.nextInt(10000));
            while (jedis.exists(_hash));
            return _hash;
        }, db);
    }

    public RedisMap(String host, int port, String hash) {
        this(host, port, hash, 0);
    }

    public RedisMap(String host, int port, String hash, int db) {
        this(host, port, (jedis) -> hash, db);
    }

    private RedisMap(String host, int port, Function<Jedis, String> hashFunc, int db) {
        jedis = new Jedis(host, port);
        jedis.select(db);
        hash = hashFunc.apply(jedis);
        State state = new State(jedis, hash);
        cleanable = cleaner.register(this, state);
        Runtime.getRuntime().addShutdownHook(new Thread(cleanable::clean));
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
        for (String val : values())
            if (val.equals(value))
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
        jedis.hmset(hash, new HashMap<>(m));
    }

    @Override
    public void clear() {
        jedis.del(hash);
    }


    @Override
    public Set<String> keySet() {
        Set<String> ks;
        return (ks = keySet) == null ? (keySet = new KeySet()) : ks;
    }

    @Override
    public void close() {
        cleanable.clean();
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
        Collection<String> vs;
        return (vs = values) == null ? (values = new Values()) : vs;
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
            if (!(o instanceof Entry))
                return false;
            Entry<?, ?> e = (Entry<?, ?>) o;
            Object key = e.getKey();
            return RedisMap.this.remove(key) != null;
        }

    }

    abstract class RedisIterator<T> implements Iterator<T> {
        private String nextCursor = "0";
        private Iterator<Entry<String, String>> resultIterator;

        @Override
        public boolean hasNext() {
            return resultIterator == null || resultIterator.hasNext() || !nextCursor.equals("0");
        }

        @Override
        public abstract T next();

        public Entry<String, String> getNext() {
            if (resultIterator == null || !resultIterator.hasNext()) {
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
