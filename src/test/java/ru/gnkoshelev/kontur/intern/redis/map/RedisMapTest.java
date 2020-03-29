package ru.gnkoshelev.kontur.intern.redis.map;


import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class RedisMapTest {

    final String HOST = "localhost";
    final int PORT = 6379;

    @Test
    public void baseTests() {
        Map<String, String> map1 = new RedisMap(HOST, PORT);
        Map<String, String> map2 = new RedisMap(HOST, PORT);

        map1.put("one", "1");

        map2.put("one", "ONE");
        map2.put("two", "TWO");

        Assert.assertEquals("1", map1.get("one"));
        Assert.assertEquals(1, map1.size());
        Assert.assertEquals(2, map2.size());

        map1.put("one", "first");

        Assert.assertEquals("first", map1.get("one"));
        Assert.assertEquals(1, map1.size());

        Assert.assertTrue(map1.containsKey("one"));
        Assert.assertFalse(map1.containsKey("two"));

        Set<String> keys2 = map2.keySet();
        Assert.assertEquals(2, keys2.size());
        Assert.assertTrue(keys2.contains("one"));
        Assert.assertTrue(keys2.contains("two"));

        Collection<String> values1 = map1.values();
        Assert.assertEquals(1, values1.size());
        Assert.assertTrue(values1.contains("first"));
    }

    public Map<String, String> prepareHashMap() {
        Map<String, String> hashMap = new HashMap<>();

        hashMap.put("test1", "value1");
        hashMap.put("test2", "value2");
        hashMap.put("test3", "value3");

        return hashMap;
    }

    public <T> boolean equalsIgnoreOrder(Collection<T> first, Collection<T> second) {
        for (T e : first)
            if (!second.contains(e))
                return false;
        return true;
    }

    @Before
    public void clearRedis() {
        Jedis jedis = new Jedis(HOST, PORT);
        jedis.flushAll();
    }

    @Test
    public void initDifferentConstructors() {
        RedisMap map = new RedisMap(HOST, PORT);
        RedisMap map1 = new RedisMap(HOST, PORT, "test");
        RedisMap map2 = new RedisMap(HOST, PORT, 0);
        RedisMap map3 = new RedisMap(HOST, PORT, "test", 0);

        Assert.assertTrue(map.isEmpty());
        Assert.assertTrue(map1.isEmpty());
        Assert.assertTrue(map2.isEmpty());
        Assert.assertTrue(map3.isEmpty());

    }

    @Test
    public void constructorGenDifferentHashes() {
        RedisMap map1 = new RedisMap(HOST, PORT);
        RedisMap map2 = new RedisMap(HOST, PORT);

        Assert.assertNotEquals(map1.getHash(), map2.getHash());
    }

    @Test
    public void createLotsDifferentInstances() {
        String hash = "-1";
        for (int i = 0; i < 100; i++) {
            RedisMap map = new RedisMap(HOST, PORT);
            Assert.assertNotEquals(hash, map.getHash());
            Assert.assertFalse(map.containsKey(hash));
            hash = map.getHash();
            map.put(hash, hash);
        }
    }

    @Test
    public void connectToSpecificDB() {
        RedisMap map = new RedisMap(HOST, PORT, 2);
        Assert.assertEquals(2, map.getDB());
    }

    @Test
    public void connectToSpecificHash() {
        Map<String, String> map = new RedisMap(HOST, PORT, "myhash");

        map.put("test", "secret");
        map = new RedisMap(HOST, PORT, "myhash");
        Assert.assertEquals("secret", map.get("test"));
    }

    @Test
    public void putKeyValue() {
        Map<String, String> map = new RedisMap(HOST, PORT, 0);

        map.put("save", "data");
        Assert.assertTrue(map.containsKey("save"));
        Assert.assertEquals("data", map.put("save", "newdata"));
        Assert.assertEquals("newdata", map.get("save"));
    }

    @Test
    public void useOneKeyValueFromDifferentApps() {
        Map<String, String> map1 = new RedisMap(HOST, PORT, "brotherhood");
        map1.put("test1", "value1");
        map1.put("test2", "value2");
        Map<String, String> map2 = new RedisMap(HOST, PORT, "brotherhood");

        Assert.assertEquals(2, map2.size());
        Assert.assertEquals("value1", map2.get("test1"));
        Assert.assertEquals("value2", map2.get("test2"));
    }

    @Test
    public void containsValue() {
        Map<String, String> map = new RedisMap(HOST, PORT, 0);

        map.put("save", "data");
        map.put("save2", "data2");
        Assert.assertTrue(map.containsValue("data2"));
        Assert.assertFalse(map.containsValue("nodata"));
    }

    @Test
    public void createNewHash() {
        Map<String, String> map1 = new RedisMap(HOST, PORT);
        Map<String, String> map2 = new RedisMap(HOST, PORT);

        map1.put("test", "test");
        Assert.assertFalse(map2.containsKey("test"));
    }

    @Test
    public void sizeIsCorrect() {
        Map<String, String> map1 = new RedisMap(HOST, PORT, "one");
        Map<String, String> map2 = new RedisMap(HOST, PORT, "two");

        map1.put("test", "testing");
        map2.put("data", "testing");
        Assert.assertEquals(1, map1.size());
        Assert.assertEquals(1, map2.size());
        clearRedis();
        Assert.assertTrue(map1.isEmpty());
    }

    @Test
    public void sameHashesInDifferentDb() {
        Map<String, String> map1 = new RedisMap(HOST, PORT, "singleton", 0);
        Map<String, String> map2 = new RedisMap(HOST, PORT, "singleton", 1);

        map1.put("test", "testing");
        map2.put("data", "testing");
        Assert.assertEquals(1, map1.size());
        Assert.assertEquals(1, map2.size());
    }

    @Test
    public void getKeyValue() {
        Map<String, String> map = new RedisMap(HOST, PORT);

        map.put("some", "value");
        Assert.assertEquals("value", map.get("some"));
        Assert.assertNull(map.get("keynotpresented"));
    }

    @Test
    public void removeKey() {
        Map<String, String> map = new RedisMap(HOST, PORT);

        map.put("some", "value");
        Assert.assertEquals("value", map.remove("some"));
        Assert.assertFalse(map.containsKey("some"));
        Assert.assertNull(map.remove("keynotpresented"));
    }

    @Test
    public void putAll() {
        Map<String, String> map = new RedisMap(HOST, PORT);
        Map<String, String> hashMap = prepareHashMap();

        map.putAll(hashMap);
        Assert.assertTrue(equalsIgnoreOrder(hashMap.values(), map.values()));
    }

    @Test
    public void clear() {
        Map<String, String> map1 = new RedisMap(HOST, PORT);
        Map<String, String> map2 = new RedisMap(HOST, PORT);

        map1.put("test1", "test1");
        map2.put("test2", "test2");

        map1.clear();
        Assert.assertTrue(map1.isEmpty());
        Assert.assertFalse(map2.isEmpty());
    }

    @Test
    public void keySetIterator() {
        Map<String, String> map = new RedisMap(HOST, PORT);
        Map<String, String> hashMap = prepareHashMap();
        map.putAll(hashMap);

        Set<String> set = map.keySet();
        Assert.assertEquals(hashMap.size(), set.size());
        Assert.assertTrue(equalsIgnoreOrder(hashMap.keySet(), map.keySet()));

        String el = hashMap.keySet().iterator().next();
        Assert.assertTrue(set.contains(el));
        set.remove(el);
        Assert.assertEquals(hashMap.size() - 1, set.size());
    }

    @Test
    public void valuesIterator() {
        Map<String, String> map = new RedisMap(HOST, PORT);
        Map<String, String> hashMap = prepareHashMap();
        map.putAll(hashMap);

        Collection<String> set = map.values();
        Assert.assertEquals(hashMap.size(), set.size());
        Assert.assertTrue(equalsIgnoreOrder(hashMap.values(), map.values()));

        String el = hashMap.values().iterator().next();
        Assert.assertTrue(set.contains(el));
    }

    @Test
    public void EntrySetIterator() {
        Map<String, String> map = new RedisMap(HOST, PORT);
        Map<String, String> hashMap = prepareHashMap();
        map.putAll(hashMap);

        Set<Entry<String, String>> set = map.entrySet();
        Assert.assertEquals(hashMap.size(), set.size());
        Assert.assertTrue(equalsIgnoreOrder(hashMap.entrySet(), map.entrySet()));
        Entry<String, String> el = hashMap.entrySet().iterator().next();
        Assert.assertTrue(set.contains(el));
        set.remove(el);
        Assert.assertEquals(hashMap.size() - 1, set.size());

        Assert.assertFalse(set.contains(new Object()));
    }

    @Test
    public void valueEntryKeySetBackedByMap() {
        Map<String, String> map = new RedisMap(HOST, PORT);
        map.put("test1", "value1");
        map.put("test2", "value2");

        Set<String> keySet = map.keySet();
        Collection<String> values = map.values();
        Set<Entry<String, String>> entrySet = map.entrySet();

        map.put("test3", "value3");
        map.put("test4", "value4");

        Assert.assertEquals(4, keySet.size());
        Assert.assertEquals(4, values.size());
        Assert.assertEquals(4, entrySet.size());

        keySet.remove("test3");

        Assert.assertFalse(map.containsKey("test3"));
        Assert.assertEquals(3, map.size());
        Assert.assertEquals(3, values.size());
        Assert.assertEquals(3, entrySet.size());

        entrySet.forEach((x) -> x.setValue("breakValue"));

        Assert.assertEquals("breakValue", map.get("test4"));
        Assert.assertEquals("breakValue", values.iterator().next());
    }

    @Test
    public void changeInUseCounter() {
        RedisMap map1 = new RedisMap(HOST, PORT, "inUse", 2);
        Assert.assertEquals("1", map1.get("___inUse___"));

        RedisMap map2 = new RedisMap(HOST, PORT, "inUse", 2);
        Assert.assertEquals("2", map2.get("___inUse___"));

        map2.close();
        Assert.assertEquals("1", map1.get("___inUse___"));
    }

    @Test
    public void breakInUseCounter() throws InterruptedException {
        RedisMap map1 = new RedisMap(HOST, PORT, "badInUse");
        map1.put("___inUse___", "NaN");
        map1.put("9", "aN");
        RedisMap map2 = new RedisMap(HOST, PORT, "badInUse");

        map1.close();
        map1 = new RedisMap(HOST, PORT, "badInUse");
        Assert.assertTrue(map1.isEmpty());
        Assert.assertTrue(map2.isEmpty());
    }

    @Test
    public void cleanAfterGC() throws InterruptedException {
        Map<String, String> map = new RedisMap(HOST, PORT, "GC", 1);
        map.put("test1", "value1");

        map = null;
        System.gc();
        Thread.sleep(30);

        map = new RedisMap(HOST, PORT, "GC", 1);
        Assert.assertTrue(map.isEmpty());
    }

    @Test
    public void notCleanAfterGCWhenInUsed() throws InterruptedException {
        Map<String, String> map1 = new RedisMap(HOST, PORT, "noGC", 2);
        Map<String, String> map2 = new RedisMap(HOST, PORT, "noGC", 2);
        Map<String, String> map3 = new RedisMap(HOST, PORT, "noGC", 2);
        map1.put("test1", "value1");
        map2.put("test2", "value2");
        map3.put("test3", "value3");

        map1 = null;
        System.gc();
        map2 = null;
        System.gc();
        Thread.sleep(30);

        map1 = new RedisMap(HOST, PORT, "noGC", 2);
        Assert.assertFalse(map1.isEmpty());
        Assert.assertFalse(map3.isEmpty());
    }
}
