package mysql.binlog.replicator.util;

import java.util.*;
import java.util.regex.Pattern;

public class RegexHashMap<V> implements Map<String, V> {
    // key -> regex
    private final Map<String, String> ref;
    // key -> value
    private final Map<String, V> internalMap;
    // regex list
    private final List<PatternMatcher> matchers;

    public RegexHashMap(int initialCapacity, float loadFactor) {
        this.ref = new WeakHashMap<>();
        this.internalMap = new HashMap<>(initialCapacity, loadFactor);
        this.matchers = new ArrayList<>(initialCapacity);
    }

    public RegexHashMap(int initialCapacity) {
        this.ref = new WeakHashMap<>();
        this.internalMap = new HashMap<>(initialCapacity);
        this.matchers = new ArrayList<>(initialCapacity);
    }

    public RegexHashMap() {
        this.ref = new WeakHashMap<>();
        this.internalMap = new HashMap<>();
        this.matchers = new ArrayList<>();
    }

    @Override
    public V get(Object key) {
        if (!ref.containsKey(key)) {
            for (PatternMatcher matcher : matchers) {
                if (matcher.matched((String) key)) {
                    synchronized (this) {
                        ref.put((String) key, matcher.regex);
                    }
                    break;
                }
            }
        }
        if (ref.containsKey(key)) {
            return internalMap.get(ref.get(key));
        }
        return internalMap.get(key);
    }

    @Override
    public V put(String key, V value) {
        V oldValue = internalMap.put(key, value);
        if (oldValue == null) {
            matchers.add(new PatternMatcher(key));
        }
        return oldValue;
    }

    @Override
    public V remove(Object key) {
        V v = internalMap.remove(key);
        if (v != null) {
            for (Iterator<PatternMatcher> iter = matchers.iterator(); iter.hasNext(); ) {
                PatternMatcher matcher = iter.next();
                if (matcher.regex.equals(key)) {
                    iter.remove();
                    break;
                }
            }
            ref.entrySet().removeIf(entry -> entry.getValue().equals(key));
        }
        return v;
    }

    @Override
    public void putAll(Map<? extends String, ? extends V> m) {
        for (Entry<? extends String, ? extends V> entry : m.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public Set<Entry<String, V>> entrySet() {
        return internalMap.entrySet();
    }

    @Override
    public int size() {
        return internalMap.size();
    }

    @Override
    public boolean isEmpty() {
        return internalMap.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        if (!ref.containsKey(key)) {
            for (PatternMatcher matcher : matchers) {
                if (matcher.matched((String) key)) {
                    ref.put((String) key, matcher.regex);
                    break;
                }
            }
        }
        return ref.containsKey(key) || internalMap.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return internalMap.containsValue(value);
    }

    @Override
    public void clear() {
        internalMap.clear();
        matchers.clear();
        ref.clear();
    }

    @Override
    public Set<String> keySet() {
        return internalMap.keySet();
    }

    @Override
    public Collection<V> values() {
        return internalMap.values();
    }

    @Override
    public String toString() {
        return "RegexHashMap{" +
                "ref=" + ref +
                ", internalMap=" + internalMap +
                '}';
    }

    private static class PatternMatcher {
        private final String regex;
        private final Pattern compiled;

        private PatternMatcher(String name) {
            this.regex = name;
            this.compiled = Pattern.compile(regex);
        }

        private boolean matched(String str) {
            return compiled.matcher(str).matches();
        }
    }
}
