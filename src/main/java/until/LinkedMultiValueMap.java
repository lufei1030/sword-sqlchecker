package until;


import java.util.*;

/**
 *
 * @create 2020/4/26 10:42
 */
public class LinkedMultiValueMap<K,object> implements MultiValueMap<K, object> {
    protected Map<K, List<object>> mSource = new LinkedHashMap<K, List<object>>();

    public LinkedMultiValueMap() {
    }

    @Override
    public void add(K key, object value) {
        if (key != null) {
            // 如果有这个Key就继续添加Value，没有就创建一个List并添加Value
            if (!mSource.containsKey(key))
                mSource.put(key, new ArrayList<object>(2));
            mSource.get(key).add(value);
        }
    }

    @Override
    public void add(K key, List<object> values) {
        // 遍历添加进来的List的Value，调用上面的add(K, V)方法添加
        for (object value : values) {
            add(key, value);
        }
    }

    @Override
    public void set(K key, object value) {
        // 移除这个Key，添加新的Key-Value
        mSource.remove(key);
        add(key, value);
    }

    @Override
    public void set(K key, List<object> values) {
        // 移除Key，添加List<V>
        mSource.remove(key);
        add(key, values);
    }

    @Override
    public void set(Map<K, List<object>> map) {
        // 移除所有值，便利Map里的所有值添加进来
        mSource.clear();
        mSource.putAll(map);
    }

    @Override
    public List<object> remove(K key) {
        return mSource.remove(key);
    }

    @Override
    public void clear() {
        mSource.clear();
    }

    @Override
    public Set<K> keySet() {
        return mSource.keySet();
    }

    @Override
    public List<object> values() {
        // 创建一个临时List保存所有的Value
        List<object> allValues = new ArrayList<object>();

        // 便利所有的Key的Value添加到临时List
        Set<K> keySet = mSource.keySet();
        for (K key : keySet) {
            allValues.addAll(mSource.get(key));
        }
        return allValues;
    }

    @Override
    public List<object> getValues(K key) {
        return mSource.get(key);
    }

    @Override
    public object getValue(K key, int index) {
        List<object> values = mSource.get(key);
        if (values != null && index < values.size())
            return values.get(index);
        return null;
    }

    @Override
    public int size() {
        return mSource.size();
    }

    @Override
    public boolean isEmpty() {
        return mSource.isEmpty();
    }

    @Override
    public boolean containsKey(K key) {
        return mSource.containsKey(key);
    }

}
