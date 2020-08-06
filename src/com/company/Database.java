package com.company;

import java.util.*;

public class Database {
    Map<String, TreeMap<Integer, String>> mem;
    Map<Integer, Set<ValueItem>> transactionLog;
    Map<String, Integer> valueCounts;
    public static final String NULL = "NULL";
    public static final String NONE = "NONE";
    public static final String NO_TRANSACTION = "NO TRANSACTION";
    public static final String EMPTY = "";
    int currentTransaction;

    public Database() {
        currentTransaction = 0;
        mem = new HashMap<>();
        transactionLog = new HashMap<>();
        valueCounts = new HashMap<>();
    }

    /**
     * Gets the value associated to the key saved under the highest transaction possible.
     * @param key
     * @return
     */
    public String get(String key) {
        if (!mem.containsKey(key)) {
            return NULL;
        } else {
            Map.Entry<Integer, String> entry = mem.get(key).lastEntry();
            return entry != null ? String.valueOf(entry.getValue()) : NULL;
        }
    }

    /**
     * Unsets the key under the current transaction level. If the current transaction level is greater than the last
     * saved value, we set a null for the current value. Otherwise, we delete the current entry.
     * @param key
     */
    public void unset(String key) {
        if (mem.containsKey(key)) {
            Map.Entry<Integer, String> entry = mem.get(key).lastEntry();
            String oldValue = entry.getValue();
            if (entry.getKey() < currentTransaction) {
                mem.get(key).put(currentTransaction, null);
            } else {
                mem.get(key).remove(currentTransaction);
            }
            // if the value exist in the map, decrease its count.
            int count = valueCounts.getOrDefault(oldValue, 0);
            if (count > 0) {
                valueCounts.put(oldValue, count - 1);
            }
            // Save the action in the log
            ValueItem valueItem = new ValueItem(key, oldValue, false);
            transactionLog.putIfAbsent(currentTransaction, new HashSet<>());
            transactionLog.get(currentTransaction).add(valueItem);
        }
    }

    /**
     * Sets the value for the given key under the current transaction.
     * @param key
     * @param value
     */
    public void set(String key, String value) {
        mem.putIfAbsent(key, new TreeMap<Integer, String>());
        Map.Entry<Integer, String> oldEntry = mem.get(key).lastEntry();
        String oldValue = oldEntry != null ? oldEntry.getValue() : NONE;
        mem.get(key).put(currentTransaction, value);

        // If the value has changed, save the change in the transaction log for the counts.
        transactionLog.putIfAbsent(currentTransaction, new HashSet<>());
        boolean isNewValue = !Objects.equals(value, oldValue);
        if (isNewValue) {
            ValueItem valueItem = new ValueItem(key, value, true);
            transactionLog.get(currentTransaction).add(valueItem);
            valueCounts.put(value, valueCounts.getOrDefault(value, 0) + 1);
            if (!Objects.equals(oldValue, NONE)) {
                ValueItem oldItem = new ValueItem(key, oldValue, false);
                transactionLog.get(currentTransaction).add(oldItem);
                valueCounts.put(oldValue, valueCounts.get(oldValue) - 1);
            }
        }
    }

    /**
     * Returns the count directly from the counts map.
     * @param value
     * @return
     */
    public String numEqualTo(String value) {
        if (!valueCounts.containsKey(value)) {
            return "0";
        }
        return String.valueOf(valueCounts.get(value));
    }

    public void begin() {
        currentTransaction++;
    }

    public String rollback() {
        if (currentTransaction == 0) {
            return NO_TRANSACTION;
        }
        Set<String> keys = getKeysFromLogs(transactionLog.get(currentTransaction));
        rollbackKeys(keys, currentTransaction);
        rollbackCounts(transactionLog.get(currentTransaction));
        transactionLog.remove(currentTransaction);
        currentTransaction--;
        return EMPTY;
    }

    public String commit() {
        if (currentTransaction == 0) {
            return NO_TRANSACTION;
        }
        Set<String> keys = getKeysForAllTransactions();
        writeLastValue(keys);

        transactionLog.clear();
        currentTransaction = 0;
        return EMPTY;
    }

    /**
     * Gets the value for the highest transaction in its value tree for each key and creates a fresh new tree with only
     * that value in it (for transaction id = 0).
     * @param keys
     */
    private void writeLastValue(Set<String> keys) {
        for (String key : keys) {
            Map.Entry<Integer, String> entry = mem.get(key).lastEntry();
            String lastValue = entry.getValue();
            mem.put(key, new TreeMap<>());
            mem.get(key).put(0, lastValue);
        }
    }

    /**
     * Returns all the keys affected by any transaction whose id is strictly greater than 0;
     * @return
     */
    private Set<String> getKeysForAllTransactions() {
        Set<String> out = new HashSet<>();
        Set<Integer> transactionIds = transactionLog.keySet();
        for (Integer tId : transactionIds) {
            if (tId > 0) {
                for (ValueItem item : transactionLog.get(tId)) {
                    out.add(item.key);
                }
            }
        }
        return out;
    }

    private Set<String> getKeysFromLogs(Set<ValueItem> items) {
        Set<String> keys = new HashSet<>();
        for (ValueItem item : items) {
            keys.add(item.key);
        }
        return keys;
    }

    /**
     * Rollback keys by remove the last entry in their value trees (which is the same as the one pointed by the
     * current transactionId).
     * @param keys
     * @param transactionId
     */
    private void rollbackKeys(Set<String> keys, int transactionId) {
        for (String key : keys) {
            mem.get(key).remove(transactionId);
        }
    }

    /**
     * Rollbacks counts. If we increased the count, then decrease it and vice versa.
     * @param items
     */
    private void rollbackCounts(Set<ValueItem> items) {
        for (ValueItem item : items) {
            Integer newCount = valueCounts.get(item.value) + (item.increase ? -1 : 1);
            valueCounts.put(item.value, newCount);
        }
    }
}
