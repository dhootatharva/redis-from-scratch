package redis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// The set of keys that have a deadline, built so that picking one at
// random is fast.
//
// A HashSet can add and remove in constant time but cannot hand out a
// random element without walking its buckets. A plain list can (pick a
// random index) but removing from the middle means shifting everything
// after it. This class combines the two:
//
//   keys       a list, so  keys.get(randomIndex)  is constant time
//   positions  key -> its index in the list, so a key can be found
//
// Removing uses the "swap with the last" trick: move the last key into
// the slot being vacated, then drop the last slot. Nothing shifts, so
// add, remove and random access are all constant time.
final class ExpiringKeys {

    private final List<String> keys = new ArrayList<>();
    private final Map<String, Integer> positions = new HashMap<>();

    void add(String key) {
        if (positions.containsKey(key)) return;
        positions.put(key, keys.size());
        keys.add(key);
    }

    void remove(String key) {
        Integer position = positions.remove(key);
        if (position == null) return;

        int last = keys.size() - 1;
        if (position != last) {
            // Fill the hole with the last key and record its new place
            String moved = keys.get(last);
            keys.set(position, moved);
            positions.put(moved, position);
        }
        keys.remove(last);
    }

    String get(int index) {
        return keys.get(index);
    }

    int size() {
        return keys.size();
    }

    boolean isEmpty() {
        return keys.isEmpty();
    }

    void clear() {
        keys.clear();
        positions.clear();
    }
}
