package redis;

import java.util.List;

public class CommandHandler {

    private final Store store;

    public CommandHandler(Store store) {
        this.store = store;
    }

    public byte[] handle(List<String> command) {

        String name = command.get(0).toUpperCase();

        try {
            switch (name) {

                case "PING": {
                    if (command.size() == 1) {
                        return RespSerializer.simpleString("PONG");
                    }
                    return RespSerializer.bulkString(command.get(1));
                }

                case "ECHO": {
                    if (command.size() < 2) {
                        return RespSerializer.error(
                            "wrong number of arguments for 'echo'"
                        );
                    }
                    return RespSerializer.bulkString(command.get(1));
                }

                case "SET": {
                    if (command.size() < 3) {
                        return RespSerializer.error(
                            "wrong number of arguments for 'set'"
                        );
                    }
                    String key = command.get(1);
                    String value = command.get(2);
                    long expiryMs = -1;
                    boolean nx = false;
                    boolean xx = false;

                    int i = 3;
                    while (i < command.size()) {
                        String option = command.get(i).toUpperCase();
                        if (option.equals("EX")
                                && i + 1 < command.size()) {
                            expiryMs = Long.parseLong(
                                command.get(i + 1)) * 1000;
                            i += 2;
                        } else if (option.equals("PX")
                                && i + 1 < command.size()) {
                            expiryMs = Long.parseLong(
                                command.get(i + 1));
                            i += 2;
                        } else if (option.equals("NX")) {
                            nx = true;
                            i++;
                        } else if (option.equals("XX")) {
                            xx = true;
                            i++;
                        } else {
                            return RespSerializer.error(
                                "invalid option for 'set': "
                                + command.get(i)
                            );
                        }
                    }

                    if (nx && store.exists(key) == 1) {
                        return RespSerializer.nullBulkString();
                    }
                    if (xx && store.exists(key) == 0) {
                        return RespSerializer.nullBulkString();
                    }

                    String result = store.set(key, value, expiryMs);
                    return RespSerializer.simpleString(result);
                }

                case "GET": {
                    if (command.size() < 2) {
                        return RespSerializer.error(
                            "wrong number of arguments for 'get'"
                        );
                    }
                    String value = store.get(command.get(1));
                    return RespSerializer.bulkString(value);
                }

                case "DEL": {
                    if (command.size() < 2) {
                        return RespSerializer.error(
                            "wrong number of arguments for 'del'"
                        );
                    }
                    int deleted = store.del(
                        command.subList(1, command.size())
                    );
                    return RespSerializer.integer(deleted);
                }

                case "EXISTS": {
                    if (command.size() < 2) {
                        return RespSerializer.error(
                            "wrong number of arguments for 'exists'"
                        );
                    }
                    int exists = store.exists(command.get(1));
                    return RespSerializer.integer(exists);
                }

                case "EXPIRE": {
                    if (command.size() < 3) {
                        return RespSerializer.error(
                            "wrong number of arguments for 'expire'"
                        );
                    }
                    long seconds = Long.parseLong(command.get(2));
                    int result = store.expire(command.get(1), seconds);
                    return RespSerializer.integer(result);
                }

                case "TTL": {
                    if (command.size() < 2) {
                        return RespSerializer.error(
                            "wrong number of arguments for 'ttl'"
                        );
                    }
                    long ttl = store.ttl(command.get(1));
                    return RespSerializer.integer(ttl);
                }

                case "PTTL": {
                    if (command.size() < 2) {
                        return RespSerializer.error(
                            "wrong number of arguments for 'pttl'"
                        );
                    }
                    long pttl = store.pttl(command.get(1));
                    return RespSerializer.integer(pttl);
                }

                case "PERSIST": {
                    if (command.size() < 2) {
                        return RespSerializer.error(
                            "wrong number of arguments for 'persist'"
                        );
                    }
                    int result = store.persist(command.get(1));
                    return RespSerializer.integer(result);
                }

                case "INCR": {
                    if (command.size() < 2) {
                        return RespSerializer.error(
                            "wrong number of arguments for 'incr'"
                        );
                    }
                    long value = store.incr(command.get(1));
                    return RespSerializer.integer(value);
                }

                case "DECR": {
                    if (command.size() < 2) {
                        return RespSerializer.error(
                            "wrong number of arguments for 'decr'"
                        );
                    }
                    long value = store.decr(command.get(1));
                    return RespSerializer.integer(value);
                }

                case "INCRBY": {
                    if (command.size() < 3) {
                        return RespSerializer.error(
                            "wrong number of arguments for 'incrby'"
                        );
                    }
                    long amount = Long.parseLong(command.get(2));
                    long value = store.incrby(command.get(1), amount);
                    return RespSerializer.integer(value);
                }

                case "DECRBY": {
                    if (command.size() < 3) {
                        return RespSerializer.error(
                            "wrong number of arguments for 'decrby'"
                        );
                    }
                    long amount = Long.parseLong(command.get(2));
                    long value = store.decrby(command.get(1), amount);
                    return RespSerializer.integer(value);
                }

                case "STRLEN": {
                    if (command.size() < 2) {
                        return RespSerializer.error(
                            "wrong number of arguments for 'strlen'"
                        );
                    }
                    int length = store.strlen(command.get(1));
                    return RespSerializer.integer(length);
                }

                case "GETDEL": {
                    if (command.size() < 2) {
                        return RespSerializer.error(
                            "wrong number of arguments for 'getdel'"
                        );
                    }
                    String value = store.getdel(command.get(1));
                    return RespSerializer.bulkString(value);
                }

                case "SETNX": {
                    if (command.size() < 3) {
                        return RespSerializer.error(
                            "wrong number of arguments for 'setnx'"
                        );
                    }
                    int result = store.setnx(
                        command.get(1),
                        command.get(2)
                    );
                    return RespSerializer.integer(result);
                }

                case "MSET": {
                    if (command.size() < 3
                            || command.size() % 2 == 0) {
                        return RespSerializer.error(
                            "wrong number of arguments for 'mset'"
                        );
                    }
                    String result = store.mset(
                        command.subList(1, command.size())
                    );
                    return RespSerializer.simpleString(result);
                }

                case "MGET": {
                    if (command.size() < 2) {
                        return RespSerializer.error(
                            "wrong number of arguments for 'mget'"
                        );
                    }
                    List<String> values = store.mget(
                        command.subList(1, command.size())
                    );
                    return serializeArray(values);
                }

                case "APPEND": {
                    if (command.size() < 3) {
                        return RespSerializer.error(
                            "wrong number of arguments for 'append'"
                        );
                    }
                    int length = store.append(
                        command.get(1),
                        command.get(2)
                    );
                    return RespSerializer.integer(length);
                }

                case "KEYS": {
                    List<String> keys = store.keys();
                    return serializeArray(keys);
                }

                case "DBSIZE": {
                    return RespSerializer.integer(store.dbsize());
                }

                case "FLUSHALL": {
                    store.flushall();
                    return RespSerializer.simpleString("OK");
                }

                case "TYPE": {
                    if (command.size() < 2) {
                        return RespSerializer.error(
                            "wrong number of arguments for 'type'"
                        );
                    }
                    String type = store.type(command.get(1));
                    return RespSerializer.simpleString(type);
                }

                case "RENAME": {
                    if (command.size() < 3) {
                        return RespSerializer.error(
                            "wrong number of arguments for 'rename'"
                        );
                    }
                    String result = store.rename(
                        command.get(1),
                        command.get(2)
                    );
                    return RespSerializer.simpleString(result);
                }

                case "LPUSH": {
                    if (command.size() < 3) {
                        return RespSerializer.error(
                            "wrong number of arguments for 'lpush'"
                        );
                    }
                    long length = store.lpush(
                        command.get(1),
                        command.subList(2, command.size())
                    );
                    return RespSerializer.integer(length);
                }

                case "RPUSH": {
                    if (command.size() < 3) {
                        return RespSerializer.error(
                            "wrong number of arguments for 'rpush'"
                        );
                    }
                    long length = store.rpush(
                        command.get(1),
                        command.subList(2, command.size())
                    );
                    return RespSerializer.integer(length);
                }

                case "LPOP": {
                    if (command.size() < 2) {
                        return RespSerializer.error(
                            "wrong number of arguments for 'lpop'"
                        );
                    }
                    String value = store.lpop(command.get(1));
                    return RespSerializer.bulkString(value);
                }

                case "RPOP": {
                    if (command.size() < 2) {
                        return RespSerializer.error(
                            "wrong number of arguments for 'rpop'"
                        );
                    }
                    String value = store.rpop(command.get(1));
                    return RespSerializer.bulkString(value);
                }

                case "LLEN": {
                    if (command.size() < 2) {
                        return RespSerializer.error(
                            "wrong number of arguments for 'llen'"
                        );
                    }
                    long length = store.llen(command.get(1));
                    return RespSerializer.integer(length);
                }

                case "LRANGE": {
                    if (command.size() < 4) {
                        return RespSerializer.error(
                            "wrong number of arguments for 'lrange'"
                        );
                    }
                    int start = Integer.parseInt(command.get(2));
                    int stop = Integer.parseInt(command.get(3));
                    List<String> items = store.lrange(
                        command.get(1), start, stop
                    );
                    return serializeArray(items);
                }

                case "LINDEX": {
                    if (command.size() < 3) {
                        return RespSerializer.error(
                            "wrong number of arguments for 'lindex'"
                        );
                    }
                    int index = Integer.parseInt(command.get(2));
                    String value = store.lindex(
                        command.get(1), index
                    );
                    return RespSerializer.bulkString(value);
                }

                case "HSET": {
                    // HSET needs key + at least one field-value pair
                    // so minimum 3 args total
                    // and field-value pairs must be even
                    if (command.size() < 4
                            || command.size() % 2 != 0) {
                        return RespSerializer.error(
                            "wrong number of arguments for 'hset'"
                        );
                    }
                    int added = store.hset(
                        command.get(1),
                        command.subList(2, command.size())
                    );
                    return RespSerializer.integer(added);
                }

                case "HGET": {
                    if (command.size() < 3) {
                        return RespSerializer.error(
                            "wrong number of arguments for 'hget'"
                        );
                    }
                    String value = store.hget(
                        command.get(1),
                        command.get(2)
                    );
                    return RespSerializer.bulkString(value);
                }

                case "HGETALL": {
                    if (command.size() < 2) {
                        return RespSerializer.error(
                            "wrong number of arguments for 'hgetall'"
                        );
                    }
                    List<String> result = store.hgetall(
                        command.get(1)
                    );
                    return serializeArray(result);
                }

                case "HDEL": {
                    if (command.size() < 3) {
                        return RespSerializer.error(
                            "wrong number of arguments for 'hdel'"
                        );
                    }
                    int deleted = store.hdel(
                        command.get(1),
                        command.subList(2, command.size())
                    );
                    return RespSerializer.integer(deleted);
                }

                case "HEXISTS": {
                    if (command.size() < 3) {
                        return RespSerializer.error(
                            "wrong number of arguments for 'hexists'"
                        );
                    }
                    int exists = store.hexists(
                        command.get(1),
                        command.get(2)
                    );
                    return RespSerializer.integer(exists);
                }

                case "HLEN": {
                    if (command.size() < 2) {
                        return RespSerializer.error(
                            "wrong number of arguments for 'hlen'"
                        );
                    }
                    int length = store.hlen(command.get(1));
                    return RespSerializer.integer(length);
                }

                case "HKEYS": {
                    if (command.size() < 2) {
                        return RespSerializer.error(
                            "wrong number of arguments for 'hkeys'"
                        );
                    }
                    List<String> keys = store.hkeys(command.get(1));
                    return serializeArray(keys);
                }

                case "HVALS": {
                    if (command.size() < 2) {
                        return RespSerializer.error(
                            "wrong number of arguments for 'hvals'"
                        );
                    }
                    List<String> vals = store.hvals(command.get(1));
                    return serializeArray(vals);
                }

                default: {
                    return RespSerializer.error(
                        "unknown command '" + command.get(0) + "'"
                    );
                }
            }

        } catch (RuntimeException e) {
            return RespSerializer.error(e.getMessage());
        }
    }

    private byte[] serializeArray(List<String> items) {
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(items.size()).append("\r\n");
        for (String item : items) {
            if (item == null) {
                sb.append("$-1\r\n");
            } else {
                sb.append("$")
                  .append(item.length())
                  .append("\r\n")
                  .append(item)
                  .append("\r\n");
            }
        }
        return sb.toString().getBytes();
    }
}