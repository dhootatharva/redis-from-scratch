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
                    if (command.size() >= 5) {
                        String option = command.get(3).toUpperCase();
                        long amount = Long.parseLong(command.get(4));
                        if (option.equals("EX")) {
                            expiryMs = amount * 1000;
                        } else if (option.equals("PX")) {
                            expiryMs = amount;
                        } else {
                            return RespSerializer.error(
                                "invalid option for 'set': "
                                + command.get(3)
                            );
                        }
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