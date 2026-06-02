package redis;

import java.util.List;

public class CommandHandler {

    // The store — one instance shared across all clients
    private final Store store;

    public CommandHandler(Store store) {
        this.store = store;
    }

    // Takes a parsed command like ["SET", "name", "Atharva"]
    // Returns the correct RESP bytes to send back to the client
    public byte[] handle(List<String> command) {

        // First element is always the command name
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
                    // SET needs at least key and value
                    if (command.size() < 3) {
                        return RespSerializer.error(
                            "wrong number of arguments for 'set'"
                        );
                    }
                    String result = store.set(
                        command.get(1),
                        command.get(2)
                    );
                    return RespSerializer.simpleString(result);
                }

                case "GET": {
                    if (command.size() < 2) {
                        return RespSerializer.error(
                            "wrong number of arguments for 'get'"
                        );
                    }
                    // get() returns null if key missing
                    // bulkString handles null → $-1\r\n
                    String value = store.get(command.get(1));
                    return RespSerializer.bulkString(value);
                }

                case "DEL": {
                    if (command.size() < 2) {
                        return RespSerializer.error(
                            "wrong number of arguments for 'del'"
                        );
                    }
                    // DEL can take multiple keys
                    // command is ["DEL", "key1", "key2", ...]
                    // subList(1, size) gives ["key1", "key2", ...]
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
                    // MSET needs even number of args after command name
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
                    // MGET returns an array response
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

                default: {
                    return RespSerializer.error(
                        "unknown command '" + command.get(0) + "'"
                    );
                }
            }

        } catch (RuntimeException e) {
            // Catches INCR/DECR on non-numeric values etc
            return RespSerializer.error(e.getMessage());
        }
    }

    // Helper — serialize a List of Strings as a RESP array
    // Used for MGET and KEYS responses
    // Format: *3\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$3\r\nbaz\r\n
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