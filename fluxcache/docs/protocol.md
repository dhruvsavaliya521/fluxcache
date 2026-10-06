# FluxCache Protocol Specification

## Overview
FluxCache uses a hybrid protocol supporting both line-based text commands (for easy debugging via telnet/nc) and length-prefixed binary payloads (for performance and binary safety).

## Framing
Frames can be separated by `\r\n`. 
For bulk data (like `SET`), the command specifies the length of the trailing payload.

## Commands

### `PING`
* **Syntax:** `PING\r\n`
* **Response:** `+PONG\r\n`

### `GET`
* **Syntax:** `GET <key>\r\n`
* **Response (Found):** `$<length>\r\n<data>\r\n`
* **Response (Not Found):** `_\r\n` (Null)

### `SET`
* **Syntax (Inline):** `SET <key> <value> [EX <seconds>]\r\n`
* **Syntax (Binary):** `SET <key> <length>\r\n<data bytes>` (Optional `EX` unsupported in pure binary framing parser currently, handled via `EXPIRE`).
* **Response:** `+OK\r\n`

### `DEL`
* **Syntax:** `DEL <key>\r\n`
* **Response:** `:1\r\n` (deleted) or `:0\r\n` (not found)

### `EXISTS`
* **Syntax:** `EXISTS <key>\r\n`
* **Response:** `:1\r\n` or `:0\r\n`

### `EXPIRE`
* **Syntax:** `EXPIRE <key> <seconds>\r\n`
* **Response:** `:1\r\n` or `:0\r\n`

### `TTL`
* **Syntax:** `TTL <key>\r\n`
* **Response:** `:<seconds>\r\n` (or `-1` if no TTL, `-2` if not found)

### `STATS`
* **Syntax:** `STATS\r\n`
* **Response:** `$<length>\r\n<json data>\r\n`
