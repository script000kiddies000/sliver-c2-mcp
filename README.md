# Sliver C2 MCP Server

A [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server for interacting with the [Sliver C2](https://github.com/BishopFox/sliver) adversary emulation framework.

This server enables AI assistants (Claude Code, Codex, etc.) to manage Sliver C2 infrastructure — sessions, beacons, listeners, loot, websites, pivots, and implant interactions — for authorized red team operations.

**104 tools** covering the full Sliver C2 API surface.

## Quick Start

```bash
# 1. Clone
git clone https://github.com/script000kiddies000/sliver-c2-mcp.git
cd sliver-c2-mcp

# 2. Install & build
npm install
npm run build

# 3. Generate operator config (on Sliver server console)
sliver > new-operator --name mcp-operator --lhost <server-ip>
# Copy the .cfg file to your machine

# 4. Configure your MCP client (see below)
```

## MCP Client Configuration

### Claude Code

Add to `~/.claude/user-mcps.json`:

```json
{
  "mcpServers": {
    "sliver-c2": {
      "command": "node",
      "args": [
        "/absolute/path/to/sliver-c2-mcp/dist/index.js",
        "--operator-config",
        "/absolute/path/to/operator.cfg"
      ]
    }
  }
}
```

Then restart Claude Code. Tools appear as `mcp__sliver-c2__sliver_*`.

### OpenAI Codex / Other MCP Clients

Same pattern — point the MCP client to the built `dist/index.js` with `--operator-config`:

```bash
node dist/index.js --operator-config /path/to/operator.cfg
```

The server communicates via **stdio** (stdin/stdout JSON-RPC), compatible with any MCP client.

### Environment Variables (alternative to CLI flags)

| Variable | Description | Default |
|----------|-------------|---------|
| `SLIVER_OPERATOR_CONFIG` | Path to operator `.cfg` file | (required) |
| `PYTHON_PATH` | Python interpreter path | `python3` |
| `SLIVER_TIMEOUT` | Command timeout in ms | `180000` |

## Prerequisites

1. **Sliver C2 Server** v1.7.x+ running and accessible
2. **Operator config** — generated via `new-operator` in Sliver console
3. **Python 3.10+** with `sliver-py`:
   ```bash
   pip install sliver-py
   ```
4. **Node.js 20+**

## Available Tools (104)

### Server Management (3)
| Tool | Description |
|------|-------------|
| `sliver_version` | Server version |
| `sliver_operators` | Connected operators |
| `sliver_canaries` | DNS canaries |

### Listeners & Jobs (8)
| Tool | Description |
|------|-------------|
| `sliver_listeners` | List active listeners/jobs |
| `sliver_start_mtls` | Start mTLS listener |
| `sliver_start_https` | Start HTTPS listener |
| `sliver_start_http` | Start HTTP listener |
| `sliver_start_dns` | Start DNS listener |
| `sliver_start_wg` | Start WireGuard listener |
| `sliver_kill_listener` | Stop a listener by job ID |
| `sliver_wg_config` | Generate WireGuard client config |

### Stage Listeners (3)
| Tool | Description |
|------|-------------|
| `sliver_start_http_stager` | Start HTTP stager listener |
| `sliver_start_https_stager` | Start HTTPS stager listener |
| `sliver_start_tcp_stager` | Start TCP stager listener |

### Implant Management (5)
| Tool | Description |
|------|-------------|
| `sliver_implant_builds` | List implant builds |
| `sliver_implant_profiles` | List generation profiles |
| `sliver_generate` | Generate a new implant |
| `sliver_regenerate` | Regenerate implant by name |
| `sliver_delete_build` | Delete an implant build |

### Sessions — Real-time Implants (22)
| Tool | Description |
|------|-------------|
| `sliver_sessions` | List active sessions |
| `sliver_kill_session` | Terminate a session |
| `sliver_rename_session` | Rename a session |
| `sliver_ls` | List files on target |
| `sliver_pwd` | Current working directory |
| `sliver_cd` | Change directory |
| `sliver_mkdir` | Create directory |
| `sliver_rm` | Remove file/directory |
| `sliver_mv` | Move/rename file |
| `sliver_download` | Download file from target |
| `sliver_upload` | Upload file to target |
| `sliver_execute` | Execute a program |
| `sliver_execute_assembly` | Execute .NET assembly in-memory |
| `sliver_execute_shellcode` | Inject and run shellcode |
| `sliver_sideload` | Sideload a shared library (DLL/SO) |
| `sliver_shell` | Interactive shell command |
| `sliver_ps` | List processes |
| `sliver_terminate` | Kill a process by PID |
| `sliver_process_dump` | Dump process memory |
| `sliver_ifconfig` | Network interfaces |
| `sliver_netstat` | Network connections |
| `sliver_screenshot` | Capture screenshot |
| `sliver_get_env` | List environment variables |
| `sliver_set_env` | Set environment variable |
| `sliver_unset_env` | Unset environment variable |
| `sliver_get_system` | Attempt SYSTEM privilege escalation |
| `sliver_impersonate` | Impersonate a user token |
| `sliver_make_token` | Create a token with credentials |
| `sliver_revert_to_self` | Revert to original token |
| `sliver_registry_read` | Read Windows registry |
| `sliver_registry_write` | Write Windows registry |
| `sliver_start_service` | Start a Windows service |
| `sliver_stop_service` | Stop a Windows service |

### Beacons — Async Callback Implants (24)

All beacon commands are async — they queue a task and poll for results.

| Tool | Description |
|------|-------------|
| `sliver_beacons` | List all beacons with status |
| `sliver_rename_beacon` | Rename a beacon |
| `sliver_prune_beacons` | Remove DEAD beacons |
| `sliver_beacon_tasks` | List pending tasks |
| `sliver_beacon_open_session` | Open real-time session from beacon |
| `sliver_beacon_ls` | List files |
| `sliver_beacon_pwd` | Current directory |
| `sliver_beacon_cd` | Change directory |
| `sliver_beacon_mkdir` | Create directory |
| `sliver_beacon_file_rm` | Remove file |
| `sliver_beacon_rm` | Remove file/directory |
| `sliver_beacon_mv` | Move/rename file |
| `sliver_beacon_upload` | Upload file |
| `sliver_beacon_download` | Download file |
| `sliver_beacon_execute` | Execute program |
| `sliver_beacon_execute_assembly` | Execute .NET assembly |
| `sliver_beacon_execute_shellcode` | Inject shellcode |
| `sliver_beacon_execute_bof` | Execute Beacon Object File |
| `sliver_beacon_call_extension` | Call loaded extension |
| `sliver_beacon_sideload` | Sideload shared library |
| `sliver_beacon_ps` | List processes |
| `sliver_beacon_terminate` | Kill process by PID |
| `sliver_beacon_process_dump` | Dump process memory |
| `sliver_beacon_ifconfig` | Network interfaces |
| `sliver_beacon_netstat` | Network connections |
| `sliver_beacon_screenshot` | Capture screenshot |
| `sliver_beacon_get_env` | List environment variables |
| `sliver_beacon_set_env` | Set environment variable |
| `sliver_beacon_unset_env` | Unset environment variable |
| `sliver_beacon_get_system` | Attempt SYSTEM escalation |
| `sliver_beacon_registry_read` | Read Windows registry |
| `sliver_beacon_registry_write` | Write Windows registry |

### Reverse Port Forward (6)
| Tool | Description |
|------|-------------|
| `sliver_rportfwd_list` | List rportfwd on a session |
| `sliver_rportfwd_start` | Start rportfwd (listen on target → forward to attacker) |
| `sliver_rportfwd_stop` | Stop rportfwd |
| `sliver_beacon_rportfwd_list` | List rportfwd on a beacon (async) |
| `sliver_beacon_rportfwd_start` | Start rportfwd on beacon (async) |
| `sliver_beacon_rportfwd_stop` | Stop rportfwd on beacon (async) |

### Pivoting (2)
| Tool | Description |
|------|-------------|
| `sliver_pivot_listeners` | List pivot listeners on session |
| `sliver_pivot_start_tcp` | Start TCP pivot listener |

### Loot (4)
| Tool | Description |
|------|-------------|
| `sliver_loot_list` | List stored loot |
| `sliver_loot_add` | Add credentials or file loot |
| `sliver_loot_rm` | Remove loot by ID |
| `sliver_loot_content` | Get loot content |

### Websites (5)
| Tool | Description |
|------|-------------|
| `sliver_websites_list` | List hosted websites |
| `sliver_website_content` | List content paths for a website |
| `sliver_website_add_content` | Add content to a website |
| `sliver_website_remove_content` | Remove content paths |
| `sliver_website_remove` | Delete entire website |

> **Note:** Websites must be created via Sliver console first: `http --lport <port> --website <name>`

### TaskMany (1)
| Tool | Description |
|------|-------------|
| `sliver_taskmany` | Execute a command on multiple beacons/sessions at once |

### Intelligence (2)
| Tool | Description |
|------|-------------|
| `sliver_creds` | Harvested credentials |
| `sliver_hosts` | Discovered hosts |

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   MCP Client    │────▶│  sliver-c2-mcp   │────▶│  Sliver Server  │
│ (Claude, Codex) │◀────│  (Node.js + TS)  │◀────│    (gRPC)       │
└─────────────────┘     └──────────────────┘     └─────────────────┘
        stdio                    │
                                 ▼
                          ┌──────────────┐
                          │  sliver-py   │
                          │  (Python)    │
                          └──────────────┘
```

Each tool invocation spawns a Python subprocess that:
1. Connects to Sliver server via gRPC (using operator config mTLS certs)
2. Executes the command
3. For beacon commands: queues async task → polls `beacon_task_content()` until result or timeout
4. Returns JSON result via stdout

### Known Limitations

- **SOCKS5 proxy & local port forward** — not possible in this architecture (requires persistent tunnel/stream; MCP spawns a new process per command)
- **Reverse port forward** — works fine (server-side listener on the implant, no persistent client tunnel needed)
- **Loot API** — Sliver v1.7.9 does not return `LootID`/`Name` in gRPC response; workaround uses `File.Name` as identifier
- **`beacon_get_system`** — Sliver v1.7.x does not support `GetSystem` via beacon async mode; use via session instead
- **Websites** — must be created via Sliver console (`http --website <name>`) before adding content via MCP

## Security Considerations

**This tool is intended for authorized security testing only.**

- **Protect operator configs** — they provide full C2 infrastructure access
- **Authorization required** — only use against systems you have explicit written permission to test
- **Audit logging** — all operations are logged by the Sliver server
- **Do not expose** the MCP server to untrusted networks

## Development

```bash
npm install        # install dependencies
npm run build      # compile TS → dist/index.js
npm run dev        # development mode
npm test           # run tests
npm run lint       # lint
```

After editing `src/index.ts`, run `npm run build` then reload your MCP client.

## License

MIT License — See [LICENSE](LICENSE) for details.

## Related Projects

- [Sliver C2](https://github.com/BishopFox/sliver) — Open source adversary emulation framework
- [sliver-py](https://github.com/moloch--/sliver-py) — Python client library for Sliver
- [Model Context Protocol](https://modelcontextprotocol.io/) — Open protocol for AI tool integration

## Contributing

Contributions welcome. Please ensure compatibility with Sliver C2 v1.7.x+ API and follow existing code patterns.
