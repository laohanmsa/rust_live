"""Run real brokers and compiled tests inside a network-disabled disposable container."""
import datetime
import json
import pathlib
import re
import socket
import subprocess
import sys
import time

OUT = pathlib.Path("/results")


def run(command, name):
    started = time.monotonic()
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=60)
        code, output = result.returncode, result.stdout + result.stderr
    except subprocess.TimeoutExpired as error:
        code = 124
        output = (error.stdout or b"").decode() + (error.stderr or b"").decode()
    (OUT / f"{name}.log").write_text(output)
    evidence = [json.loads(line.partition("EVIDENCE_JSON ")[2])
                for line in output.splitlines() if "EVIDENCE_JSON " in line]
    row = {"name": name, "status": "pass" if code == 0 else "fail",
           "exit_code": code, "seconds": round(time.monotonic() - started, 3),
           "evidence": evidence}
    print(f"{row['status'].upper()} {name}", flush=True)
    return row


def main():
    OUT.mkdir(exist_ok=True)
    report = {"started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
              "isolation": "Docker network none; loopback brokers; no credentials or production mounts",
              "infrastructure": "fail", "regression": [], "acceptance": []}
    processes = []
    logs = []
    try:
        # Linux exposes loopback only when Docker runs with --network none.
        if set(p.name for p in pathlib.Path("/sys/class/net").iterdir()) != {"lo"}:
            raise RuntimeError("This runner requires Docker --network none")
        if not pathlib.Path("/etc/ssl/certs/ca-certificates.crt").is_file():
            raise RuntimeError("System CA certificates are required by the HTTP client builder")
        for name, port, command in [
            ("nats", 4222, ["nats-server", "-a", "127.0.0.1", "-p", "4222"]),
            ("redis", 6379, ["redis-server", "--bind", "127.0.0.1", "--port", "6379",
                             "--save", "", "--appendonly", "no", "--dir", "/tmp"]),
        ]:
            log = (OUT / f"{name}.log").open("w")
            logs.append(log)
            process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
            processes.append(process)
            deadline = time.monotonic() + 10
            while True:
                if process.poll() is not None:
                    raise RuntimeError(f"{name} exited before readiness")
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.2) as client:
                        if name == "nats":
                            assert client.recv(4096).startswith(b"INFO ")
                        else:
                            client.sendall(b"PING\r\n")
                            assert client.recv(1024).startswith(b"+PONG")
                    break
                except OSError:
                    if time.monotonic() > deadline:
                        raise RuntimeError(f"{name} readiness timed out") from None
                    time.sleep(0.05)
        report["infrastructure"] = "pass"
        for binary in sorted(pathlib.Path("/binaries").iterdir()):
            report["regression"].append(run([str(binary), "--test-threads=1"], binary.name))
            listing = subprocess.check_output([str(binary), "--ignored", "--list"], text=True)
            names = re.findall(r"^(uma::replacement_tests::\S+): test$", listing, re.M)
            # Separate processes, one test at a time: no shared-subject cross-talk.
            for name in names:
                report["acceptance"].append(run(
                    [str(binary), name, "--exact", "--ignored", "--nocapture"], name.split("::")[-1]))
        if not report["acceptance"]:
            raise RuntimeError("No replacement gates were discovered")
        if any(p.poll() is not None for p in processes):
            raise RuntimeError("A fixture broker exited during the tests")
    except Exception as error:
        report["infrastructure"] = "fail"
        report["error"] = str(error)
    finally:
        for process in processes:
            process.terminate()
        for process in processes:
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        for log in logs:
            log.close()
        report["finished_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        (OUT / "results.json").write_text(json.dumps(report, indent=2) + "\n")
    if report["infrastructure"] != "pass":
        print(report.get("error", "infrastructure failed"), file=sys.stderr)
        return 2
    return int(any(r["status"] != "pass" for r in report["regression"] + report["acceptance"]))


if __name__ == "__main__":
    sys.exit(main())
