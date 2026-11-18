"""
Independent drill for the LaminaDB identity/format migration.

Nothing here imports the project. The legacy binary in the first argument is
the *baseline* build (format 1), the second is the current build. The drill
creates real legacy material with the baseline, then exercises upgrade,
rollback, interruption recovery, same-sequence-different-source pairing,
portable restore inheritance and the public serving workflows.

Usage: python3 -I examples/migration_drill.py OLD_BINARY NEW_BINARY [--keep DIR]
"""

import argparse
import hashlib
import json
import pathlib
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

FINDINGS = []


def record(name, ok, detail):
    FINDINGS.append({"check": name, "ok": bool(ok), "detail": detail})
    print(("PASS " if ok else "FAIL ") + name + ": " + str(detail), flush=True)


class Session:
    def __init__(self, binary, directory, restore=None):
        self.binary = str(binary)
        self.dir = pathlib.Path(directory)
        self.restore = restore
        self.process = None
        self.log = None

    def start(self):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        self.url = f"http://127.0.0.1:{port}/v1/lamina"
        args = [self.binary, "-data", str(self.dir), "-listen", f"127.0.0.1:{port}"]
        if self.restore:
            args += ["-restore", str(self.restore)]
            self.restore = None
        self.log = tempfile.TemporaryFile()
        self.process = subprocess.Popen(args, stdout=self.log, stderr=self.log)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                self.log.seek(0)
                raise RuntimeError(self.log.read().decode())
            try:
                self.call("GET", "/health")
                return self
            except (OSError, urllib.error.URLError, RuntimeError):
                time.sleep(0.03)
        raise RuntimeError("service did not become ready")

    def stop(self):
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=12)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        if self.log:
            self.log.close()
            self.log = None

    def call(self, method, path, data=None, status=200, raw=False):
        body = None if data is None else json.dumps(data).encode()
        request = urllib.request.Request(
            self.url + path,
            data=body,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=12) as response:
                code, payload = response.status, response.read()
        except urllib.error.HTTPError as error:
            code, payload = error.code, error.read()
        if code != status:
            raise RuntimeError(f"{method} {path}: expected {status}, got {code}: {payload.decode()}")
        return payload if raw else json.loads(payload)

    def begin(self):
        return self.call("POST", "/transactions", {}, 201)["id"]

    def put(self, tx, row, insert=False):
        return self.call(
            "POST",
            f"/transactions/{tx}/put",
            {"relation": "spectra", "row": row, "insert_only": insert},
        )

    def commit(self, tx):
        return self.call("POST", f"/transactions/{tx}/commit")

    def seed(self, band_prefix="b", scale=10, count=7):
        self.call(
            "POST",
            "/relations",
            {
                "name": "spectra",
                "primary": "sample",
                "columns": [
                    {"name": "sample", "type": "int"},
                    {"name": "band", "type": "text"},
                    {"name": "amplitude", "type": "int", "nullable": True},
                ],
            },
            201,
        )
        tx = self.begin()
        for n in range(1, count + 1):
            self.put(tx, {"sample": n, "band": f"{band_prefix}{n}", "amplitude": n * scale})
        self.commit(tx)


def make_legacy(old_binary, root):
    """Real format-1 material: checkpoint plus a journal suffix."""
    first = Session(old_binary, root / "old-a").start()
    first.seed()
    first.call("POST", "/indexes", {"name": "by_band", "relation": "spectra", "column": "band", "unique": True}, 201)
    checkpoint = first.call("POST", "/checkpoint")["sequence"]
    tx = first.begin()
    first.put(tx, {"sample": 8, "band": "b8", "amplitude": 80})
    first.put(tx, {"sample": 9, "band": "b9", "amplitude": 90})
    first.commit(tx)
    tx = first.begin()
    first.put(tx, {"sample": 2, "band": "b2", "amplitude": 222})
    first.commit(tx)
    head = first.call("GET", "/stats")["sequence"]
    (root / "old-a-backup.lmi").write_bytes(first.call("GET", "/backup", raw=True))
    first.stop()

    second = Session(old_binary, root / "old-b").start()
    second.seed(band_prefix="c", scale=100)
    second.call("POST", "/checkpoint")
    tx = second.begin()
    second.put(tx, {"sample": 8, "band": "c8", "amplitude": 800})
    second.commit(tx)
    second.stop()
    return {"checkpoint": checkpoint, "head": head}


def sha(path):
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()


def cli(binary, *args):
    return subprocess.run([str(binary), *map(str, args)], capture_output=True, text=True, timeout=30)


def classify(binary, directory):
    result = cli(binary, "recover", "-data", directory, "-json")
    if result.returncode != 0:
        return {"state": "error", "error": result.stderr, "serving_ok": False}
    return json.loads(result.stdout)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("old_binary")
    parser.add_argument("new_binary")
    parser.add_argument("--keep", help="keep the working tree at this path")
    args = parser.parse_args()
    old_binary, new_binary = pathlib.Path(args.old_binary), pathlib.Path(args.new_binary)

    root = pathlib.Path(args.keep) if args.keep else pathlib.Path(tempfile.mkdtemp(prefix="lamina-migration-"))
    if root.exists() and args.keep is None:
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)
    print(json.dumps({"workdir": str(root), "old_binary": str(old_binary), "new_binary": str(new_binary)}))
    try:
        material = make_legacy(old_binary, root / "material")
        original = {
            "checkpoint": sha(root / "material/old-a/checkpoint.lmi"),
            "journal": sha(root / "material/old-a/journal.lmw"),
        }

        # 1. The serving command refuses identity-less material.
        refused = cli(new_binary, "-data", root / "material/old-a", "-listen", "127.0.0.1:0")
        record("serving refuses legacy material", refused.returncode != 0 and "upgrade" in refused.stderr, refused.stderr.strip().splitlines()[-1])

        # 2. Diagnosis states the readable boundary without mutating bytes.
        diagnosis = classify(new_binary, root / "material/old-a")
        record(
            "recover reports readable legacy history",
            diagnosis["state"] == "legacy" and diagnosis["readable_sequence"] == material["head"] and diagnosis["readable_keys"] == 9,
            {k: diagnosis[k] for k in ("state", "readable_sequence", "readable_keys", "readable_versions")},
        )
        record("recover is read-only", sha(root / "material/old-a/journal.lmw") == original["journal"], original["journal"][:16])

        # 3. Same sequence numbers, different sources: pairing is refused.
        mixed = root / "mixed"
        mixed.mkdir()
        shutil.copy(root / "material/old-a/checkpoint.lmi", mixed / "checkpoint.lmi")
        shutil.copy(root / "material/old-b/journal.lmw", mixed / "journal.lmw")
        mismatch = cli(new_binary, "upgrade", "-data", mixed, "-json")
        record(
            "same-sequence different-source pair refused",
            mismatch.returncode != 0 and "disagree" in mismatch.stderr,
            mismatch.stderr.strip().splitlines()[-1],
        )
        record(
            "refusal leaves data files untouched",
            sorted(p.name for p in mixed.iterdir() if p.name != "lamina.lock") == ["checkpoint.lmi", "journal.lmw"]
            and sha(mixed / "journal.lmw") == sha(root / "material/old-b/journal.lmw"),
            sorted(p.name for p in mixed.iterdir()),
        )

        # 4. In-place upgrade with rollback material.
        work = root / "work"
        shutil.copytree(root / "material/old-a", work)
        upgraded = cli(new_binary, "upgrade", "-data", work, "-json")
        report = json.loads(upgraded.stdout) if upgraded.returncode == 0 else {}
        record("upgrade succeeds", upgraded.returncode == 0, report.get("status"))
        record(
            "upgrade states what it adopted",
            report.get("legacy_journal", {}).get("legacy_sha256") == original["journal"]
            and report.get("legacy_journal", {}).get("legacy_records") == 5
            and report.get("readable_formats") == "2..2"
            and report.get("rollback", {}).get("executable") is True,
            {k: report.get("legacy_journal", {}).get(k) for k in ("legacy_sha256", "legacy_records")},
        )
        record("upgrade verifies checkpoint against journal replay", report.get("legacy_cross_check", {}).get("result") == "verified", report.get("legacy_cross_check"))
        record("upgraded directory keeps v1 prefix byte-identical", sha(work / "journal.lmw") == original["journal"], sha(work / "journal.lmw")[:16])
        record("upgraded directory keeps v1 checkpoint byte-identical", sha(work / "checkpoint.v1.lmi") == original["checkpoint"], sha(work / "checkpoint.v1.lmi")[:16])
        record("upgrade does not duplicate the image", (work / "checkpoint.v1.lmi").stat().st_size == (root / "material/old-a/checkpoint.lmi").stat().st_size, (work / "checkpoint.lmi").stat().st_size)

        # 5. Normal startup, checkpoint replacement and restart on upgraded data.
        session = Session(new_binary, work).start()
        try:
            identity = session.call("GET", "/identity")
            rows = session.call("POST", "/query", {"relation": "spectra"})
            historical = session.call("POST", "/query", {"relation": "spectra", "at": material["checkpoint"], "where": [{"column": "sample", "op": "=", "value": 2}]})
            record(
                "upgraded directory serves inherited rows",
                rows["matched"] == 9 and any(r["sample"] == 9 for r in rows["rows"]),
                rows["matched"],
            )
            record(
                "upgraded directory serves inherited history",
                historical["rows"] == [{"sample": 2, "band": "b2", "amplitude": 20}],
                historical["rows"],
            )
            record(
                "identity is self-describing",
                identity["provenance"]["lineage"].startswith("legacy-v1-to-v2:")
                and identity["adopted_legacy"] is True
                and identity["readable_formats"] == "2..2",
                identity["provenance"],
            )
            backup_bytes = session.call("GET", "/backup", raw=True)
            (root / "upgraded-backup.lmi").write_bytes(backup_bytes)
            session.call("POST", "/checkpoint")
        finally:
            session.stop()
        Session(new_binary, work).start().stop()
        after = Session(new_binary, work).start()
        try:
            record("checkpoint replacement keeps upgraded data", after.call("POST", "/query", {"relation": "spectra"})["matched"] == 9, 9)
        finally:
            after.stop()

        # 6. Portable restore inherits, and its commits are its own.
        restored = Session(new_binary, root / "restored", restore=root / "upgraded-backup.lmi").start()
        tx = restored.begin()
        restored.put(tx, {"sample": 20, "band": "r20", "amplitude": 200})
        own = restored.commit(tx)
        inherited = restored.call("GET", "/identity")
        changes = restored.call("GET", "/changes?after=0", status=410)
        try:
            restored.call("GET", "/changes?after=0")
        except RuntimeError as error:
            changes = str(error)
        restored.stop()
        source_identity = identity["provenance"]
        record(
            "restore states inherited origin and parent",
            inherited["provenance"].get("parent") == source_identity["instance"]
            and inherited["provenance"]["lineage"] == source_identity["lineage"]
            and inherited["provenance"]["instance"] != source_identity["instance"],
            inherited["provenance"],
        )
        record("restore keeps the source's change history out of the new head", "410" in changes, changes)
        record("restored instance commits continue at its own sequence", own["sequence"] == material["head"] + 1, own)
        reopened = Session(new_binary, root / "restored").start()
        try:
            record("restored rows survive restart", reopened.call("POST", "/query", {"relation": "spectra"})["matched"] == 10, 10)
        finally:
            reopened.stop()
        source_again = Session(new_binary, work).start()
        try:
            record(
                "source does not inherit restored commits",
                source_again.call("POST", "/query", {"relation": "spectra"})["matched"] == 9,
                9,
            )
        finally:
            source_again.stop()

        # 7. Interrupted upgrade phases reopen and continue under one identity.
        #    Phase A: legacy image moved aside, nothing new written.
        #    Phase B: v2 checkpoint written, adoption record not yet installed.
        #    Phase C: adoption installed but the v2 checkpoint is missing.
        for phase in ("phase-checkpoint-moved", "phase-no-adoption", "phase-no-checkpoint"):
            broken = root / phase
            shutil.copytree(root / "material/old-a", broken)
            original_head = sha(broken / "journal.lmw")
            if phase == "phase-checkpoint-moved":
                (broken / "checkpoint.lmi").rename(broken / "checkpoint.v1.lmi")
            elif phase == "phase-no-adoption":
                staging = root / ("staging-" + phase)
                shutil.copytree(root / "material/old-a", staging)
                cli(new_binary, "upgrade", "-data", staging, "-json")
                shutil.copy(staging / "checkpoint.lmi", broken / "checkpoint.lmi")
                (broken / "checkpoint.lmi").rename(broken / "checkpoint.v2.tmp")
                (broken / "checkpoint.v1.lmi").write_bytes((staging / "checkpoint.v1.lmi").read_bytes())
                (broken / "checkpoint.v2.tmp").rename(broken / "checkpoint.lmi")
                shutil.rmtree(staging)
            else:
                staging = root / ("staging-" + phase)
                shutil.copytree(root / "material/old-a", staging)
                cli(new_binary, "upgrade", "-data", staging, "-json")
                shutil.copy(staging / "journal.lmw.id", broken / "journal.lmw.id")
                (broken / "checkpoint.lmi").rename(broken / "checkpoint.v1.lmi")
                shutil.copy(staging / "checkpoint.lmi", broken / "checkpoint.lmi")
                (broken / "checkpoint.lmi").unlink()
                shutil.rmtree(staging)
            diagnosis = classify(new_binary, broken)
            refused = cli(new_binary, "-data", broken, "-listen", "127.0.0.1:0")
            record(f"{phase}: not servable while incomplete", refused.returncode != 0, refused.stderr.strip().splitlines()[-1])
            repaired = cli(new_binary, "upgrade", "-data", broken, "-json")
            if phase == "phase-no-adoption":
                record(
                    f"{phase}: two identities are never guessed",
                    repaired.returncode != 0,
                    repaired.stderr.strip().splitlines()[-1],
                )
                continue
            resumed = json.loads(repaired.stdout) if repaired.returncode == 0 else {}
            record(f"{phase}: reopen completes or reverts deterministically", repaired.returncode == 0, resumed.get("status"))
            record(f"{phase}: legacy journal bytes unchanged", sha(broken / "journal.lmw").startswith(original_head[:16]), sha(broken / "journal.lmw")[:16])
            check = Session(new_binary, broken).start()
            try:
                record(f"{phase}: rows readable after repair", check.call("POST", "/query", {"relation": "spectra"})["matched"] == 9, 9)
            finally:
                check.stop()

        # 8. Rollback restores byte-identical legacy material.
        victim = root / "rollback"
        shutil.copytree(root / "material/old-a", victim)
        cli(new_binary, "upgrade", "-data", victim, "-json")
        rolled = cli(new_binary, "rollback", "-data", victim, "-json")
        record("rollback succeeds", rolled.returncode == 0, json.loads(rolled.stdout).get("status") if rolled.returncode == 0 else rolled.stderr.strip().splitlines()[-1])
        record("rollback restores byte-identical journal", sha(victim / "journal.lmw") == original["journal"], sha(victim / "journal.lmw")[:16])
        record("rollback restores byte-identical checkpoint", sha(victim / "checkpoint.lmi") == original["checkpoint"], sha(victim / "checkpoint.lmi")[:16])
        legacy_check = Session(old_binary, victim).start()
        try:
            record("legacy binary still reads rolled-back directory", legacy_check.call("POST", "/query", {"relation": "spectra"})["matched"] == 9, 9)
        finally:
            legacy_check.stop()

        # 9. Torn legacy tail is reported before any conversion.
        torn = root / "torn"
        shutil.copytree(root / "material/old-a", torn)
        with (torn / "journal.lmw").open("ab") as handle:
            handle.write(b"\x00\x00\x00")
        report_before = cli(new_binary, "upgrade", "-data", torn, "-json")
        diagnosis = classify(new_binary, torn)
        record("torn tail is reported", diagnosis["recovered_tail_bytes"] == 3 and diagnosis["readable_keys"] == 9, diagnosis["recovered_tail_bytes"])
        record("torn tail is not silently discarded", report_before.returncode != 0 and "cut-tail" in report_before.stderr, report_before.stderr.strip().splitlines()[-1])
        report_after = cli(new_binary, "upgrade", "-data", torn, "-cut-tail", "-json")
        record("explicit cut-tail converts", report_after.returncode == 0 and json.loads(report_after.stdout)["keys"] == 9, json.loads(report_after.stdout)["recovered_tail_bytes"] if report_after.returncode == 0 else report_after.stderr)

        # 10. Full-frame corruption is rejected, complete interruption leaves no second head.
        corrupt = root / "corrupt"
        shutil.copytree(root / "material/old-a", corrupt)
        data = bytearray((corrupt / "journal.lmw").read_bytes())
        data[-1] ^= 1
        (corrupt / "journal.lmw").write_bytes(data)
        bad = cli(new_binary, "upgrade", "-data", corrupt, "-json")
        record("complete corruption is rejected", bad.returncode != 0 and "checksum" in bad.stderr, bad.stderr.strip().splitlines()[-1])
        record("rejected upgrade leaves no adoption record", not (corrupt / "journal.lmw.id").exists() and not (corrupt / "checkpoint.v1.lmi").exists(), sorted(p.name for p in corrupt.iterdir()))

        # 11. Identity swap of whole v2 artifacts is rejected.
        left, right = root / "left", root / "right"
        shutil.copytree(root / "material/old-a", left)
        shutil.copytree(root / "material/old-b", right)
        cli(new_binary, "upgrade", "-data", left, "-json")
        cli(new_binary, "upgrade", "-data", right, "-json")
        shutil.copy(right / "journal.lmw", left / "journal.lmw")
        shutil.copy(right / "journal.lmw.id", left / "journal.lmw.id")
        swapped = cli(new_binary, "-data", left, "-listen", "127.0.0.1:0")
        record("foreign journal is refused by identity", swapped.returncode != 0 and "belongs to instance" in swapped.stderr, swapped.stderr.strip().splitlines()[-1])

        failed = [finding for finding in FINDINGS if not finding["ok"]]
        print(json.dumps({"checks": len(FINDINGS), "failed": len(failed), "workdir": str(root), "results": FINDINGS}, indent=1))
        return 1 if failed else 0
    finally:
        if args.keep is None:
            shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
