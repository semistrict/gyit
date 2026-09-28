#!/usr/bin/env python3
"""Acquire a complete snapshot listing through GitHub's Git trees REST API.

Measures how many API requests one snapshot costs and how it affects the rate
limit. Recursive responses are truncated beyond GitHub's entry/byte limits, so a
truncated tree is split into its children. A complete subtree inside a truncated
response is kept when its reconstructed Git tree objects hash to the expected
IDs. The final root hash proves the listing is complete. Blob sizes are
reported by GitHub and cannot be verified without the blobs.

GH_TOKEN or GITHUB_TOKEN authenticates requests unless --anonymous is given.
/rate_limit is queried before and after; GitHub does not count those calls.
"""
import argparse
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

API = "https://api.github.com"


class RateLimited(Exception):
    pass


def tree_id(entries):
    """SHA-1 Git tree ID of direct entries, each with path (a name), mode, type and sha."""
    ordered = sorted(entries, key=lambda e: e["path"].encode() + (b"/" if e["type"] == "tree" else b""))
    body = b"".join(e["mode"].lstrip("0").encode() + b" " + e["path"].encode() + b"\0" + bytes.fromhex(e["sha"]) for e in ordered)
    return hashlib.sha1(b"tree %d\0" % len(body) + body).hexdigest()


def group(entries):
    """Direct entries, renamed to their base names, keyed by parent path ("" is the root)."""
    children = {"": []}
    for e in entries:
        parent, _, name = e["path"].rpartition("/")
        children.setdefault(parent, []).append({**e, "path": name})
    return children


def complete_subtrees(root_sha, entries):
    """Relative directory paths ("" is the root) whose entire subtree is present in entries."""
    children = group(entries)
    ids = {"": root_sha} | {e["path"]: e["sha"] for e in entries if e["type"] == "tree"}
    complete = set()
    for path in sorted(ids, key=lambda p: p.count("/") + 1 if p else 0, reverse=True):
        direct = children.get(path, [])
        prefix = path + "/" if path else ""
        if tree_id(direct) == ids[path] and all(prefix + e["path"] in complete for e in direct if e["type"] == "tree"):
            complete.add(path)
    return complete


def acquire(fetch, root_sha, concurrency):
    """Return {path: entry} for the whole snapshot. fetch(sha, recursive) returns a trees API response."""
    entries = {}
    waiting = {}
    running = {}

    def add(prefix, items):
        for e in items:
            path = prefix + e["path"]
            entries[path] = {k: v for k, v in e.items() if k != "url"} | {"path": path}

    def expand(prefix, direct, partial, complete):
        for e in direct:
            add(prefix, [e])
            if e["type"] != "tree":
                continue
            if e["path"] in complete:
                add(prefix, [p for p in partial if p["path"].startswith(e["path"] + "/")])
            else:
                running[pool.submit(fetch, e["sha"], True)] = (e["sha"], prefix + e["path"] + "/", True)

    with ThreadPoolExecutor(concurrency) as pool:
        running[pool.submit(fetch, root_sha, True)] = (root_sha, "", True)
        try:
            while running:
                done, _ = wait(running, return_when=FIRST_COMPLETED)
                for future in done:
                    sha, prefix, recursive = running.pop(future)
                    data = future.result()
                    if not recursive:
                        if data["truncated"]:
                            raise RuntimeError(f"direct listing of {prefix or '/'} is truncated")
                        expand(prefix, data["tree"], *waiting.pop(prefix))
                        continue
                    if not data["truncated"]:
                        add(prefix, data["tree"])
                        continue
                    partial = data["tree"]
                    complete = complete_subtrees(sha, partial)
                    direct = group(partial)[""]
                    if tree_id(direct) == sha:
                        expand(prefix, direct, partial, complete)
                    else:
                        waiting[prefix] = (partial, complete)
                        running[pool.submit(fetch, sha, False)] = (sha, prefix, False)
        except BaseException:
            for future in running:
                future.cancel()
            raise
    if "" not in complete_subtrees(root_sha, list(entries.values())):
        raise ValueError(f"acquired listing does not hash to root tree {root_sha}")
    return entries


class GitHub:
    def __init__(self, repository, token, max_requests):
        self.repository = repository
        self.token = token
        self.max_requests = max_requests
        self.lock = threading.Lock()
        self.requests = 0
        self.response_bytes = 0

    def get(self, path, counted=True):
        if counted:
            with self.lock:
                if self.requests >= self.max_requests:
                    raise RuntimeError(f"request budget of {self.max_requests} exhausted")
                self.requests += 1
        headers = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "gyit-tree-probe"}
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        started = time.perf_counter()
        try:
            with urllib.request.urlopen(urllib.request.Request(API + path, headers=headers), timeout=300) as response:
                status, reply, body = response.status, response.headers, response.read()
        except urllib.error.HTTPError as error:
            status, reply, body = error.code, error.headers, error.read()
        elapsed = time.perf_counter() - started
        if counted:
            with self.lock:
                self.response_bytes += len(body)
        if status in (403, 429) and (reply.get("x-ratelimit-remaining") == "0" or reply.get("retry-after") or b"rate limit" in body.lower()):
            raise RateLimited(f"GET {path}: HTTP {status}, remaining={reply.get('x-ratelimit-remaining')}, retry-after={reply.get('retry-after')}, reset={reply.get('x-ratelimit-reset')}: {body[:300]!r}")
        if status != 200:
            raise RuntimeError(f"GET {path}: HTTP {status}: {body[:300]!r}")
        return json.loads(body), reply, elapsed, len(body)

    def tree(self, sha, recursive):
        data, reply, elapsed, size = self.get(f"/repos/{self.repository}/git/trees/{sha}" + ("?recursive=1" if recursive else ""))
        kind = "recursive" if recursive else "direct"
        print(f"{kind:9} {sha[:12]} {size:>10} B {len(data['tree']):>7} entries truncated={data['truncated']!s:5} remaining={reply.get('x-ratelimit-remaining')} {elapsed:.2f}s", file=sys.stderr)
        return data

    def root_tree(self, commit):
        return self.get(f"/repos/{self.repository}/git/commits/{commit}")[0]["tree"]["sha"]

    def core_limit(self):
        return self.get("/rate_limit", counted=False)[0]["resources"]["core"]


def resolve(repository, revision):
    """Resolve a branch, tag or HEAD to a commit with the Git protocol, which uses no API quota."""
    if len(revision) == 40 and all(c in "0123456789abcdef" for c in revision):
        return revision
    out = subprocess.run(["git", "ls-remote", f"https://github.com/{repository}.git", revision], check=True, capture_output=True, text=True).stdout
    refs = {ref: sha for sha, ref in (line.split("\t") for line in out.splitlines())}
    matches = {refs[r] for r in (revision, f"refs/heads/{revision}") if r in refs}
    tag = refs.get(f"refs/tags/{revision}^{{}}") or refs.get(f"refs/tags/{revision}")
    if tag:
        matches.add(tag)
    if len(matches) != 1:
        raise SystemExit(f"cannot resolve {revision!r} to one commit: {sorted(matches)}")
    return matches.pop()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("repository", help="owner/repository")
    parser.add_argument("--revision", default="HEAD", help="branch, tag, HEAD or full commit ID")
    parser.add_argument("--anonymous", action="store_true", help="ignore GH_TOKEN/GITHUB_TOKEN")
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--max-requests", type=int, default=100, help="abort before exceeding this many counted requests")
    parser.add_argument("--output", type=Path, help="also write the JSON summary here")
    args = parser.parse_args()

    token = None if args.anonymous else os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    github = GitHub(args.repository, token, args.max_requests)
    commit = resolve(args.repository, args.revision)
    summary = {"repository": args.repository, "revision": args.revision, "commit": commit, "authenticated": token is not None, "rate_limit_before": github.core_limit()}
    started = time.perf_counter()
    status = 0
    try:
        summary["tree"] = github.root_tree(commit)
        entries = acquire(github.tree, summary["tree"], args.concurrency)
        kinds = [e["type"] for e in entries.values()]
        summary |= {
            "verified": True,
            "entries": len(entries),
            "trees": kinds.count("tree"),
            "blobs": kinds.count("blob"),
            "submodules": kinds.count("commit"),
            "blobs_without_size": sum(e["type"] == "blob" and "size" not in e for e in entries.values()),
        }
    except (RateLimited, RuntimeError, ValueError) as error:
        summary |= {"verified": False, "error": str(error)}
        status = 2
    summary |= {"requests": github.requests, "response_bytes": github.response_bytes, "seconds": round(time.perf_counter() - started, 3), "rate_limit_after": github.core_limit()}
    text = json.dumps(summary, indent=2)
    print(text)
    if args.output:
        args.output.write_text(text + "\n")
    return status


if __name__ == "__main__":
    sys.exit(main())
