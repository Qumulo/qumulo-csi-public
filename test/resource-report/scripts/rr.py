# Copyright (c) 2026 Qumulo, Inc. All rights reserved.
# Use of this software is governed by the Qumulo CSI Driver License
# found in the LICENSE file at the root of this repository.
# SPDX-License-Identifier: LicenseRef-Qumulo-CSI-Driver-License

"""Shared helpers for the resource-report suite: configuration and a read-only Qumulo REST client.

Configuration comes from resource-report.env next to run.sh (override the path with RR_CONFIG); environment variables
of the same name override the file. Tokens are read from the files the configuration names -- raw bearer token or the
JSON login response -- and only ever sent in an Authorization header, never logged or written anywhere.
"""
import json
import os
import re
import ssl
import urllib.error
import urllib.request
from pathlib import Path

SUITE = Path(__file__).resolve().parents[1]          # test/resource-report/
REPO = SUITE.parents[1]                               # repository root
PLACEHOLDER = re.compile(r"<[^<>]*>")


def config_path():
    return Path(os.environ.get("RR_CONFIG", SUITE / "resource-report.env"))


def config():
    """KEY=value pairs; inline `# comments` stripped; <placeholders> count as unset."""
    out = {}
    p = config_path()
    if not p.exists():
        raise SystemExit(f"{p} not found: cp resource-report.env.example resource-report.env and fill it in")
    for raw in p.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        v = re.sub(r"\s+#.*$", "", v).strip().strip('"').strip("'")
        if v and not PLACEHOLDER.search(v):
            out[k.strip()] = v
    for k in list(out) + [k for k in os.environ if k.startswith(("ARRAY_", "NODE_", "NAME_PREFIX", "OUT_DIR"))]:
        if os.environ.get(k):
            out[k] = os.environ[k]
    return out


def need(cfg, *keys):
    missing = [k for k in keys if not cfg.get(k)]
    if missing:
        raise SystemExit(f"{config_path()}: set {', '.join(missing)}")
    return [cfg[k] for k in keys]


def resolve(path):
    """Config paths are relative to the suite folder."""
    p = Path(path).expanduser()
    return p if p.is_absolute() else (SUITE / p).resolve()


def read_token(path):
    txt = resolve(path).read_text().strip()
    if txt.startswith("{"):
        return json.loads(txt)["bearer_token"]
    return txt


def array_cfg(cfg, side):
    """Everything the suite needs to know about array A or B."""
    keys = ["SERVER", "API", "TOKEN_FILE", "SHARED_REAL", "SHARED_EXPORT", "DEDICATED_REAL", "DEDICATED_EXPORT"]
    vals = need(cfg, *[f"ARRAY_{side}_{k}" for k in keys])
    a = dict(zip([k.lower() for k in keys], vals))
    a["token_file"] = str(resolve(a["token_file"]))
    return a


class Array:
    """GET-only REST client for one array."""

    def __init__(self, side, cfg=None, timeout=20):
        cfg = cfg or config()
        a = array_cfg(cfg, side)
        self.side, self.api, self.timeout = side, a["api"].rstrip("/"), timeout
        self._token = read_token(a["token_file"])
        self._ctx = ssl._create_unverified_context()          # lab arrays use self-signed certificates

    def get(self, path):
        req = urllib.request.Request(self.api + path, headers={"Authorization": f"Bearer {self._token}"})
        try:
            with urllib.request.urlopen(req, context=self._ctx, timeout=self.timeout) as r:
                body = r.read()
                return json.loads(body) if body else None
        except urllib.error.HTTPError as e:
            return {"_error": e.code, "_path": path, "_body": e.read()[:300].decode(errors="replace")}
        except Exception as e:                                 # timeouts, resets: report, don't crash a collector
            return {"_error": type(e).__name__, "_path": path, "_body": str(e)[:300]}

    def paged(self, path, key="entries"):
        """Follow paging.next until exhausted; returns the concatenated list (or the error document)."""
        items, nxt = [], path
        while nxt:
            doc = self.get(nxt)
            if not isinstance(doc, dict) or "_error" in doc:
                return items if items else doc
            items += doc.get(key, doc.get("quotas", []))
            nxt = (doc.get("paging") or {}).get("next")
        return items
