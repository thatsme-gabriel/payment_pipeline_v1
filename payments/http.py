"""REST com TLS, timeout e retries somente em operacoes de leitura/idempotentes."""

from __future__ import annotations

import base64
import json
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen


class ApiError(RuntimeError):
    def __init__(self, status: int, method: str, path: str):
        self.status = status
        # Respostas de APIs podem conter a configuracao do conector e senhas: nao imprimir.
        super().__init__(f"HTTP {status}: {method} {path}. Confira permissoes e painel do servico.")


class Rest:
    def __init__(self, base: str, key: str, secret: str):
        if not base.startswith("https://"):
            raise ValueError("API exige HTTPS")
        self.base = base.rstrip("/")
        self.authorization = "Basic " + base64.b64encode(f"{key}:{secret}".encode()).decode()

    def request(self, method: str, path: str, body=None):
        payload = None if body is None else json.dumps(body).encode()
        request = Request(
            self.base + path,
            data=payload,
            method=method,
            headers={
                "Authorization": self.authorization,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        for attempt in range(5):
            try:
                with urlopen(request, timeout=30) as response:
                    data = response.read()
                    return json.loads(data) if data else {}
            except HTTPError as exc:
                retry = method in ("GET", "PUT", "DELETE") and exc.code in (429, 502, 503, 504)
                if not retry or attempt == 4:
                    raise ApiError(exc.code, method, path) from None
                time.sleep(min(2**attempt, 16))
        raise RuntimeError("API sem resposta")

    def pages(self, path: str) -> list:
        from urllib.parse import urlsplit

        items = []
        while path:
            page = self.request("GET", path)
            items.extend(page.get("data", []))
            next_url = page.get("metadata", {}).get("next")
            if next_url:
                parsed = urlsplit(next_url)
                if parsed.netloc and parsed.netloc != urlsplit(self.base).netloc:
                    raise ValueError("Paginacao apontou para outro dominio")
                path = parsed.path + ("?" + parsed.query if parsed.query else "")
            else:
                path = ""
        return items
