from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Optional

import requests
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class UIAPI:
    """
    Клиент для 3x-ui v2.6.6 (и форков), заточенный под:
    - добавление клиента через panel/inbound/addClient
    - получение "панельной" VLESS-ссылки через запрос inbound/get
    """

    def __init__(self, base_url: str, username: str, password: str, timeout: int = 15):
        """
        base_url: например, http://45.82.254.48:80/secretpanel/panel
        """
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json, text/plain, */*",
            "User-Agent": "Mozilla/5.0 (UIAPI/3.0)",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": base_url.rstrip("/") + "/#/",
        })
        self.timeout = timeout

        base = base_url.rstrip("/")
        if base.endswith("/panel"):
            self.secret_base = base.rsplit("/panel", 1)[0]  # .../secretpanel
            self.panel_base = base                            # .../secretpanel/panel
        else:
            # если вдруг дали .../secretpanel, дособираем /panel
            self.secret_base = base
            self.panel_base = f"{base}/panel"

        # Логин в панели расположен по /secretpanel/login
        self.login_url = f"{self.secret_base}/login"

        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password

        self.log = logging.getLogger("uiapi")
        self._login()

    # -------------------------------
    # Внутренние утилиты
    # -------------------------------

    def _build_url(self, path: str) -> str:
        if path.startswith("http://") or path.startswith("https://"):
            return path
        # Все API-пути здесь относительны к /secretpanel (см. фронтовой код)
        if not path.startswith("/"):
            path = "/" + path
        return f"{self.secret_base}{path}"

    def _request(self, method: str, path: str, **kwargs):
        url = self._build_url(path)
        resp = self.session.request(method, url, timeout=self.timeout, **kwargs)
        resp.raise_for_status()
        # Пытаемся JSON
        try:
            return resp.json()
        except Exception:
            return {"raw": resp.text, "ok": resp.ok}

    def _post(self, path: str, payload: dict):
        return self._request("POST", path, json=payload)

    def _login(self):
        data = {"username": self.username, "password": self.password}
        r = self.session.post(self.login_url, data=data, allow_redirects=True, timeout=self.timeout)
        r.raise_for_status()
        if not self.session.cookies:
            raise RuntimeError("Login failed: no cookies set")
        logger.info("UIAPI login OK")

    def _resolve_server_host(self) -> str:
        # Пытаемся взять хост из base панели (не обязательно публичный)
        try:
            p = urlparse(self.panel_base)
            return p.hostname or "localhost"
        except Exception:
            return "localhost"

    def _safe_json_load(self, obj):
        """Пытаемся распарсить JSON до упора, пока это строка."""
        parsed = obj
        for _ in range(3):  # максимум три уровня вложенности
            if isinstance(parsed, str):
                try:
                    parsed = json.loads(parsed)
                except Exception:
                    break
            else:
                break
        return parsed if isinstance(parsed, dict) else {}

    # -------------------------------
    # API-методы панели
    # -------------------------------

    def get_inbounds_list(self) -> list[dict]:
        """
        POST /secretpanel/panel/inbound/list -> {"success": true, "obj": [...]}
        """
        res = self._post("panel/inbound/list", {})
        if isinstance(res, dict) and res.get("success") is True:
            obj = res.get("obj") or []
            return obj if isinstance(obj, list) else []
        # Фолбэк: многие форки всё равно возвращают success/obj
        return res.get("obj") or []

    def get_inbound(self, inbound_id: int) -> dict:
        """
        Получить полный inbound по id через несколько вариантов эндпоинта.
        """
        # 1) быстрый путь через список
        for ib in self.get_inbounds_list():
            if str(ib.get("id")) == str(inbound_id):
                return ib

        # 2) прямые GET-пути
        variants = [
            ("GET", f"panel/inbound/get/{inbound_id}", None),
            ("GET", "panel/inbound/get", {"id": inbound_id}),
        ]
        for method, path, payload in variants:
            try:
                res = self._request(method, path, json=payload)
            except Exception:
                continue
            if isinstance(res, dict):
                inbound = res.get("obj") or res.get("data") or res.get("inbound") or res
                if isinstance(inbound, dict) and inbound:
                    return inbound
        return {}

    def get_clients_list(self, inbound_id: int) -> list[dict]:
        inbound = self.get_inbound(inbound_id)
        if not inbound:
            return []
        settings_raw = inbound.get("settings")
        try:
            settings = json.loads(settings_raw) if isinstance(settings_raw, str) else (settings_raw or {})
        except Exception:
            settings = {}
        return settings.get("clients") or []

    # -------------------------------
    # Сбор "панельной" VLESS-ссылки
    # -------------------------------

    def try_get_client_vless_link(self, inbound_id: int, email: str, wait_seconds: int = 12) -> Optional[str]:
        """
        Пытается собрать VLESS-ссылку для клиента строго в формате панели.
        Ждём появления клиента max wait_seconds.
        """
        target = (email or "").strip().lower()
        deadline = time.time() + max(1, int(wait_seconds))

        while time.time() < deadline:
            inbound = self.get_inbound(inbound_id)
            if not inbound:
                time.sleep(0.4)
                continue

            # settings → clients
            settings = self._safe_json_load(inbound.get("settings") or {})
            clients = settings.get("clients") or inbound.get("clients") or []
            client = next((c for c in clients if (c.get("email") or "").strip().lower() == target), None)
            if not client:
                time.sleep(0.4)
                continue

            # streamSettings
            ss = self._safe_json_load(inbound.get("streamSettings") or {})

           # realitySettings и tlsSettings
            rs = self._safe_json_load(ss.get("realitySettings") or {})
            tls = self._safe_json_load(ss.get("tlsSettings") or {})

            port = inbound.get("port") or ""
            uuid_val = client.get("id") or client.get("password")
            if not uuid_val:
                return None

            host = self._resolve_server_host()

            # >>> вот здесь вставляешь фикс <<<
            settings_inner = self._safe_json_load(rs.get("settings") or {})
            public_key = rs.get("publicKey") or settings_inner.get("publicKey") or ""

            short_id = ""
            sid_val = rs.get("shortIds") or []
            if isinstance(sid_val, list) and sid_val:
                short_id = sid_val[0]

            sni_list = rs.get("serverNames") or [host]
            sni = sni_list[0] if isinstance(sni_list, list) and sni_list else host
            fp = tls.get("fingerprint") or rs.get("fingerprint") or "chrome"
            flow = (client.get("flow") or "xtls-rprx-vision").strip()

            params = [
                "type=tcp",
                "security=reality",
                f"pbk={public_key}",
                f"fp={fp}",
                f"sni={sni}",
                f"sid={short_id}",
                "spx=%2F",
                f"flow={flow}",
            ]
            remark = (inbound.get("remark") or "").strip()
            tag = email if not remark else f"{remark}-{email}"
            return f"vless://{uuid_val}@{host}:{port}?{'&'.join(params)}#{tag}"

        return None

    def get_client_vless_link(self, inbound_id: int, email: str) -> str:
        link = self.try_get_client_vless_link(inbound_id, email, wait_seconds=12)
        if not link:
            raise LookupError(f"Клиент {email} не найден в inbound {inbound_id}")
        return link

    # -------------------------------
    # Создание клиента (только панель)
    # -------------------------------

    def add_client(
        self,
        inbound_id: int,
        email: str,
        limit_ip: int = 0,
        total_gb: float = 0,
        expiry_time_ms: int = 0,
        flow: Optional[str] = None,
        wait_seconds: int = 12,
    ) -> str:
        """
        Добавляет клиента только через API панели и возвращает ссылку из панели (str).
        Никакой ручной сборки: строго формат панели.
        """
        # 0) Проверим inbound
        inbounds = self.get_inbounds_list()
        if not any(str(ib.get("id")) == str(inbound_id) for ib in inbounds):
            raise RuntimeError(f"Inbound {inbound_id} не найден. Доступны: {[ib.get('id') for ib in inbounds]}")

        client_uuid = str(uuid.uuid4())
        client_obj = {
            "id": client_uuid,
            "email": email,
            "limitIp": int(limit_ip) if limit_ip else 0,
            "totalGB": int(total_gb * 1024 * 1024 * 1024) if total_gb else 0,
            "expiryTime": int(expiry_time_ms) if expiry_time_ms else 0,
        }
        if flow:
            client_obj["flow"] = flow

        # API панели: settings — это строка JSON
        payload = {
            "id": inbound_id,
            "settings": json.dumps({"clients": [client_obj]}, separators=(",", ":")),
        }

        # Попробуем несколько вариантов путей, чтобы покрыть форки
        paths = [
            "panel/inbound/addClient",
            f"panel/inbound/addClient/{inbound_id}",  # некоторые форки принимают id в URL
            "xui/inbound/addClient",
            "inbound/addClient",
        ]
        last_res = {}
        ok = False
        for p in paths:
            try:
                # Если id в пути — отправим только settings
                body = payload if "{inbound_id}" not in p and not p.endswith(f"/{inbound_id}") else {"settings": payload["settings"]}
                res = self._post(p, body)
                last_res = res
                if isinstance(res, dict) and res.get("success") is True:
                    ok = True
                    break
            except Exception as e:
                last_res = {"success": False, "msg": str(e)}

        if not ok:
            raise RuntimeError(f"addClient failed: {last_res}")

        # Ждём и берём ссылку из панели
        link = self.get_client_vless_link(inbound_id, email)
        return link

    def add_traffic(self, inbound_id: int, email: str, add_gb: int):
        inbound = self.get_inbound(inbound_id)
        if not inbound:
            raise RuntimeError(f"Inbound {inbound_id} не найден")

        settings = json.loads(inbound["settings"]) if isinstance(inbound["settings"], str) else dict(inbound["settings"])
        clients = settings.get("clients", [])
        BYTES_IN_GB = 1024 * 1024 * 1024

        found = False
        for c in clients:
            if c.get("email") == email:
                current_limit = int(c.get("totalGB") or 0)
                if current_limit == 0:
                    raise RuntimeError("У клиента безлимитный тариф")
                c["totalGB"] = current_limit + (int(add_gb) * BYTES_IN_GB)
                found = True
                break
        if not found:
            raise RuntimeError(f"Клиент {email} не найден")

        settings["clients"] = clients
        inbound["settings"] = json.dumps(settings, separators=(",", ":"))

        payload = {
            "id": inbound_id,
            "remark": inbound.get("remark", ""),
            "port": inbound.get("port"),
            "protocol": inbound.get("protocol"),
            "settings": inbound["settings"],
            "streamSettings": inbound.get("streamSettings"),
            "sniffing": inbound.get("sniffing"),
            "enable": inbound.get("enable", True),
            "tag": inbound.get("tag"),
        }

        res = self._post(f"panel/inbound/update/{inbound_id}", payload)
        if not res.get("success"):
            raise RuntimeError(f"Не удалось обновить клиента: {res}")

        # Верификация
        updated_client = next((c for c in self.get_clients_list(inbound_id) if c.get("email") == email), None)
        if updated_client:
            self.log.info(f"Теперь totalGB={updated_client.get('totalGB')} (в байтах)")
        return True
    
    def update_client(
        self,
        inbound_id: int,
        email: str,
        total_gb: int,
        expiry_time_ms: Optional[int] = None
    ) -> bool:
        """
        Обновляет клиента по email:
        - total_gb   — новый общий лимит в ГБ (0 для безлимита)
        - expiry_time_ms — новое время окончания в мс (None чтобы не менять)
        """
        inbound = self.get_inbound(inbound_id)
        if not inbound:
            raise RuntimeError(f"Inbound {inbound_id} не найден")

        # распаковываем settings -> clients
        settings = (
            json.loads(inbound["settings"])
            if isinstance(inbound["settings"], str)
            else dict(inbound["settings"])
        )
        clients = settings.get("clients", [])

        found = False
        for c in clients:
            if c.get("email") == email:
                # обновляем лимит
                c["totalGB"] = int(total_gb) * 1024**3
                # обновляем expiryTime, если передан
                if expiry_time_ms is not None:
                    c["expiryTime"] = int(expiry_time_ms)
                found = True
                break
        if not found:
            raise RuntimeError(f"Клиент {email} не найден в clients")

        # запаковываем обратно settings
        settings["clients"] = clients
        inbound["settings"] = json.dumps(settings, separators=(",", ":"))

        # формируем полный payload inbound
        payload = {
            "id": inbound_id,
            "remark": inbound.get("remark", ""),
            "port": inbound.get("port"),
            "protocol": inbound.get("protocol"),
            "settings": inbound["settings"],
            "streamSettings": inbound.get("streamSettings"),
            "sniffing": inbound.get("sniffing"),
            "enable": inbound.get("enable", True),
            "tag": inbound.get("tag"),
        }

        res = self._post(f"panel/inbound/update/{inbound_id}", payload)
        if not (isinstance(res, dict) and res.get("success") is True):
            raise RuntimeError(f"updateClient failed: {res}")
        return True
