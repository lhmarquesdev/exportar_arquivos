
import os
import io
import time
import uuid
import csv
import json
import html as html_lib
import sqlite3
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse, parse_qs

import requests
from flask import Flask, render_template, request, send_file, jsonify, make_response
from flask_socketio import SocketIO
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

# ================== ENV/TOKENS ==================
load_dotenv()  # lê .env se existir

def load_tokens():
    toks = []
    env_multi = os.getenv("MULTI_TOKENS", "").strip()
    if env_multi:
        toks = [t.strip() for t in env_multi.split(",") if t.strip()]
    elif os.path.exists("tokens.txt"):
        with open("tokens.txt", "r", encoding="utf-8") as f:
            toks = [ln.strip() for ln in f if ln.strip() and not ln.strip().startswith("#")]
    else:
        single = os.getenv("SINGLE_TOKEN") or os.getenv("TOKEN") or ""
        if single.strip():
            toks = [single.strip()]
    return toks

TOKENS = load_tokens()
if not TOKENS:
    print("ℹ️  Nenhum token em .env/arquivo. Vou aceitar tokens via URL, cookie ou header.")

BASE_URL = "https://api.wescctech.com.br/core/v2/api"

def HEADERS(token):
    return {"access-token": token, "Content-Type": "application/json"}

PORT = int(os.getenv("PORT", "5000"))

EXPORT_DIR = "exports"
DB_PATH = "exports.db"
os.makedirs(EXPORT_DIR, exist_ok=True)
DB_DIR = "exports_dbs"
os.makedirs(DB_DIR, exist_ok=True)


app = Flask(__name__, static_folder=None, template_folder="templates")
socketio = SocketIO(app, async_mode="threading")

# ===== utils: tokens helpers =====
def _split_tokens_str(s):
    if not s:
        return []
    return [t.strip() for t in s.replace(";", ",").split(",") if t.strip()]

def _mask(ts):
    return [t[:4] + "..." + t[-4:] if len(t) > 8 else "***" for t in ts]

def get_request_tokens():
    """Prioridade: ?tokens -> headers -> cookie -> referer -> fallback (.env/tokens.txt)"""
    url_tokens_str = (request.args.get("tokens") or request.args.get("token") or "").strip()
    if url_tokens_str:
        ts = _split_tokens_str(url_tokens_str)
        if ts:
            print(f"→ tokens por QUERY: { _mask(ts) }")
            return ts

    for h in ["X-Access-Tokens", "X-Access-Token", "access-token"]:
        if request.headers.get(h):
            ts = _split_tokens_str(request.headers.get(h))
            if ts:
                print(f"→ tokens por HEADER {h}: { _mask(ts) }")
                return ts

    auth = request.headers.get("Authorization", "").strip()
    if auth.lower().startswith("bearer "):
        bearer = auth[7:].strip()
        ts = _split_tokens_str(bearer)
        if ts:
            print(f"→ tokens por Authorization Bearer: { _mask(ts) }")
            return ts

    cookie_tokens = request.cookies.get("tokens")
    if cookie_tokens:
        ts = _split_tokens_str(cookie_tokens)
        if ts:
            print(f"→ tokens por COOKIE: { _mask(ts) }")
            return ts

    ref = request.referrer or request.headers.get("Referer") or ""
    if ref:
        try:
            q = parse_qs(urlparse(ref).query)
            ref_tokens = q.get("tokens") or q.get("token") or []
            if ref_tokens:
                ts = _split_tokens_str(ref_tokens[0])
                if ts:
                    print(f"→ tokens por REFERER: { _mask(ts) }")
                    return ts
        except Exception:
            pass

    if TOKENS:
        print(f"→ tokens por FALLBACK (env/arquivo): { _mask(TOKENS) }")
    else:
        print("→ sem tokens detectados para esta request.")
    return TOKENS

# ============== HTTP helpers (backoff) ==============
def _http_post(token, url, json_body):
    for attempt in range(2):
        resp = requests.post(url, headers=HEADERS(token), json=json_body, timeout=60)
        if resp.status_code != 429:
            return resp
        time.sleep(0.6)
    return resp

def _http_get(token, url):
    for attempt in range(2):
        resp = requests.get(url, headers=HEADERS(token), timeout=60)
        if resp.status_code != 429:
            return resp
        time.sleep(0.6)
    return resp

# -------------- DB (SQLite) --------------
# -------------- DB (SQLite) --------------

DB_DIR = "exports_dbs"
os.makedirs(DB_DIR, exist_ok=True)

from flask import has_request_context

def get_current_db_path():
    token = None
    if has_request_context():
        origin = request.host or "default"
        tokens = get_request_tokens()
        if tokens:
            token = tokens[0][:8]
        safe_origin = origin.replace(":", "_").replace("/", "_")
        name = f"{safe_origin}_{token or 'default'}.db"
    else:
        global LAST_TOKEN_DB
        if 'LAST_TOKEN_DB' in globals() and LAST_TOKEN_DB:
            name = os.path.basename(LAST_TOKEN_DB)
        else:
            name = "default.db"
    return os.path.join(DB_DIR, name)

from flask import has_request_context

LAST_TOKEN_DB = None  # memória global do último banco usado

def get_current_db_path():
    """Seleciona o arquivo .db baseado no token e domínio da requisição."""
    global LAST_TOKEN_DB
    token = None

    if has_request_context():
        origin = request.host or "default"
        tokens = get_request_tokens()
        if tokens:
            token = tokens[0][:8]
        safe_origin = origin.replace(":", "_").replace("/", "_")
        name = f"{safe_origin}_{token or 'default'}.db"
        db_path = os.path.join(DB_DIR, name)
        LAST_TOKEN_DB = db_path  # salva o último banco ativo
        return db_path

    # fora de contexto (thread/socket)
    if LAST_TOKEN_DB:
        return LAST_TOKEN_DB
    return os.path.join(DB_DIR, "default.db")


def db_conn():
    """Abre conexão com o banco correto, inicializando se necessário."""
    try:
        db_path = get_current_db_path()
        global LAST_TOKEN_DB
        LAST_TOKEN_DB = db_path

        first_creation = not os.path.exists(db_path)
        con = sqlite3.connect(db_path)

        cur = con.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='exports';")
        exists = cur.fetchone()

        if first_creation or not exists:
            print(f"⚙️ Inicializando banco para {os.path.basename(db_path)} (novo token/URL)...")
            init_db(db_path)
            con.close()
            con = sqlite3.connect(db_path)

        print(f"→ usando banco: {os.path.basename(db_path)}")
        return con

    except Exception as e:
        print(f"❌ Erro ao conectar ao banco SQLite: {e}")
        raise


def init_db(db_path=None):
    if db_path is None:
        db_path = get_current_db_path()
    with sqlite3.connect(db_path) as con:
        cur = con.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS exports (
            id TEXT PRIMARY KEY,
            filename TEXT,
            format TEXT,
            created_at TEXT,
            status TEXT,
            total INTEGER DEFAULT 0,
            processed INTEGER DEFAULT 0,
            filters TEXT,
            description TEXT,
            error TEXT,
            size_bytes INTEGER DEFAULT 0
        );
        """)
        con.commit()


def db_insert_export(_id, _format, description, filters_json, status="indexing"):
    with db_conn() as con:
        con.execute("""
            INSERT INTO exports (id, filename, format, created_at, status, total, processed, filters, description, error, size_bytes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (_id, None, _format, datetime.utcnow().isoformat(timespec="seconds")+"Z",
              status, 0, 0, filters_json, description, None, 0))
        con.commit()

def db_update_status(_id, *, status=None, total=None, processed=None, filename=None, size_bytes=None, error=None):
    sets, params = [], []
    if status is not None:
        sets.append("status=?"); params.append(status)
    if total is not None:
        sets.append("total=?"); params.append(int(total))
    if processed is not None:
        sets.append("processed=?"); params.append(int(processed))
    if filename is not None:
        sets.append("filename=?"); params.append(filename)
    if size_bytes is not None:
        sets.append("size_bytes=?"); params.append(int(size_bytes))
    if error is not None:
        sets.append("error=?"); params.append(error)
    if not sets:
        return
    sql = f"UPDATE exports SET {', '.join(sets)} WHERE id=?"
    params.append(_id)
    with db_conn() as con:
        con.execute(sql, tuple(params))
        con.commit()

def db_get_export(_id):
    with db_conn() as con:
        cur = con.cursor()
        cur.execute("""
            SELECT id, filename, format, created_at, status, total, processed, filters, description, error, size_bytes
            FROM exports WHERE id=?
        """, (_id,))
        r = cur.fetchone()
        if not r: return None
        (_id, filename, _format, created_at, status, total, processed, filters, description, error, size_bytes) = r
        percent = 0
        try:
            if total and total > 0:
                percent = round((processed / total) * 100)
        except Exception:
            percent = 0
        try:
            fdict = json.loads(filters or "{}")
        except Exception:
            fdict = {}
        return {
            "id": _id,
            "filename": filename,
            "format": _format,
            "createdAt": created_at,
            "status": status,
            "total": total,
            "processed": processed,
            "percent": percent,
            "filters": fdict,
            "description": description,
            "error": error,
            "sizeBytes": size_bytes,
            "downloadUrl": f"/exports/{_id}/download" if (status == "done" and filename) else None
        }

def db_list_exports(limit=100):
    with db_conn() as con:
        cur = con.cursor()
        cur.execute("""
            SELECT id, filename, format, created_at, status, total, processed, filters, description, error, size_bytes
            FROM exports
            ORDER BY datetime(created_at) DESC
            LIMIT ?
        """, (limit,))
        rows = cur.fetchall()
    out = []
    for r in rows:
        (_id, filename, _format, created_at, status, total, processed, filters, description, error, size_bytes) = r
        try:
            fdict = json.loads(filters or "{}")
        except Exception:
            fdict = {}
        short_desc = _resumo_filtros_curto(fdict, TOKENS)
        percent = 0
        try:
            if total and total > 0:
                percent = round((processed / total) * 100)
        except Exception:
            percent = 0
        out.append({
            "id": _id,
            "filename": filename,
            "format": _format,
            "createdAt": created_at,
            "status": status,
            "total": total,
            "processed": processed,
            "percent": percent,
            "filters": fdict,
            "description": description if description else _resumo_filtros(fdict, TOKENS),
            "shortDescription": short_desc,
            "error": error,
            "sizeBytes": size_bytes,
            "downloadUrl": f"/exports/{_id}/download" if (status == "done" and filename) else None
        })
    return out

def db_delete_export(_id):
    reg = db_get_export(_id)
    if not reg: return False
    if reg.get("filename"):
        path = os.path.join(EXPORT_DIR, reg["filename"])
        try:
            if os.path.exists(path):
                os.remove(path)
        except Exception:
            pass
    with db_conn() as con:
        con.execute("DELETE FROM exports WHERE id=?", (_id,))
        con.commit()
    return True

# --------- Cache de nomes (por TOKEN) ----------
CACHE_TTL = 600  # 10 min
_cache_maps = {
    "users": {},     # token -> {"at":ts, "map":{id:name}}
    "channels": {},
    "sectors": {},
}

def _now_ts(): return int(time.time())

def _cache_get(kind, token):
    n = _cache_maps[kind].get(token)
    if n and (_now_ts() - n["at"] < CACHE_TTL):
        return n["map"]
    return None

def _cache_set(kind, token, data_map):
    _cache_maps[kind][token] = {"at": _now_ts(), "map": data_map or {}}

def _build_map(lst, id_keys=("id","channelId","departmentId"), name_keys=("name","description","identifier","nickName","email")):
    out = {}
    for it in lst or []:
        _id = None
        for k in id_keys:
            if it.get(k):
                _id = it[k]; break
        if not _id: continue
        name = None
        for k in name_keys:
            if it.get(k):
                name = it[k]; break
        if not name: name = _id
        out[_id] = name
    return out

def _load_users_map(token):
    cached = _cache_get("users", token)
    if cached is not None: return cached
    url = f"{BASE_URL}/users"
    r = _http_get(token, url); r.raise_for_status()
    m = _build_map(r.json(), id_keys=("id",), name_keys=("name","nickName","email"))
    _cache_set("users", token, m); return m

def _load_channels_map(token):
    cached = _cache_get("channels", token)
    if cached is not None: return cached
    url = f"{BASE_URL}/channel/list"
    r = _http_get(token, url); r.raise_for_status()
    m = _build_map(r.json(), id_keys=("id","channelId"), name_keys=("description","identifier","name"))
    _cache_set("channels", token, m); return m

def _load_sectors_map(token):
    cached = _cache_get("sectors", token)
    if cached is not None: return cached
    url = f"{BASE_URL}/sectors"
    r = _http_get(token, url); r.raise_for_status()
    m = _build_map(r.json(), id_keys=("id","departmentId"), name_keys=("name","description"))
    _cache_set("sectors", token, m); return m

def _split_tok_id(val):
    """Retorna (token, id) a partir de 'TOKEN::ID' ou (None, val) se não houver prefixo."""
    if not val: return (None, None)
    if "::" in val:
        t, i = val.split("::", 1)
        return (t, i)
    return (None, val)

# ------- resolvers para nome→ID por token (EXATO) -------
def _resolve_id_by_hint(kind, token, hint, *, exact_only=True):
    """
    - Se hint vier como 'TOKEN::ID' e o token não bater, retorna None.
    - Se hint for um ID válido (sem token), usa direto.
    - Se for NOME, com exact_only=True só aceita match **exato** (case-insensitive).
    """
    if not hint:
        return None
    t, maybe_id_or_name = _split_tok_id(hint)
    if t and t != token:
        return None

    if kind == "users":
        m = _load_users_map(token)
    elif kind == "channels":
        m = _load_channels_map(token)
    else:
        m = _load_sectors_map(token)

    # ID sem token
    if t is None and maybe_id_or_name in m:
        return maybe_id_or_name

    name_hint = (maybe_id_or_name or "").strip()
    if not name_hint:
        return None

    # match exato sensível
    for _id, nm in m.items():
        if nm == name_hint:
            return _id

    # match exato case-insensitive
    name_fold = name_hint.casefold()
    for _id, nm in m.items():
        if (nm or "").casefold() == name_fold:
            return _id

    if exact_only:
        return None

    # opcional (desativado por padrão): prefixo
    for _id, nm in m.items():
        if (nm or "").casefold().startswith(name_fold):
            return _id
    return None


# ------- normalization helpers (accent/space-insensitive) -------
import unicodedata
def _norm_txt(s: str) -> str:
    if not s: return ""
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = " ".join(s.replace("\u00A0"," ").split())  # collapse spaces
    return s.strip().casefold()

# ------- raw loaders to match across name/nick/email -------
_users_raw_cache = {}
def _load_users_raw(token):
    cached = _users_raw_cache.get(token)
    if cached and (_now_ts() - cached["at"] < CACHE_TTL):
        return cached["data"]
    url = f"{BASE_URL}/users"
    r = _http_get(token, url); r.raise_for_status()
    data = r.json() or []
    _users_raw_cache[token] = {"at": _now_ts(), "data": data}
    return data

def _resolve_user_id(token, hint):
    """Resolve por:
    - TOKEN::ID (direto)
    - ID exato
    - match EXATO e NORMALIZADO em name/nickName/email (e parte local do email)
    - se não achar, tenta prefixo único (normalizado)
    """
    if not hint:
        return None
    t, val = _split_tok_id(hint)
    if t and t != token:
        return None
    # candidate direct ID
    raw = _load_users_raw(token)
    ids = { str(u.get("id")) for u in raw if u.get("id") }
    if t is None and val in ids:
        return val

    target = _norm_txt(val)
    if not target:
        return None

    def keys(u):
        nm  = u.get("name") or ""
        nn  = u.get("nickName") or ""
        em  = u.get("email") or ""
        em_local = em.split("@",1)[0] if "@" in em else em
        return [(nm, _norm_txt(nm)), (nn, _norm_txt(nn)), (em, _norm_txt(em)), (em_local, _norm_txt(em_local))]

    # 1) exact (non-normalized) on any field
    for u in raw:
        for orig, normed in keys(u):
            if orig == val:
                return str(u.get("id"))

    # 2) exact normalized on any field
    match_ids = set()
    for u in raw:
        for orig, normed in keys(u):
            if normed and normed == target:
                match_ids.add(str(u.get("id")))
                break
    if len(match_ids) == 1:
        return next(iter(match_ids))
    # 3) unique prefix normalized
    pref_ids = set()
    for u in raw:
        for orig, normed in keys(u):
            if normed and normed.startswith(target):
                pref_ids.add(str(u.get("id")))
                break
    if len(pref_ids) == 1:
        return next(iter(pref_ids))
    return None

# -------------- Helpers Gerais --------------
def _esc(s: str) -> str:
    return html_lib.escape(s or "")

def parse_iso_guess(s):
    if not s:
        return None
    try:
        if "T" in s:
            s2 = s.rstrip("Z")
            try:
                return datetime.fromisoformat(s2)
            except ValueError:
                if "." not in s2:
                    return datetime.fromisoformat(s2 + ".000")
                raise
        else:
            return datetime.fromisoformat(s + "T00:00:00")
    except Exception:
        return None

def to_iso_millis(d: datetime):
    if not d:
        return None
    return d.strftime("%Y-%m-%dT%H:%M:%S.000")

def clip_to_day_end(d: datetime):
    return d.replace(hour=23, minute=59, second=59, microsecond=0)

def _to_epoch_ms(unix_like):
    try:
        v = int(unix_like or 0)
    except Exception:
        return 0
    # heurística: segundos -> ms
    if v > 10**12:
        return v
    if v < 10**10:
        return v * 1000
    return v

def _resumo_filtros(f, tokens_pool):
    def _v(k):
        v = f.get(k)
        return (str(v).strip() if isinstance(v, str) else v)

    mapa_status = {"1": "Aguardando", "2": "Manual", "3": "Finalizado"}
    mapa_tipo   = {"2": "Individual", "3": "Grupo"}

    status_txt  = mapa_status.get(_v("status") or "", "Todos os status")
    tipo_txt    = mapa_tipo.get(_v("typeChat") or "", "Todos os tipos")
    cliente_txt = _v("cliente") or "Todos os clientes"
    usuario_txt = _v("usuario") or "Todos os usuários"
    canal_txt   = _v("canal") or "Todos os canais"
    setor_txt   = _v("setor") or "Todos os setores"
    protocolo_tx = _v("protocolo") or "Todos os protocolos"

    ini = _v("startDate"); fim = _v("endDate")
    periodo_txt = "Todo o período" if (not ini and not fim) else f"Período: {ini or '?'} → {fim or '?'}"

    partes = [
        f"Status: {status_txt}",
        f"Cliente: {cliente_txt}",
        f"Usuário: {usuario_txt}",
        f"Canal: {canal_txt}",
        f"Setor: {setor_txt}",
        f"Protocolo: {protocolo_tx}",
        f"Tipo: {tipo_txt}",
        periodo_txt
    ]
    return " | ".join(partes)

def _resumo_filtros_curto(f, tokens_pool):
    def _v(k):
        v = f.get(k)
        return (str(v).strip() if isinstance(v, str) else v)

    mapa_status = {"1": "Aguardando", "2": "Manual", "3": "Finalizado"}
    mapa_tipo   = {"2": "Individual", "3": "Grupo"}

    pecas = []
    if _v("status"):    pecas.append(f"Status: {mapa_status.get(_v('status'), _v('status'))}")
    if _v("cliente"):   pecas.append(f"Cliente: {_v('cliente')}")
    if _v("usuario"):   pecas.append(f"Usuário: {_v('usuario')}")
    if _v("canal"):     pecas.append(f"Canal: {_v('canal')}")
    if _v("setor"):     pecas.append(f"Setor: {_v('setor')}")
    if _v("protocolo"): pecas.append(f"Protocolo: {_v('protocolo')}")
    if _v("typeChat"):  pecas.append(f"Tipo: {mapa_tipo.get(_v('typeChat'), _v('typeChat'))}")

    if _v("startDate") or _v("endDate"):
        ini = _v("startDate") or "?"
        fim = _v("endDate") or "?"
        pecas.append(f"{ini} → {fim}")

    if not pecas:
        return "Todos os registros (resumido)"
    return " | ".join(pecas[:4]) + (" ..." if len(pecas) > 4 else "")

# ---------- Helpers de payload compatível Swagger ----------
def _formatar_data(d: str) -> str:
    """Normaliza data para formato esperado pela API (YYYY-MM-DDTHH:MM:SS.mmm)."""
    try:
        if not d: return None
        if "T" in d:
            if "." in d: return d
            return d + ".000"
        dt = datetime.fromisoformat(d)
        return dt.strftime("%Y-%m-%dT%H:%M:%S.000")
    except Exception:
        return None

def _build_date_filters(start_iso: str, end_iso: str):
    """Monta o objeto dateFilters do contrato (byStartDate/byEndDate com start/finish)."""
    df = {}
    by = {}
    if start_iso: by["start"] = start_iso
    if end_iso:   by["finish"] = end_iso
    if by:
        df["byStartDate"] = by.copy()
        df["byEndDate"] = by.copy()
    return df or None

def _map_cliente_fields(cli: str):
    """Heurística: se tiver dígitos, usar como number; se vier CONTACT::ID, usar contactId."""
    out = {}
    if not cli:
        return out
    cli = cli.strip()
    if cli.upper().startswith("CONTACT::"):
        out["contactId"] = cli.split("::", 1)[1].strip()
        return out
    if any(ch.isdigit() for ch in cli):
        out["number"] = cli
    return out

# -------------- API: Chats (multi-token) --------------
def _iter_api_chunks(start_dt: datetime, end_dt: datetime):
    """Gera janelas [s, e] com no máx. 30 dias para cobrir [start_dt, end_dt]."""
    if start_dt is None and end_dt is None:
        yield None, None
        return
    s = start_dt or datetime(2020, 1, 1)
    e = end_dt or datetime.now()
    if e < s:
        s, e = e, s
    e = clip_to_day_end(e)
    CHUNK = timedelta(days=30) - timedelta(seconds=1)
    cur = s
    while cur <= e:
        cur_end = min(cur + CHUNK, e)
        yield cur, cur_end
        cur = cur_end + timedelta(seconds=1)

def listar_chats_page_multi(filtros: dict, page: int, tokens_pool, chunk_start: datetime, chunk_end: datetime):
    """Consulta /chats/list em TODOS os tokens considerando chunk de datas e filtro por NOME estrito."""
    url = f"{BASE_URL}/chats/list"
    usuario_hint = filtros.get("usuario")
    setor_hint   = filtros.get("setor")
    cliente_hint = filtros.get("cliente")
    protocolo    = filtros.get("protocolo")

    alvo_tokens = set(tokens_pool or [])

    # dateFilters (do chunk, se houver)
    start_iso = to_iso_millis(chunk_start) if chunk_start else _formatar_data(filtros.get("startDate"))
    end_iso   = to_iso_millis(chunk_end)   if chunk_end   else _formatar_data(filtros.get("endDate"))
    date_filters = _build_date_filters(start_iso, end_iso)

    payload_base = {
        "page": page,
        "status": int(filtros["status"]) if filtros.get("status") else 3,
        "typeChat": int(filtros["typeChat"]) if filtros.get("typeChat") else 2,
        "protocol": protocolo or None
    }
    if date_filters:
        payload_base["dateFilters"] = date_filters
    payload_base.update(_map_cliente_fields(cliente_hint))

    todos, has_next_any = [], False

    for tok in alvo_tokens:
        payload = dict(payload_base)

        # userId por token (resolução exata)
        if usuario_hint:
            uid = _resolve_user_id(tok, usuario_hint)
            if uid is None:
                continue
            payload["userId"] = uid

        # sectorId por token (resolução exata)
        if setor_hint:
            sid_ = _resolve_id_by_hint("sectors", tok, setor_hint, exact_only=True)
            if sid_ is None:
                continue
            payload["sectorId"] = sid_

        payload = {k: v for k, v in payload.items() if v not in (None, "", [], {})}

        resp = _http_post(tok, url, payload)
        if resp.status_code == 400:
            # contrato invalido para este token / ids inexistentes
            continue
        resp.raise_for_status()
        data = resp.json() or {}
        chats = data.get("chats", [])
        for c in chats:
            c["_token"] = tok
        todos.extend(chats)
        if data.get("hasNextPage"):
            has_next_any = True

    return {"chats": todos, "hasNextPage": has_next_any}

def coletar_todos_chats(filtros: dict, sid: str, job_id: str, request_tokens=None):
    tokens_pool = request_tokens if request_tokens is not None else TOKENS
    todos = []
    unique = set()  # (token, attendanceId)

    # período solicitado (para recorte exato por mensagem)
    req_start = parse_iso_guess(filtros.get("startDate"))
    req_end   = parse_iso_guess(filtros.get("endDate"))

    sem_filtros = not any([
        filtros.get("status"), filtros.get("typeChat"),
        filtros.get("startDate"), filtros.get("endDate"),
        filtros.get("cliente"), filtros.get("usuario"),
        filtros.get("canal"), filtros.get("setor"),
        filtros.get("protocolo")
    ])

    if sem_filtros:
        inicio = datetime(2020, 1, 1, tzinfo=timezone.utc)
        fim = datetime.now(timezone.utc)
        cursor = inicio
        while cursor < fim:
            bloco_fim = cursor + relativedelta(months=6)
            payload = filtros.copy()
            payload["startDate"] = cursor.strftime("%Y-%m-%dT00:00:00.000")
            payload["endDate"] = bloco_fim.strftime("%Y-%m-%dT23:59:59.000")

            page = 1
            while True:
                data = listar_chats_page_multi(payload, page, tokens_pool, None, None)
                items = data.get("chats", [])
                if not items: break
                for it in items:
                    key = (it.get("_token"), it.get("attendanceId"))
                    if key in unique: continue
                    unique.add(key)
                    todos.append(it)
                socketio.emit("indexando", {"coletados": len(todos)}, room=sid)
                db_update_status(job_id, status="indexing", processed=len(todos))
                if not data.get("hasNextPage"): break
                page += 1
            cursor = bloco_fim
    else:
        if req_start or req_end:
            for ch_s, ch_e in _iter_api_chunks(req_start, req_end):
                payload = filtros.copy()
                payload["startDate"] = to_iso_millis(ch_s)
                payload["endDate"] = to_iso_millis(ch_e)
                page = 1
                while True:
                    data = listar_chats_page_multi(payload, page, tokens_pool, ch_s, ch_e)
                    items = data.get("chats", [])
                    if not items: break
                    for it in items:
                        key = (it.get("_token"), it.get("attendanceId"))
                        if key in unique: continue
                        unique.add(key)
                        todos.append(it)
                    socketio.emit("indexando", {"coletados": len(todos)}, room=sid)
                    db_update_status(job_id, status="indexing", processed=len(todos))
                    if not data.get("hasNextPage"): break
                    page += 1
        else:
            page = 1
            while True:
                data = listar_chats_page_multi(filtros, page, tokens_pool, None, None)
                items = data.get("chats", [])
                if not items: break
                for it in items:
                    key = (it.get("_token"), it.get("attendanceId"))
                    if key in unique: continue
                    unique.add(key)
                    todos.append(it)
                socketio.emit("indexando", {"coletados": len(todos)}, room=sid)
                db_update_status(job_id, status="indexing", processed=len(todos))
                if not data.get("hasNextPage"): break
                page += 1

    return todos, req_start, req_end

def obter_chat(attendance_id: str, token: str):
    url = f"{BASE_URL}/chats/{attendance_id}"
    resp = _http_get(token, url)
    resp.raise_for_status()
    return resp.json()


def _contact_meta(det):
    """Extrai metadados do contato de forma tolerante ao schema."""
    c = det.get("contact") or {}
    ch = det.get("channel") or {}
    org = det.get("currentOrganization") or {}
    return {
        "name": det.get("description") or c.get("name") or c.get("description") or "—",
        "number": c.get("number") or det.get("number") or det.get("contactNumber") or "—",
        "id": c.get("id") or det.get("contactId") or "—",
        "channel": ch.get("description") or ch.get("identifier") or det.get("channelId") or "—",
        "organization": org.get("description") or org.get("name") or "—",
        "survey": det.get("resultResearchSatisfaction") or "—",
    }

# -------------- Filtro de mensagens --------------
def _formatar_item_visivel(m):
    texto = m.get("text") or ""
    if m.get("isSystemMessage") and not ("iniciado" in texto.lower() or "transferido" in texto.lower()):
        return None
    return (texto, bool(m.get("isSentByMe")), m.get("dhMessage") or "", bool(m.get("isSystemMessage")), _to_epoch_ms(m.get("unixTimeMessage")))

def _msg_in_period(epoch_ms, req_start: datetime, req_end: datetime):
    if not (req_start or req_end):
        return True
    if epoch_ms <= 0:
        return False
    t = datetime.fromtimestamp(epoch_ms/1000.0)
    if req_start and t < req_start: return False
    if req_end and t > clip_to_day_end(req_end): return False
    return True

# -------------- PDF --------------
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4

def _fundo(canvas, doc):
    """Fundo bege estilo WhatsApp"""
    canvas.saveState()
    canvas.setFillColor(colors.HexColor("#ECE5DD"))
    canvas.rect(0, 0, A4[0], A4[1], stroke=0, fill=1)
    canvas.restoreState()

def gerar_pdf(chats_list, sid: str, job_id: str, tokens_pool, req_start, req_end):
    out_name = f"{job_id}.pdf"
    out_path = os.path.join(EXPORT_DIR, out_name)

    doc = SimpleDocTemplate(
        out_path,
        pagesize=A4,
        rightMargin=30, leftMargin=30,
        topMargin=30, bottomMargin=30
    )
    styles = getSampleStyleSheet()

    # estilos
    estilo_msg = ParagraphStyle("msg", parent=styles["Normal"], fontSize=9, leading=12, textColor=colors.black)
    estilo_hora = ParagraphStyle("hora", parent=styles["Normal"], fontSize=7, textColor=colors.HexColor("#777"), alignment=2)
    estilo_sys = ParagraphStyle("sys", parent=styles["Normal"], fontSize=8, textColor=colors.HexColor("#444"), alignment=1)

    story = []
    total = len(chats_list)
    socketio.emit("inicio", {"total": total}, room=sid)
    db_update_status(job_id, status="processing", total=total, processed=0)

    kept = 0
    for i, chat in enumerate(chats_list, start=1):
        tok = chat.get("_token") or ((tokens_pool or TOKENS or [""])[0])
        detalhes = obter_chat(chat["attendanceId"], tok)

        msgs_src = sorted(detalhes.get("messages", []), key=lambda x: _to_epoch_ms(x.get("unixTimeMessage")))
        msgs = []
        for m in msgs_src:
            vis = _formatar_item_visivel(m)
            if not vis: 
                continue
            texto, is_me, data_iso, is_sys, epoch_ms = vis
            if not _msg_in_period(epoch_ms, req_start, req_end):
                continue
            msgs.append((texto, is_me, data_iso, is_sys))

        if not msgs:
            socketio.emit("progresso", {"atual": i, "total": total}, room=sid)
            db_update_status(job_id, processed=i)
            continue

        kept += 1
        meta = _contact_meta(detalhes)
        story.append(Paragraph(f"Protocolo: {detalhes.get('protocol', '')}", styles["Heading2"]))
        story.append(Paragraph(f"Contato: {meta['name']} (Número: {meta['number']})", styles["Normal"]))
        story.append(Paragraph(f"Contato ID: {meta['id']} • Canal: {meta['channel']} • Empresa: {meta['organization']} • Pesquisa: {meta['survey']}", styles["Normal"]))
        story.append(Spacer(1, 12))

        for (texto, is_me, data_iso, is_sys) in msgs:
            safe_text = _esc(texto).replace("\n", "<br/>")

            if is_sys:
                p_msg = Paragraph(safe_text, estilo_sys)
                balao_sys = Table([[p_msg]], colWidths=[doc.width * 0.5])
                balao_sys.setStyle(TableStyle([
                    ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#F0F0F0")),
                    ("TEXTCOLOR", (0, 0), (-1, -1), colors.HexColor("#555")),
                    ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("LEFTPADDING", (0, 0), (-1, -1), 6),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                    ("TOPPADDING", (0, 0), (-1, -1), 3),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
                    ("ROUNDED", (0, 0), (-1, -1), 12),
                ]))
                linha_sys = Table([[balao_sys]], colWidths=[doc.width])
                linha_sys.setStyle(TableStyle([
                    ("ALIGN", (0, 0), (0, 0), "CENTER"),
                ]))
                story.append(linha_sys)
                story.append(Spacer(1, 6))
                continue

            p_msg = Paragraph(safe_text, estilo_msg)
            p_hora = Paragraph(_esc(data_iso), estilo_hora)

            balao = Table([[p_msg], [p_hora]], colWidths=[doc.width * 0.6])
            balao.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#DCF8C6") if is_me else colors.white),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
                ("ROUNDED", (0, 0), (-1, -1), 30), 
            ]))

            linha = Table([[balao]], colWidths=[doc.width])
            linha.setStyle(TableStyle([
                ("ALIGN", (0, 0), (0, 0), "RIGHT" if is_me else "LEFT"),
            ]))

            story.append(linha)
            story.append(Spacer(1, 8))

        story.append(PageBreak())
        socketio.emit("progresso", {"atual": i, "total": total}, room=sid)
        db_update_status(job_id, processed=i)
        time.sleep(0.002)

    if kept == 0 and total > 0:
        story = [Paragraph("Nenhuma mensagem encontrada no período solicitado.", styles["Heading2"])]
        SimpleDocTemplate(out_path, pagesize=A4, rightMargin=30, leftMargin=30, topMargin=30, bottomMargin=30).build(story)
    else:
        doc.build(story, onFirstPage=_fundo, onLaterPages=_fundo)

    size_bytes = os.path.getsize(out_path)
    db_update_status(job_id, status="done", filename=out_name, size_bytes=size_bytes)
    return out_path

# -------------- CSV --------------
def gerar_csv(chats_list, sid: str, job_id: str, tokens_pool, req_start, req_end):
    out_name = f"{job_id}.csv"
    out_path = os.path.join(EXPORT_DIR, out_name)

    total = len(chats_list)
    socketio.emit("inicio", {"total": total}, room=sid)
    db_update_status(job_id, status="processing", total=total, processed=0)

    with open(out_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=";")

        # Cabeçalho fixo
        writer.writerow(["Protocolo", "Contato", "ContatoNumero", "ContatoId", "Empresa", "Canal", "Pesquisa", "Conversa"])

        for i, chat in enumerate(chats_list, start=1):
            tok = chat.get("_token") or ((tokens_pool or TOKENS or [""])[0])
            detalhes = obter_chat(chat["attendanceId"], tok)

            msgs_src = sorted(detalhes.get("messages", []), key=lambda x: _to_epoch_ms(x.get("unixTimeMessage")))
            msgs = []
            for m in msgs_src:
                vis = _formatar_item_visivel(m)
                if not vis:
                    continue
                texto, is_me, data_iso, _is_sys, epoch_ms = vis
                if not _msg_in_period(epoch_ms, req_start, req_end):
                    continue
                msgs.append((texto, is_me, data_iso))

            if not msgs:
                socketio.emit("progresso", {"atual": i, "total": total}, room=sid)
                db_update_status(job_id, processed=i)
                continue

            meta = _contact_meta(detalhes)
            protocolo = detalhes.get("protocol", "")
            contato = meta["name"]
            numero = meta["number"]
            contato_id = meta["id"]
            org = meta["organization"]
            canal = meta["channel"]
            pesquisa = meta["survey"]

            header_line = f"===== Protocolo: {protocolo} | Contato: {contato} | Número: {numero} | ID: {contato_id} | Empresa: {org} | Canal: {canal} | Pesquisa: {pesquisa} ====="
            writer.writerow([protocolo, contato, numero, contato_id, org, canal, pesquisa, header_line])

            for (texto, is_me, data_iso) in msgs:
                remetente = "Você" if is_me else "Contato"
                texto_processado = (texto or '').replace(';', ',').replace('\n', ' ').strip()
                msg_line = f"{data_iso} - {remetente}: {texto_processado}"
                writer.writerow([protocolo, contato, numero, contato_id, org, canal, pesquisa, msg_line])

            writer.writerow([protocolo, contato, numero, contato_id, org, canal, pesquisa, ""])

            socketio.emit("progresso", {"atual": i, "total": total}, room=sid)
            db_update_status(job_id, processed=i)

    size_bytes = os.path.getsize(out_path)
    db_update_status(job_id, status="done", filename=out_name, size_bytes=size_bytes)
    return out_path

# -------------- HTML (estilo WhatsApp real) --------------
HTML_CSS = """
<!DOCTYPE html>
<html lang="pt-br">
<head>
<meta charset="UTF-8">
<title>Histórico de Conversa</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  body {
    margin:0;
    font-family: system-ui, -apple-system, 'Segoe UI', Roboto, Arial;
    background:#ECE5DD;
  }
  .chat-wrap {
    max-width: 900px;
    margin: 0 auto;
    min-height: 100vh;
    padding: 20px;
  }
  .header {
    background: #075E54;
    color:#fff;
    padding: 14px 16px;
  }
  .title { font-size:18px; font-weight:700; }
  .subtitle { font-size:12px; opacity:.9; margin-top:2px; }

  .conv { padding: 20px 10px 40px; }

  .bubble {
    position: relative;
    margin: 6px 0;
    padding: 8px 10px 4px 10px;
    border-radius: 18px;
    max-width: 65%;
    box-shadow: 0 1px 1px rgba(0,0,0,.15);
    word-wrap:break-word;
    font-size:14px;
    line-height:1.4;
  }
  .me {
    background:#DCF8C6;
    margin-left:auto;
    border-top-right-radius: 0;
  }
  .other {
    background:#fff;
    margin-right:auto;
    border-top-left-radius: 0;
  }
  .sys {
    background:#F0F0F0;
    color:#555;
    font-size:12px;
    text-align:center;
    margin: 10px auto;
    border-radius:12px;
    padding:4px 10px;
    max-width: 60%;
  }
  .meta {
    display:block;
    text-align:right;
    font-size:11px;
    color:#777;
    margin-top:3px;
  }
  .meta .check {
    color:#34B7F1;
    font-size:12px;
    margin-left:3px;
  }
  .divider {
    text-align:center;
    color:#607d8b;
    font-size:12px;
    margin:14px 0 8px;
  }
</style>
</head>
<body>
<div class="chat-wrap">
  {BODY}
</div>
</body>
</html>
"""

def _bubble(texto, data_iso, is_me, is_system):
    safe_text = _esc(texto).replace("\n", "<br>")
    if is_system:
        return f'<div class="sys">{safe_text}</div>'
    else:
        klass = "bubble me" if is_me else "bubble other"
        meta = f'{_esc(data_iso)}'
        if is_me:
            meta += '<span class="check">✔✔</span>'
        return f'<div class="{klass}">{safe_text}<span class="meta">{meta}</span></div>'

def gerar_html(chats_list, sid: str, job_id: str, tokens_pool, req_start, req_end):
    out_name = f"{job_id}.html"
    out_path = os.path.join(EXPORT_DIR, out_name)

    total = len(chats_list)
    socketio.emit("inicio", {"total": total}, room=sid)
    db_update_status(job_id, status="processing", total=total, processed=0)

    parts = []
    for i, chat in enumerate(chats_list, start=1):
        tok = chat.get("_token") or ((tokens_pool or TOKENS or [""])[0])
        detalhes = obter_chat(chat["attendanceId"], tok)

        msgs_src = sorted(detalhes.get("messages", []), key=lambda x: _to_epoch_ms(x.get("unixTimeMessage")))
        bubbles, last_day = [], None
        kept_any = False
        for m in msgs_src:
            vis = _formatar_item_visivel(m)
            if not vis: continue
            texto, is_me, data_iso, is_sys, epoch_ms = vis
            if not _msg_in_period(epoch_ms, req_start, req_end):
                continue
            kept_any = True
            dia = (data_iso or "")[:10]
            if dia and dia != last_day:
                bubbles.append(f'<div class="divider">{_esc(dia)}</div>')
                last_day = dia
            bubbles.append(_bubble(texto, data_iso, is_me, is_sys))

        if not kept_any:
            socketio.emit("progresso", {"atual": i, "total": total}, room=sid)
            db_update_status(job_id, processed=i)
            continue

        meta = _contact_meta(detalhes)
        contato = meta["name"]
        numero = meta["number"]
        contato_id = meta["id"]
        protocolo = detalhes.get("protocol","—")
        org = meta["organization"]
        canal = meta["channel"]
        pesquisa = meta["survey"]
        head = f'<div class="header"><div class="title">{_esc(contato)} <span style="font-weight:400">({_esc(numero)})</span></div><div class="subtitle"><strong>Protocolo:</strong> {_esc(protocolo)} • <strong>ID:</strong> {_esc(contato_id)} • <strong>Canal:</strong> {_esc(canal)} • <strong>Empresa:</strong> {_esc(org)} • <strong>Pesquisa:</strong> {_esc(pesquisa)}</div></div>'
        conv_open = '<div class="conv">'
        conv_close = "</div>"
        parts.append(head + conv_open + "".join(bubbles) + conv_close)

        socketio.emit("progresso", {"atual": i, "total": total}, room=sid)
        db_update_status(job_id, processed=i)

    html_final = HTML_CSS.replace("{BODY}", "".join(parts) if parts else "<div class='header'><div class='title'>Nenhuma mensagem no período solicitado.</div></div>")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html_final)

    size_bytes = os.path.getsize(out_path)
    db_update_status(job_id, status="done", filename=out_name, size_bytes=size_bytes)
    return out_path

# -------------- Dedup nos selects (um nome, múltiplas fontes) --------------
def _dedupe_collect(kind_loader, prefer_names):
    name_map = {}
    seen = set()
    for tok in (get_request_tokens() or TOKENS or []):
        rlist = []
        if kind_loader == "users":
            r = _http_get(tok, f"{BASE_URL}/users")
            if r.status_code == 200:
                rlist = r.json() or []
        elif kind_loader == "channels":
            r = _http_get(tok, f"{BASE_URL}/channel/list")
            if r.status_code == 200:
                rlist = r.json() or []
        else:
            r = _http_get(tok, f"{BASE_URL}/sectors")
            if r.status_code == 200:
                rlist = r.json() or []

        for it in rlist:
            _id = it.get("id") or it.get("channelId") or it.get("departmentId")
            name = None
            for key in prefer_names:
                if it.get(key):
                    name = it[key]; break
            if not name:
                name = it.get("description") or it.get("identifier") or it.get("nickName") or _id or "—"
            if not _id:
                continue
            key = (tok, _id)
            if key in seen: 
                continue
            seen.add(key)

            name_key = (name or "").strip()
            name_key_fold = name_key.casefold()
            if name_key_fold not in name_map:
                name_map[name_key_fold] = {"name": name_key, "sources": set()}
            name_map[name_key_fold]["sources"].add(f"{tok}::{_id}")

    out = []
    for fold, obj in name_map.items():
        out.append({
            "id": obj["name"],        # o select usa o NOME como id
            "name": obj["name"],
            "sources": sorted(obj["sources"])  # para debug/tooltip se quiser
        })
    out.sort(key=lambda x: x["name"].casefold())
    return out

# -------------- Rotas UI --------------
@app.route("/")
def index():
    url_tokens_str = (request.args.get("tokens") or request.args.get("token") or "").strip()
    resp = make_response(render_template("index.html"))
    if url_tokens_str:
        resp.set_cookie("tokens", url_tokens_str, max_age=7*24*3600, httponly=False, samesite="Lax")
        print(f"Cookie 'tokens' gravado: { _mask(_split_tokens_str(url_tokens_str)) }")
    return resp

# -------------- Fluxo de exportação --------------
@app.route("/exportar", methods=["POST"])
def exportar():
    sid = request.form.get("sid")
    formato = request.form.get("formato", "pdf")
    filtros = {
        "status": request.form.get("status"),
        "typeChat": request.form.get("typeChat"),
        "startDate": request.form.get("startDate"),
        "endDate": request.form.get("endDate"),
        "cliente": request.form.get("cliente"),
        "usuario": request.form.get("usuario"),
        "canal": request.form.get("canal"),
        "setor": request.form.get("setor"),
        "protocolo": request.form.get("protocolo")
    }

    current_request_tokens = get_request_tokens()

    job_id = str(uuid.uuid4())
    descricao = f"Exportação ({formato.upper()}) - " + _resumo_filtros(filtros, current_request_tokens)
    db_insert_export(job_id, formato, descricao, json.dumps(filtros, ensure_ascii=False))

    def tarefa():
        try:
            chats_list, req_start, req_end = coletar_todos_chats(filtros, sid, job_id, request_tokens=current_request_tokens)
            if not chats_list:
                db_update_status(job_id, status="done", total=0, processed=0, filename=None, size_bytes=0)
                socketio.emit("erro", {"mensagem": "Nenhum chat encontrado."}, room=sid)
                return

            if formato == "csv":
                gerar_csv(chats_list, sid, job_id, current_request_tokens, req_start, req_end)
            elif formato == "html":
                gerar_html(chats_list, sid, job_id, current_request_tokens, req_start, req_end)
            else:
                gerar_pdf(chats_list, sid, job_id, current_request_tokens, req_start, req_end)

            socketio.emit("concluido", {"url": f"/exports/{job_id}/download"}, room=sid)
        except requests.HTTPError as e:
            msg = getattr(e.response, "text", str(e))
            db_update_status(job_id, status="error", error=f"HTTPError: {msg}")
            socketio.emit("erro", {"mensagem": f"Erro HTTP: {msg}"}, room=sid)
        except Exception as e:
            db_update_status(job_id, status="error", error=str(e))
            socketio.emit("erro", {"mensagem": f"Erro inesperado: {e}"}, room=sid)

    socketio.start_background_task(tarefa)
    return jsonify({"status": "started", "id": job_id}), 202

# -------------- Download por ID --------------
@app.route("/exports/<job_id>/download")
def download_by_id(job_id):
    reg = db_get_export(job_id)
    if not reg or reg.get("status") != "done" or not reg.get("filename"):
        return "Arquivo não disponível.", 404
    path = os.path.join(EXPORT_DIR, reg["filename"])
    if not os.path.exists(path):
        return "Arquivo não encontrado.", 404
    ext = reg["filename"].split(".")[-1].lower()
    mime = "application/pdf" if ext == "pdf" else ("text/csv" if ext == "csv" else "text/html; charset=utf-8")
    return send_file(path, as_attachment=True, download_name=reg["filename"], mimetype=mime)

# -------------- API de histórico de exportações --------------
@app.route("/api/exports")
def api_exports():
    limite = int(request.args.get("limit", 100))
    return jsonify(db_list_exports(limite))

@app.route("/api/exports/<job_id>")
def api_export(job_id):
    reg = db_get_export(job_id)
    if not reg:
        return jsonify({"error": "not_found"}), 404
    return jsonify(reg)

@app.route("/api/exports/<job_id>/delete", methods=["POST"])
def api_exports_delete(job_id):
    ok = db_delete_export(job_id)
    return jsonify({"ok": bool(ok)}), (200 if ok else 404)

# -------------- APIs auxiliares (selects dedup por nome) --------------
@app.route("/api/usuarios")
def api_usuarios():
    merged = _dedupe_collect("users", ("name","nickName","email"))
    return jsonify(merged)

@app.route("/api/canais")
def api_canais():
    merged = _dedupe_collect("channels", ("description","identifier","name"))
    return jsonify(merged)

@app.route("/api/setores")
def api_setores():
    merged = _dedupe_collect("sectors", ("name",))
    return jsonify(merged)

# -------------- Debug helpers --------------
@app.route("/debug/tokens")
def debug_tokens():
    ts = get_request_tokens()
    return jsonify({"detected_tokens": ts, "count": len(ts)})

@app.route("/debug/resolve_user")
def debug_resolve_user():
    # diagnostico: tenta várias estratégias
    name = request.args.get("name","").strip()
    tokens_q = (request.args.get("tokens") or "").strip()
    tokens_pool = [t.strip() for t in tokens_q.split(",") if t.strip()] if tokens_q else (get_request_tokens() or TOKENS or [])
    result = {}
    for tok in tokens_pool:
        rid = _resolve_id_by_hint("users", tok, name, exact_only=True)
        result[tok] = rid
    return jsonify({"hint": name, "resolved": result})

@app.route("/debug/probe")
def debug_probe():
    tokens_q = (request.args.get("tokens") or "").strip()
    tokens_pool = [t.strip() for t in tokens_q.split(",") if t.strip()] if tokens_q else (get_request_tokens() or TOKENS or [])
    out = []
    for tok in tokens_pool:
        r1 = _http_get(tok, f"{BASE_URL}/users")
        r2 = _http_get(tok, f"{BASE_URL}/channel/list")
        out.append({
            "token": tok[:4]+"..."+tok[-4:] if len(tok)>8 else tok,
            "users_status": getattr(r1, "status_code", "err"),
            "channels_status": getattr(r2, "status_code", "err"),
            "users_err": None if getattr(r1, "status_code", 0)==200 else getattr(r1, "text", ""),
            "channels_err": None if getattr(r2, "status_code", 0)==200 else getattr(r2, "text", "")
        })
    return jsonify(out)

# -------------- bootstrap --------------
if __name__ == "__main__":
    # Garante que o banco correspondente ao domínio atual será inicializado
    os.makedirs(EXPORT_DIR, exist_ok=True)
    os.makedirs(DB_DIR, exist_ok=True)
    init_db()
    socketio.run(app, debug=True, port=PORT)

# ===== Overrides: always include a header per chat even if no messages in the period =====
def gerar_pdf(chats_list, sid: str, job_id: str, tokens_pool, req_start, req_end):
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    out_name = f"{job_id}.pdf"
    out_path = os.path.join(EXPORT_DIR, out_name)
    doc = SimpleDocTemplate(out_path, pagesize=A4, rightMargin=30, leftMargin=30, topMargin=30, bottomMargin=30)
    styles = getSampleStyleSheet()
    estilo_msg = ParagraphStyle("msg", parent=styles["Normal"], fontSize=9, leading=12, textColor=colors.black)
    estilo_hora = ParagraphStyle("hora", parent=styles["Normal"], fontSize=7, textColor=colors.HexColor("#777"), alignment=2)
    estilo_sys = ParagraphStyle("sys", parent=styles["Normal"], fontSize=8, textColor=colors.HexColor("#444"), alignment=1)

    story = []
    total = len(chats_list)
    socketio.emit("inicio", {"total": total}, room=sid)
    db_update_status(job_id, status="processing", total=total, processed=0)

    for i, chat in enumerate(chats_list, start=1):
        tok = chat.get("_token") or ((tokens_pool or TOKENS or [""])[0])
        detalhes = obter_chat(chat["attendanceId"], tok)
        meta = _contact_meta(detalhes)

        # Cabeçalho (sempre)
        story.append(Paragraph(f"Protocolo: {detalhes.get('protocol', '')}", styles["Heading2"]))
        story.append(Paragraph(f"Contato: {meta['name']} (Número: {meta['number']})", styles["Normal"]))
        story.append(Paragraph(f"Contato ID: {meta['id']} • Canal: {meta['channel']} • Empresa: {meta['organization']} • Pesquisa: {meta['survey']}", styles["Normal"]))
        story.append(Spacer(1, 12))

        msgs_src = sorted(detalhes.get("messages", []), key=lambda x: _to_epoch_ms(x.get("unixTimeMessage")))
        kept_any = False
        for m in msgs_src:
            vis = _formatar_item_visivel(m)
            if not vis: 
                continue
            texto, is_me, data_iso, is_sys, epoch_ms = vis
            if not _msg_in_period(epoch_ms, req_start, req_end):
                continue
            kept_any = True
            safe_text = _esc(texto).replace("\\n", "<br/>")

            if is_sys:
                p_msg = Paragraph(safe_text, estilo_sys)
                balao_sys = Table([[p_msg]], colWidths=[doc.width * 0.5])
                balao_sys.setStyle(TableStyle([
                    ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#F0F0F0")),
                    ("TEXTCOLOR", (0, 0), (-1, -1), colors.HexColor("#555")),
                    ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("LEFTPADDING", (0, 0), (-1, -1), 6),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                    ("TOPPADDING", (0, 0), (-1, -1), 3),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
                    ("ROUNDED", (0, 0), (-1, -1), 12),
                ]))
                linha_sys = Table([[balao_sys]], colWidths=[doc.width])
                linha_sys.setStyle(TableStyle([("ALIGN", (0, 0), (0, 0), "CENTER"),]))
                story.append(linha_sys)
                story.append(Spacer(1, 6))
                continue

            p_msg = Paragraph(safe_text, estilo_msg)
            p_hora = Paragraph(_esc(data_iso), estilo_hora)
            balao = Table([[p_msg], [p_hora]], colWidths=[doc.width * 0.6])
            balao.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#DCF8C6") if is_me else colors.white),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
                ("ROUNDED", (0, 0), (-1, -1), 30), 
            ]))
            linha = Table([[balao]], colWidths=[doc.width])
            linha.setStyle(TableStyle([("ALIGN", (0, 0), (0, 0), "RIGHT" if is_me else "LEFT"),]))
            story.append(linha)
            story.append(Spacer(1, 8))

        if not kept_any:
            story.append(Paragraph("Sem mensagens dentro do período solicitado.", styles["Normal"]))

        story.append(PageBreak())
        socketio.emit("progresso", {"atual": i, "total": total}, room=sid)
        db_update_status(job_id, processed=i)
        time.sleep(0.001)

    if not story:
        story = [Paragraph("Nenhuma conversa encontrada.", styles["Heading2"])]
        SimpleDocTemplate(out_path, pagesize=A4, rightMargin=30, leftMargin=30, topMargin=30, bottomMargin=30).build(story)
    else:
        doc.build(story, onFirstPage=_fundo, onLaterPages=_fundo)

    size_bytes = os.path.getsize(out_path)
    db_update_status(job_id, status="done", filename=out_name, size_bytes=size_bytes)
    return out_path

def gerar_csv(chats_list, sid: str, job_id: str, tokens_pool, req_start, req_end):
    out_name = f"{job_id}.csv"
    out_path = os.path.join(EXPORT_DIR, out_name)
    total = len(chats_list)
    socketio.emit("inicio", {"total": total}, room=sid)
    db_update_status(job_id, status="processing", total=total, processed=0)
    with open(out_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["Protocolo", "Contato", "ContatoNumero", "ContatoId", "Empresa", "Canal", "Pesquisa", "Conversa"])
        for i, chat in enumerate(chats_list, start=1):
            tok = chat.get("_token") or ((tokens_pool or TOKENS or [""])[0])
            detalhes = obter_chat(chat["attendanceId"], tok)
            meta = _contact_meta(detalhes)
            protocolo = detalhes.get("protocol", "")
            contato = meta["name"]
            numero = meta["number"]
            contato_id = meta["id"]
            org = meta["organization"]
            canal = meta["channel"]
            pesquisa = meta["survey"]
            header_line = f"===== Protocolo: {protocolo} | Contato: {contato} | Número: {numero} | ID: {contato_id} | Empresa: {org} | Canal: {canal} | Pesquisa: {pesquisa} ====="
            writer.writerow([protocolo, contato, numero, contato_id, org, canal, pesquisa, header_line])
            msgs_src = sorted(detalhes.get("messages", []), key=lambda x: _to_epoch_ms(x.get("unixTimeMessage")))
            kept_any = False
            for m in msgs_src:
                vis = _formatar_item_visivel(m)
                if not vis:
                    continue
                texto, is_me, data_iso, _is_sys, epoch_ms = vis
                if not _msg_in_period(epoch_ms, req_start, req_end):
                    continue
                kept_any = True
                remetente = "Você" if is_me else "Contato"
                msg_line = f"{data_iso} - {remetente}: {(texto or '').replace(';', ',').replace('\\n', ' ').strip()}"
                writer.writerow([protocolo, contato, numero, contato_id, org, canal, pesquisa, msg_line])
            if not kept_any:
                writer.writerow([protocolo, contato, numero, contato_id, org, canal, pesquisa, "(Sem mensagens no período)"])
            writer.writerow([protocolo, contato, numero, contato_id, org, canal, pesquisa, ""])
            socketio.emit("progresso", {"atual": i, "total": total}, room=sid)
            db_update_status(job_id, processed=i)
    size_bytes = os.path.getsize(out_path)
    db_update_status(job_id, status="done", filename=out_name, size_bytes=size_bytes)
    return out_path

def gerar_html(chats_list, sid: str, job_id: str, tokens_pool, req_start, req_end):
    out_name = f"{job_id}.html"
    out_path = os.path.join(EXPORT_DIR, out_name)
    total = len(chats_list)
    socketio.emit("inicio", {"total": total}, room=sid)
    db_update_status(job_id, status="processing", total=total, processed=0)
    parts = []
    for i, chat in enumerate(chats_list, start=1):
        tok = chat.get("_token") or ((tokens_pool or TOKENS or [""])[0])
        detalhes = obter_chat(chat["attendanceId"], tok)
        meta = _contact_meta(detalhes)
        contato = meta["name"]
        numero = meta["number"]
        contato_id = meta["id"]
        protocolo = detalhes.get("protocol","—")
        org = meta["organization"]
        canal = meta["channel"]
        pesquisa = meta["survey"]
        head = f'<div class="header"><div class="title">{_esc(contato)} <span style="font-weight:400">({_esc(numero)})</span></div><div class="subtitle"><strong>Protocolo:</strong> {_esc(protocolo)} • <strong>ID:</strong> {_esc(contato_id)} • <strong>Canal:</strong> {_esc(canal)} • <strong>Empresa:</strong> {_esc(org)} • <strong>Pesquisa:</strong> {_esc(pesquisa)}</div></div>'
        conv_open = '<div class="conv">'
        conv_close = "</div>"
        msgs_src = sorted(detalhes.get("messages", []), key=lambda x: _to_epoch_ms(x.get("unixTimeMessage")))
        bubbles, last_day = [], None
        kept_any = False
        for m in msgs_src:
            vis = _formatar_item_visivel(m)
            if not vis: continue
            texto, is_me, data_iso, is_sys, epoch_ms = vis
            if not _msg_in_period(epoch_ms, req_start, req_end):
                continue
            kept_any = True
            dia = (data_iso or "")[:10]
            if dia and dia != last_day:
                bubbles.append(f'<div class="divider">{_esc(dia)}</div>')
                last_day = dia
            # bubble
            safe_text = _esc(texto).replace("\\n", "<br>")
            if is_sys:
                bubbles.append(f'<div class="sys">{safe_text}</div>')
            else:
                klass = "bubble me" if is_me else "bubble other"
                meta_txt = f'{_esc(data_iso)}' + ('<span class="check">✔✔</span>' if is_me else '')
                bubbles.append(f'<div class="{klass}">{safe_text}<span class="meta">{meta_txt}</span></div>')
        if not kept_any:
            bubbles.append('<div class="sys">Sem mensagens dentro do período solicitado.</div>')
        parts.append(head + conv_open + "".join(bubbles) + conv_close)
        socketio.emit("progresso", {"atual": i, "total": total}, room=sid)
        db_update_status(job_id, processed=i)

    html_final = HTML_CSS.replace("{BODY}", "".join(parts) if parts else "<div class='header'><div class='title'>Nenhuma mensagem no período solicitado.</div></div>")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html_final)
    size_bytes = os.path.getsize(out_path)
    db_update_status(job_id, status="done", filename=out_name, size_bytes=size_bytes)
    return out_path
