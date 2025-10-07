import os
import time
import uuid
import csv
import json
import sqlite3
from datetime import datetime, timezone
from flask import Flask, request, jsonify, send_file
from flask_socketio import SocketIO
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from dotenv import load_dotenv
import requests

# Carregar variáveis de ambiente
load_dotenv()

# ================== Configurações Iniciais ==================

PORT = int(os.getenv("PORT", "5000"))
EXPORT_DIR = "exports"
DB_DIR = "exports_dbs"

os.makedirs(EXPORT_DIR, exist_ok=True)
os.makedirs(DB_DIR, exist_ok=True)

# Base URL da API
BASE_URL = "https://api.wescctech.com.br/core/v2/api"

# Função para carregar tokens
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

# Configuração do Flask e SocketIO
app = Flask(__name__)
socketio = SocketIO(app, async_mode="threading")

# Cabeçalhos de requisição
def HEADERS(token):
    return {"access-token": token, "Content-Type": "application/json"}

# ================== Banco de Dados ==================

DB_PATH = os.path.join(DB_DIR, "exports.db")

def db_conn():
    """Abre uma conexão com o banco de dados"""
    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row  # Para acessar colunas por nome
        return con
    except sqlite3.Error as e:
        print(f"Erro ao conectar no banco de dados: {e}")
        return None

def init_db():
    """Inicializa o banco de dados se não existir"""
    with db_conn() as con:
        cursor = con.cursor()
        cursor.execute("""
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

# ================== Geração de Exportações ==================

def gerar_pdf(chats_list, job_id, req_start, req_end):
    out_name = f"{job_id}.pdf"
    out_path = os.path.join(EXPORT_DIR, out_name)

    doc = SimpleDocTemplate(out_path, pagesize=A4, rightMargin=30, leftMargin=30, topMargin=30, bottomMargin=30)
    styles = getSampleStyleSheet()

    # Estilos
    estilo_msg = ParagraphStyle("msg", parent=styles["Normal"], fontSize=9, leading=12, textColor=colors.black)
    estilo_hora = ParagraphStyle("hora", parent=styles["Normal"], fontSize=7, textColor=colors.HexColor("#777"), alignment=2)
    estilo_sys = ParagraphStyle("sys", parent=styles["Normal"], fontSize=8, textColor=colors.HexColor("#444"), alignment=1)

    story = []
    total = len(chats_list)
    socketio.emit("inicio", {"total": total}, room=job_id)
    db_update_status(job_id, status="processing", total=total, processed=0)

    for i, chat in enumerate(chats_list, start=1):
        detalhes = obter_chat(chat["attendanceId"])

        msgs_src = sorted(detalhes.get("messages", []), key=lambda x: x.get("unixTimeMessage"))
        for m in msgs_src:
            texto, data_iso = m.get("text"), m.get("timestamp")
            story.append(Paragraph(f"{data_iso} - {texto}", estilo_msg))

        socketio.emit("progresso", {"atual": i, "total": total}, room=job_id)
        db_update_status(job_id, processed=i)

    doc.build(story)

    size_bytes = os.path.getsize(out_path)
    db_update_status(job_id, status="done", filename=out_name, size_bytes=size_bytes)
    return out_path

def gerar_csv(chats_list, job_id):
    out_name = f"{job_id}.csv"
    out_path = os.path.join(EXPORT_DIR, out_name)

    total = len(chats_list)
    socketio.emit("inicio", {"total": total}, room=job_id)
    db_update_status(job_id, status="processing", total=total, processed=0)

    with open(out_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["Protocolo", "Contato", "Mensagem", "Data"])

        for i, chat in enumerate(chats_list, start=1):
            protocolo = chat.get("protocol")
            contato = chat.get("contact")
            for m in chat.get("messages", []):
                writer.writerow([protocolo, contato, m.get("text"), m.get("timestamp")])

            socketio.emit("progresso", {"atual": i, "total": total}, room=job_id)
            db_update_status(job_id, processed=i)

    size_bytes = os.path.getsize(out_path)
    db_update_status(job_id, status="done", filename=out_name, size_bytes=size_bytes)
    return out_path

def gerar_html(chats_list, job_id):
    out_name = f"{job_id}.html"
    out_path = os.path.join(EXPORT_DIR, out_name)

    total = len(chats_list)
    socketio.emit("inicio", {"total": total}, room=job_id)
    db_update_status(job_id, status="processing", total=total, processed=0)

    html_content = "<html><body>"

    for i, chat in enumerate(chats_list, start=1):
        protocolo = chat.get("protocol")
        contato = chat.get("contact")
        html_content += f"<h2>Protocolo: {protocolo} - Contato: {contato}</h2>"
        for m in chat.get("messages", []):
            html_content += f"<p>{m.get('timestamp')}: {m.get('text')}</p>"

        socketio.emit("progresso", {"atual": i, "total": total}, room=job_id)
        db_update_status(job_id, processed=i)

    html_content += "</body></html>"

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    size_bytes = os.path.getsize(out_path)
    db_update_status(job_id, status="done", filename=out_name, size_bytes=size_bytes)
    return out_path

# ================== Rota para Exportação ==================

@app.route("/exportar", methods=["POST"])
def exportar():
    job_id = str(uuid.uuid4())
    formato = request.form.get("formato", "pdf")
    filtros = request.form.to_dict()

    job_id = str(uuid.uuid4())
    descricao = f"Exportação ({formato.upper()})"
    db_insert_export(job_id, formato, descricao, json.dumps(filtros))

    def tarefa():
        try:
            chats_list = coletar_chats(filtros)

            if formato == "csv":
                gerar_csv(chats_list, job_id)
            elif formato == "html":
                gerar_html(chats_list, job_id)
            else:
                gerar_pdf(chats_list, job_id)

            socketio.emit("concluido", {"url": f"/exports/{job_id}/download"}, room=job_id)
        except Exception as e:
            socketio.emit("erro", {"mensagem": str(e)}, room=job_id)

    socketio.start_background_task(tarefa)
    return jsonify({"status": "started", "id": job_id}), 202

# ================== Rota para Download ==================

@app.route("/exports/<job_id>/download")
def download(job_id):
    file_path = os.path.join(EXPORT_DIR, f"{job_id}.pdf")  # ajuste conforme o formato
    return send_file(file_path, as_attachment=True)

# ================== Rodar o Flask ==================

if __name__ == "__main__":
    init_db()
    socketio.run(app, host="0.0.0.0", port=PORT)
