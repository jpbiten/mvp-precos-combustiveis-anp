# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Coleta de dados: preços de combustíveis (ANP)
# MAGIC
# MAGIC **MVP - Preços de combustíveis no Brasil** | Universidade de Brasília - Engenharia de Produção - PSP8
# MAGIC **Autor:** João Pedro Ferreira Bitencourt (matrícula 211068047)
# MAGIC
# MAGIC Este notebook coleta e persiste na nuvem (Volume do Unity Catalog) a **Série Histórica de Preços de Combustíveis** da ANP
# MAGIC (Agência Nacional do Petróleo, Gás Natural e Biocombustíveis), no recorte de **combustíveis automotivos**
# MAGIC (gasolina, etanol, diesel e GNV), de **janeiro de 2024 a junho de 2026** (5 arquivos semestrais).
# MAGIC
# MAGIC - Os arquivos ZIP são gravados **exatamente como publicados** (camada raw). Nenhuma transformação acontece aqui.
# MAGIC - Ao final, um **manifesto** registra data, volume, formato, hash e uma amostra das primeiras linhas de cada arquivo.
# MAGIC - **Retomada:** se a execução for interrompida, basta rodar de novo; arquivos já baixados são reaproveitados.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração

# COMMAND ----------

import hashlib
import io
import json
import os
import time
import zipfile
from datetime import datetime, timezone

import requests

CATALOG = "workspace"
SCHEMA = "mvp_combustiveis"
VOLUME = "raw"
BASE = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

URL_BASE = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsas/ca/ca-{periodo}.zip"
PERIODOS = ["2024-01", "2024-02", "2025-01", "2025-02", "2026-01"]  # ano-semestre

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
for pasta in ("anp", "manifesto"):
    os.makedirs(f"{BASE}/{pasta}", exist_ok=True)
print("Destino:", BASE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Download dos arquivos semestrais

# COMMAND ----------


def baixar(url: str) -> bytes:
    """Baixa um arquivo com até 5 tentativas (espera crescente entre elas)."""
    for tentativa in range(1, 6):
        try:
            r = requests.get(url, timeout=180)
            if r.status_code == 200 and r.content[:2] == b"PK":  # assinatura de arquivo ZIP
                return r.content
            print(f"  HTTP {r.status_code} ({len(r.content)} bytes) - tentativa {tentativa}/5")
        except requests.RequestException as e:
            print(f"  {type(e).__name__} - tentativa {tentativa}/5")
        time.sleep(15 * tentativa)
    raise RuntimeError(f"Falha ao baixar {url}")


registros = []
for periodo in PERIODOS:
    url = URL_BASE.format(periodo=periodo)
    destino = f"{BASE}/anp/ca-{periodo}.zip"
    inicio = datetime.now(timezone.utc)
    if os.path.exists(destino):
        with open(destino, "rb") as f:
            conteudo = f.read()
        origem = "reaproveitado (já existia no Volume)"
    else:
        conteudo = baixar(url)
        with open(destino + ".tmp", "wb") as f:
            f.write(conteudo)
        os.replace(destino + ".tmp", destino)  # evita arquivo corrompido se a execução cair no meio
        origem = "baixado"
    registros.append({
        "periodo": periodo, "url": url, "arquivo": destino, "origem": origem,
        "coletado_em_utc": inicio.isoformat(timespec="seconds"),
        "bytes_zip": len(conteudo), "sha256": hashlib.sha256(conteudo).hexdigest(),
    })
    print(f"{periodo}: {origem}, {len(conteudo)/1e6:.1f} MB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Inspeção do conteúdo (formato, codificação e amostra)
# MAGIC
# MAGIC Lê cada ZIP sem alterá-lo: lista os arquivos internos, identifica a codificação do texto, conta as linhas e guarda as 3 primeiras linhas como amostra.

# COMMAND ----------


def detectar_codificacao(bruto: bytes) -> str:
    """UTF-8 (com ou sem BOM) se o arquivo inteiro for UTF-8 válido; caso contrário, Latin-1."""
    if bruto.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    try:
        bruto.decode("utf-8")
        return "utf-8"
    except UnicodeDecodeError:
        return "latin-1"


for reg in registros:
    with zipfile.ZipFile(reg["arquivo"]) as z:
        internos = [i for i in z.infolist() if not i.is_dir()]
        reg["arquivos_internos"] = [{"nome": i.filename, "bytes": i.file_size} for i in internos]
        csv_info = max(internos, key=lambda i: i.file_size)  # o CSV de dados é o maior arquivo do ZIP
        with z.open(csv_info) as f:
            bruto = f.read()
    cod = detectar_codificacao(bruto)
    texto = bruto.decode(cod, errors="replace")
    linhas = texto.splitlines()
    cabecalho = linhas[0] if linhas else ""
    reg.update({
        "csv": csv_info.filename,
        "bytes_csv": csv_info.file_size,
        "codificacao": cod,
        "separador": ";" if cabecalho.count(";") >= cabecalho.count(",") else ",",
        "linhas_de_dados": max(len(linhas) - 1, 0),
        "amostra": linhas[:3],
    })
    print(f"\n=== {reg['periodo']} | {reg['csv']} | {reg['bytes_csv']/1e6:.1f} MB | {cod} | "
          f"{reg['linhas_de_dados']} linhas de dados ===")
    for linha in reg["amostra"]:
        print(linha[:400])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Manifesto da coleta

# COMMAND ----------

manifesto = {
    "fonte": "ANP - Série Histórica de Preços de Combustíveis (combustíveis automotivos, dados semestrais)",
    "pagina": "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis",
    "gerado_em_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    "total_arquivos": len(registros),
    "total_bytes_zip": sum(r["bytes_zip"] for r in registros),
    "total_linhas_de_dados": sum(r["linhas_de_dados"] for r in registros),
    "arquivos": registros,
}
with open(f"{BASE}/manifesto/coleta_manifesto.json", "w", encoding="utf-8") as f:
    json.dump(manifesto, f, indent=2, ensure_ascii=False)

resumo = {k: v for k, v in manifesto.items() if k != "arquivos"}
print(json.dumps(resumo, indent=2, ensure_ascii=False))
for r in registros:
    print(f"{r['periodo']}: {r['linhas_de_dados']:>9} linhas | {r['bytes_zip']/1e6:5.1f} MB zip | sha256 {r['sha256'][:16]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Conferência final (evidência de persistência na nuvem)

# COMMAND ----------

display(dbutils.fs.ls(f"{BASE}/anp"))
display(dbutils.fs.ls(f"{BASE}/manifesto"))
