# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Modelagem e carga (ETL em Spark): esquema estrela
# MAGIC
# MAGIC **MVP - Preços de combustíveis no Brasil** | Universidade de Brasília - Engenharia de Produção - PSP8
# MAGIC **Autor:** João Pedro Ferreira Bitencourt (matrícula 211068047)
# MAGIC
# MAGIC Lê os arquivos brutos da ANP gravados pelo notebook 01, aplica as regras de transformação e grava o modelo dimensional em tabelas Delta (Unity Catalog). Toda a transformação é feita em **Spark**, de forma distribuída.
# MAGIC
# MAGIC ## Modelo (esquema estrela)
# MAGIC
# MAGIC ```
# MAGIC                 dim_tempo
# MAGIC                     |
# MAGIC   dim_local ---- fato_preco ---- dim_produto
# MAGIC                 /         \
# MAGIC         dim_posto          dim_bandeira
# MAGIC ```
# MAGIC
# MAGIC - **Grão da fato:** um preço de venda de um produto, em um posto, em uma data de coleta.
# MAGIC - A **bandeira** fica na fato (e não no posto) porque um posto pode trocar de bandeira ao longo do tempo: a fato registra a bandeira vigente na data da coleta.
# MAGIC
# MAGIC ## Regras de transformação
# MAGIC
# MAGIC | # | Regra |
# MAGIC |---|---|
# MAGIC | R1 | **Leitura posicional:** as 16 colunas são lidas por posição, como texto, com separador `;` e codificação UTF-8. Os nomes originais (com acentos e espaços) são substituídos por nomes padronizados. |
# MAGIC | R2 | **Textos:** espaços nas pontas removidos e caixa alta em UF, município, produto e bandeira. |
# MAGIC | R3 | **CNPJ:** mantidos só os dígitos (o arquivo traz o CNPJ com e sem pontuação, conforme o semestre). Válido se tiver 14 dígitos. |
# MAGIC | R4 | **Preço:** vírgula decimal convertida em ponto; vira número. |
# MAGIC | R5 | **Data:** convertida de `dd/MM/aaaa` para `DATE`. |
# MAGIC | R6 | **Faixa plausível:** preços fora de R$ 2 a R$ 15 por litro (R$ 1 a R$ 15 por m³ no GNV) são marcados e excluídos da fato. |
# MAGIC | R7 | **Exclusão com motivo:** linhas com CNPJ inválido, UF ou município ausente, data inválida, preço ausente, preço fora da faixa ou produto desconhecido ficam no estágio com o motivo registrado e não entram na fato. |
# MAGIC | R8 | **Duplicatas:** mais de um preço para o mesmo posto, produto e data vira uma única linha com o preço médio; a quantidade original fica registrada. |
# MAGIC | R9 | **Categoria do produto:** GASOLINA e GASOLINA ADITIVADA = Gasolina; DIESEL e DIESEL S10 = Diesel; ETANOL = Etanol; GNV = GNV. |
# MAGIC | R10 | **Bandeira:** `BRANCA` = Bandeira branca (posto sem marca de distribuidora); demais = Bandeirada. |
# MAGIC | R11 | **Minimização de dados (LGPD):** nome do posto e endereço (rua, número, complemento, bairro e CEP) não são levados ao modelo; o posto é identificado só pelo CNPJ. A coluna Valor de Compra é descartada por vir vazia. |
# MAGIC | R12 | **Chaves:** `sk_data = AAAAMMDD`; demais chaves substitutas são sequenciais. |
# MAGIC | R13 | **Idempotência:** as tabelas são sobrescritas a cada execução. |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração

# COMMAND ----------

import os
import zipfile

import pandas as pd
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

CATALOG = "workspace"
SCHEMA = "mvp_combustiveis"
BASE = f"/Volumes/{CATALOG}/{SCHEMA}/raw"
DESTINO = f"{CATALOG}.{SCHEMA}"
CSV_DIR = f"{BASE}/anp_csv"

COLUNAS = ["regiao", "uf", "municipio", "revenda", "cnpj_original", "rua", "numero", "complemento", "bairro",
           "cep", "produto", "data_coleta_original", "valor_venda_original", "valor_compra_original", "unidade",
           "bandeira"]
CATEGORIA = {"GASOLINA": "Gasolina", "GASOLINA ADITIVADA": "Gasolina", "ETANOL": "Etanol",
             "DIESEL": "Diesel", "DIESEL S10": "Diesel", "GNV": "GNV"}
FAIXA = {"Gasolina": (2.0, 15.0), "Etanol": (2.0, 15.0), "Diesel": (2.0, 15.0), "GNV": (1.0, 15.0)}

print("Origem:", BASE, "| Destino:", DESTINO)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Extração
# MAGIC
# MAGIC Os ZIPs ficam intactos em `raw/anp`; os CSVs são extraídos para `raw/anp_csv` e lidos pelo Spark.

# COMMAND ----------

os.makedirs(CSV_DIR, exist_ok=True)
for nome in sorted(os.listdir(f"{BASE}/anp")):
    if not nome.endswith(".zip"):
        continue
    destino = f"{CSV_DIR}/{nome.replace('.zip', '.csv')}"
    if os.path.exists(destino):
        continue
    with zipfile.ZipFile(f"{BASE}/anp/{nome}") as z:
        maior = max((i for i in z.infolist() if not i.is_dir()), key=lambda i: i.file_size)
        with z.open(maior) as origem, open(destino + ".tmp", "wb") as saida:
            while bloco := origem.read(16 * 1024 * 1024):
                saida.write(bloco)
    os.replace(destino + ".tmp", destino)
    print("Extraído:", destino)

esquema = StructType([StructField(c, StringType(), True) for c in COLUNAS])  # R1
bruto = (spark.read.format("csv")
         .option("header", True).option("sep", ";").option("encoding", "UTF-8").option("quote", '"')
         .option("pathGlobFilter", "*.csv")
         .schema(esquema)
         .load(CSV_DIR)
         .select("*", F.col("_metadata.file_name").alias("arquivo_origem")))

print("Linhas lidas:", bruto.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Transformação: estágio com regras de qualidade

# COMMAND ----------

categoria = F.lit(None).cast("string")
for prod, cat in CATEGORIA.items():
    categoria = F.when(F.col("produto") == prod, cat).otherwise(categoria)
limpo = (bruto
    .withColumn("regiao", F.upper(F.trim("regiao")))                                    # R2
    .withColumn("uf", F.upper(F.trim("uf")))
    .withColumn("municipio", F.upper(F.trim("municipio")))
    .withColumn("produto", F.upper(F.trim("produto")))
    .withColumn("bandeira", F.upper(F.trim("bandeira")))
    .withColumn("unidade", F.trim("unidade"))
    .withColumn("cnpj", F.regexp_replace("cnpj_original", "[^0-9]", ""))                # R3
    .withColumn("valor_venda", F.expr("try_cast(replace(trim(valor_venda_original), ',', '.') AS DOUBLE)"))  # R4
    .withColumn("data_coleta", F.expr("CAST(try_to_timestamp(trim(data_coleta_original), 'dd/MM/yyyy') AS DATE)"))  # R5
    .withColumn("categoria", categoria)                                                  # R9
)

minimo = F.when(F.col("categoria") == "GNV", F.lit(FAIXA["GNV"][0])).otherwise(F.lit(FAIXA["Gasolina"][0]))
maximo = F.lit(15.0)
limpo = limpo.withColumn("motivo_exclusao",                                              # R6, R7
    F.when(F.col("cnpj").isNull() | (F.length("cnpj") != 14), "CNPJ inválido")
     .when(F.col("uf").isNull() | F.col("municipio").isNull(), "Local ausente")
     .when(F.col("data_coleta").isNull(), "Data inválida")
     .when(F.col("categoria").isNull(), "Produto desconhecido")
     .when(F.col("valor_venda").isNull(), "Preço ausente ou inválido")
     .when((F.col("valor_venda") < minimo) | (F.col("valor_venda") > maximo), "Preço fora da faixa plausível"))

# R11: o estágio já não carrega nome do posto nem endereço
stg_precos = limpo.select("arquivo_origem", "regiao", "uf", "municipio", "cnpj", "produto", "categoria",
                          "unidade", "bandeira", "data_coleta", "valor_venda", "motivo_exclusao")

display(stg_precos.groupBy("motivo_exclusao").count().orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Transformação: dimensões e fato

# COMMAND ----------

validos = stg_precos.filter(F.col("motivo_exclusao").isNull())

# R8: um preço por posto, produto e data
precos = (validos.groupBy("cnpj", "produto", "data_coleta")
          .agg(F.avg("valor_venda").alias("valor_venda"),
               F.count("*").alias("qtd_registros_origem"),
               F.max("bandeira").alias("bandeira"),
               F.max("regiao").alias("regiao"), F.max("uf").alias("uf"), F.max("municipio").alias("municipio"),
               F.max("categoria").alias("categoria"), F.max("unidade").alias("unidade")))


def com_chave(df, nome_chave, ordem):
    """R12: chave substituta sequencial, determinística pela ordenação informada."""
    return df.withColumn(nome_chave, F.row_number().over(Window.orderBy(*ordem)).cast("int"))


# dim_tempo: todos os dias do período, com a segunda-feira de cada semana
limites = precos.agg(F.min("data_coleta").alias("ini"), F.max("data_coleta").alias("fim")).first()
dim_tempo = (spark.sql(f"SELECT explode(sequence(DATE'{limites['ini']}', DATE'{limites['fim']}', INTERVAL 1 DAY)) AS data")
    .select(F.date_format("data", "yyyyMMdd").cast("int").alias("sk_data"), "data",
            F.year("data").alias("ano"),
            F.when(F.month("data") <= 6, 1).otherwise(2).alias("semestre"),
            F.month("data").alias("mes"),
            F.date_trunc("week", "data").cast("date").alias("inicio_semana"),
            F.dayofweek("data").alias("dia_semana")))

dim_local = com_chave(precos.select("regiao", "uf", "municipio").distinct(), "sk_local", ["uf", "municipio"]) \
    .select("sk_local", "regiao", "uf", "municipio")

dim_produto = com_chave(
    precos.groupBy("produto", "categoria").agg(F.max("unidade").alias("unidade")), "sk_produto", ["categoria", "produto"]
).select("sk_produto", "produto", "categoria", "unidade")

dim_bandeira = com_chave(precos.select("bandeira").distinct(), "sk_bandeira", ["bandeira"]) \
    .withColumn("tipo_bandeira", F.when(F.col("bandeira") == "BRANCA", "Bandeira branca").otherwise("Bandeirada")) \
    .select("sk_bandeira", "bandeira", "tipo_bandeira")                                  # R10

dim_posto = com_chave(
    precos.groupBy("cnpj").agg(F.min("data_coleta").alias("primeira_coleta"),
                               F.max("data_coleta").alias("ultima_coleta"),
                               F.count("*").alias("qtd_precos")),
    "sk_posto", ["cnpj"]
).select("sk_posto", "cnpj", "primeira_coleta", "ultima_coleta", "qtd_precos")

fato_preco = (precos
    .join(dim_local, ["regiao", "uf", "municipio"])
    .join(dim_produto.select("sk_produto", "produto"), "produto")
    .join(dim_bandeira.select("sk_bandeira", "bandeira"), "bandeira")
    .join(dim_posto.select("sk_posto", "cnpj"), "cnpj")
    .withColumn("sk_data", F.date_format("data_coleta", "yyyyMMdd").cast("int"))
    .select("sk_data", "sk_posto", "sk_produto", "sk_local", "sk_bandeira",
            F.round("valor_venda", 4).alias("valor_venda"), F.col("qtd_registros_origem").cast("int")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Carga (tabelas Delta no Unity Catalog)

# COMMAND ----------

TABELAS = {"stg_precos": stg_precos, "dim_tempo": dim_tempo, "dim_local": dim_local, "dim_produto": dim_produto,
           "dim_bandeira": dim_bandeira, "dim_posto": dim_posto, "fato_preco": fato_preco}
for nome, df in TABELAS.items():
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{DESTINO}.{nome}")
    print(f"{DESTINO}.{nome}: {spark.table(f'{DESTINO}.{nome}').count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Controle de qualidade depois da carga

# COMMAND ----------

T = {n: spark.table(f"{DESTINO}.{n}") for n in TABELAS}
total = T["stg_precos"].count()
excluidas = T["stg_precos"].filter(F.col("motivo_exclusao").isNotNull()).count()
validas = total - excluidas
f = T["fato_preco"]
qa = [
    ("Linhas lidas dos arquivos (estágio)", total, "2.134.685 (manifesto da coleta)"),
    ("Linhas excluídas por alguma regra (R6, R7)", excluidas, "informativo"),
    ("Linhas válidas", validas, "informativo"),
    ("Linhas da fato", f.count(), "linhas válidas menos duplicatas"),
    ("Duplicatas agregadas (R8)", validas - f.count(), "informativo"),
    ("Grão único (posto, produto, data) na fato",
     f.count() == f.select("sk_posto", "sk_produto", "sk_data").distinct().count(), "True"),
    ("Preço nulo na fato", f.filter(F.col("valor_venda").isNull()).count(), "0"),
]
for dim, chave in [("dim_tempo", "sk_data"), ("dim_posto", "sk_posto"), ("dim_produto", "sk_produto"),
                   ("dim_local", "sk_local"), ("dim_bandeira", "sk_bandeira")]:
    orfas = f.join(T[dim].select(chave), chave, "left_anti").count()
    qa.append((f"Chaves de {dim} órfãs na fato", orfas, "0"))
qa_df = pd.DataFrame([(t, str(r), str(e)) for t, r, e in qa], columns=["teste", "resultado", "esperado"])
display(qa_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Catálogo de dados (nome, tipo, descrição, domínio, obrigatoriedade, linhagem)

# COMMAND ----------

A = "ANP, série semestral"
CATALOGO = [
    # tabela, coluna, tipo, descricao, dominio, obrigatoria, linhagem
    ("stg_precos", "arquivo_origem", "STRING", "Arquivo CSV de origem da linha", "ca-AAAA-SS.csv", "Sim", "Metadado da leitura"),
    ("stg_precos", "regiao", "STRING", "Sigla da região do posto", "N, NE, CO, SE, S", "Sim", f"{A}: Regiao - Sigla (R2)"),
    ("stg_precos", "uf", "STRING", "Sigla do estado do posto", "27 UFs", "Sim", f"{A}: Estado - Sigla (R2)"),
    ("stg_precos", "municipio", "STRING", "Município do posto", "Texto em caixa alta", "Sim", f"{A}: Municipio (R2)"),
    ("stg_precos", "cnpj", "STRING", "CNPJ do posto, só dígitos", "14 dígitos", "Sim", f"{A}: CNPJ da Revenda (R3)"),
    ("stg_precos", "produto", "STRING", "Combustível pesquisado", "GASOLINA, GASOLINA ADITIVADA, ETANOL, DIESEL, DIESEL S10, GNV", "Sim", f"{A}: Produto (R2)"),
    ("stg_precos", "categoria", "STRING", "Categoria do combustível; nula se o produto for desconhecido", "Gasolina, Etanol, Diesel, GNV", "Não", "Derivada de produto (R9)"),
    ("stg_precos", "unidade", "STRING", "Unidade do preço", "R$ / litro, R$ / m³", "Sim", f"{A}: Unidade de Medida"),
    ("stg_precos", "bandeira", "STRING", "Distribuidora cuja marca o posto exibe", "Texto; BRANCA = sem marca", "Sim", f"{A}: Bandeira (R2)"),
    ("stg_precos", "data_coleta", "DATE", "Data da coleta do preço; nula se inválida", "jan/2024 a jun/2026", "Não", f"{A}: Data da Coleta (R5)"),
    ("stg_precos", "valor_venda", "DOUBLE", "Preço de venda ao consumidor; nulo se ausente ou inválido", "Número positivo", "Não", f"{A}: Valor de Venda (R4)"),
    ("stg_precos", "motivo_exclusao", "STRING", "Motivo de exclusão da fato; nulo se a linha é válida", "CNPJ inválido, Local ausente, Data inválida, Produto desconhecido, Preço ausente ou inválido, Preço fora da faixa plausível", "Não", "Regras R6 e R7"),
    ("dim_tempo", "sk_data", "INT", "Chave da data (AAAAMMDD)", "20240101 a 20260630", "Sim", "Gerada (R12)"),
    ("dim_tempo", "data", "DATE", "Data", "Período da coleta", "Sim", "Gerada"),
    ("dim_tempo", "ano", "INT", "Ano", "2024 a 2026", "Sim", "Derivada de data"),
    ("dim_tempo", "semestre", "INT", "Semestre do ano", "1, 2", "Sim", "Derivada de data"),
    ("dim_tempo", "mes", "INT", "Mês", "1 a 12", "Sim", "Derivada de data"),
    ("dim_tempo", "inicio_semana", "DATE", "Segunda-feira da semana da data", "Data", "Sim", "Derivada de data"),
    ("dim_tempo", "dia_semana", "INT", "Dia da semana", "1 (domingo) a 7 (sábado)", "Sim", "Derivada de data"),
    ("dim_local", "sk_local", "INT", "Chave do município", "Inteiro sequencial", "Sim", "Gerada (R12)"),
    ("dim_local", "regiao", "STRING", "Sigla da região", "N, NE, CO, SE, S", "Sim", "stg_precos.regiao"),
    ("dim_local", "uf", "STRING", "Sigla do estado", "27 UFs", "Sim", "stg_precos.uf"),
    ("dim_local", "municipio", "STRING", "Município", "Texto em caixa alta", "Sim", "stg_precos.municipio"),
    ("dim_produto", "sk_produto", "INT", "Chave do produto", "Inteiro sequencial", "Sim", "Gerada (R12)"),
    ("dim_produto", "produto", "STRING", "Combustível", "GASOLINA, GASOLINA ADITIVADA, ETANOL, DIESEL, DIESEL S10, GNV", "Sim", "stg_precos.produto"),
    ("dim_produto", "categoria", "STRING", "Categoria do combustível", "Gasolina, Etanol, Diesel, GNV", "Sim", "R9"),
    ("dim_produto", "unidade", "STRING", "Unidade do preço", "R$ / litro, R$ / m³", "Sim", "stg_precos.unidade"),
    ("dim_bandeira", "sk_bandeira", "INT", "Chave da bandeira", "Inteiro sequencial", "Sim", "Gerada (R12)"),
    ("dim_bandeira", "bandeira", "STRING", "Distribuidora exibida pelo posto", "Texto; BRANCA = sem marca", "Sim", "stg_precos.bandeira"),
    ("dim_bandeira", "tipo_bandeira", "STRING", "Posto com ou sem marca de distribuidora", "Bandeira branca, Bandeirada", "Sim", "R10"),
    ("dim_posto", "sk_posto", "INT", "Chave do posto", "Inteiro sequencial", "Sim", "Gerada (R12)"),
    ("dim_posto", "cnpj", "STRING", "CNPJ do posto, só dígitos", "14 dígitos", "Sim", "stg_precos.cnpj (R3, R11)"),
    ("dim_posto", "primeira_coleta", "DATE", "Primeira data com preço válido do posto", "Período da coleta", "Sim", "Derivada da fato"),
    ("dim_posto", "ultima_coleta", "DATE", "Última data com preço válido do posto", "Período da coleta", "Sim", "Derivada da fato"),
    ("dim_posto", "qtd_precos", "BIGINT", "Quantidade de preços válidos do posto", "Inteiro positivo", "Sim", "Derivada da fato"),
    ("fato_preco", "sk_data", "INT", "Chave da data da coleta", "FK dim_tempo", "Sim", "stg_precos.data_coleta (R12)"),
    ("fato_preco", "sk_posto", "INT", "Chave do posto", "FK dim_posto", "Sim", "stg_precos.cnpj"),
    ("fato_preco", "sk_produto", "INT", "Chave do produto", "FK dim_produto", "Sim", "stg_precos.produto"),
    ("fato_preco", "sk_local", "INT", "Chave do município do posto", "FK dim_local", "Sim", "stg_precos.uf, municipio"),
    ("fato_preco", "sk_bandeira", "INT", "Chave da bandeira vigente na data da coleta", "FK dim_bandeira", "Sim", "stg_precos.bandeira"),
    ("fato_preco", "valor_venda", "DOUBLE", "Preço de venda ao consumidor (média, se houver duplicata)", "R$ 2 a 15 (litro); R$ 1 a 15 (m³)", "Sim", "stg_precos.valor_venda (R6, R8)"),
    ("fato_preco", "qtd_registros_origem", "INT", "Quantidade de linhas de origem agregadas na linha", "Inteiro >= 1", "Sim", "R8"),
]
catalogo = pd.DataFrame(CATALOGO, columns=["tabela", "coluna", "tipo", "descricao", "dominio", "obrigatoria", "linhagem"])

# Conferência: o catálogo precisa conter exatamente as colunas das tabelas gravadas, com os mesmos tipos
real = {(n, c.name, c.dataType.simpleString().upper()) for n in TABELAS for c in T[n].schema.fields}
documentado = set(zip(catalogo["tabela"], catalogo["coluna"], catalogo["tipo"]))
assert real == documentado, f"Catálogo diverge. Faltam: {real - documentado} | Sobram: {documentado - real}"
print(f"Catálogo completo: {len(catalogo)} atributos em {catalogo['tabela'].nunique()} tabelas.")

for _, r in catalogo.iterrows():
    try:
        spark.sql(f"ALTER TABLE {DESTINO}.{r['tabela']} ALTER COLUMN {r['coluna']} COMMENT '{r['descricao'].replace(chr(39), '')}'")
    except Exception as e:  # o comentário é um extra; o CSV é o entregável
        print("Comentários de coluna não aplicados:", str(e)[:100])
        break

os.makedirs(f"{BASE}/catalogo", exist_ok=True)
catalogo.to_csv(f"{BASE}/catalogo/catalogo_dados.csv", index=False, encoding="utf-8")
display(catalogo)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Validação final (evidência)

# COMMAND ----------

display(spark.createDataFrame([(n, T[n].count()) for n in TABELAS], "tabela string, linhas long"))

# COMMAND ----------

# Amostra do modelo funcionando: preço médio da gasolina comum por região, por semestre
display(spark.sql(f"""
    SELECT l.regiao, t.ano, t.semestre, ROUND(AVG(f.valor_venda), 2) AS preco_medio, COUNT(*) AS coletas
    FROM {DESTINO}.fato_preco f
    JOIN {DESTINO}.dim_produto p ON f.sk_produto = p.sk_produto
    JOIN {DESTINO}.dim_local l ON f.sk_local = l.sk_local
    JOIN {DESTINO}.dim_tempo t ON f.sk_data = t.sk_data
    WHERE p.produto = 'GASOLINA'
    GROUP BY l.regiao, t.ano, t.semestre
    ORDER BY l.regiao, t.ano, t.semestre
"""))
