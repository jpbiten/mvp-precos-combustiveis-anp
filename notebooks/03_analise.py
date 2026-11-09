# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Análise: qualidade dos dados e respostas às perguntas
# MAGIC
# MAGIC **MVP - Preços de combustíveis no Brasil** | Universidade de Brasília - Engenharia de Produção - PSP8
# MAGIC **Autor:** João Pedro Ferreira Bitencourt (matrícula 211068047)
# MAGIC
# MAGIC | # | Pergunta |
# MAGIC |---|---|
# MAGIC | P1 | Quanto o preço da gasolina varia entre os estados, e quanto isso representa num tanque de 50 litros? |
# MAGIC | P2 | Em quais estados, e em que proporção das semanas, o etanol compensa mais que a gasolina (preço do etanol até 70% do da gasolina)? Isso mudou ao longo do tempo? |
# MAGIC | P3 | Quanto o preço varia entre postos de uma mesma cidade? Postos de bandeira branca são mais baratos que os de bandeira? |
# MAGIC
# MAGIC As agregações pesadas são feitas em Spark; só os resultados (tabelas pequenas) vão para o pandas, para exibição e gráficos. Tabelas (CSV) e gráficos (PNG) são salvos em `/Volumes/workspace/mvp_combustiveis/raw/resultados`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração

# COMMAND ----------

import json
import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "mvp_combustiveis"
DESTINO = f"{CATALOG}.{SCHEMA}"
SAIDA = f"/Volumes/{CATALOG}/{SCHEMA}/raw/resultados"
os.makedirs(SAIDA, exist_ok=True)

TANQUE = 50              # litros, tanque de referência de um carro de passeio
LIMITE_ETANOL = 0.70     # regra prática: etanol compensa se custar até 70% da gasolina
MIN_COLETAS_SEMANA = 5   # mínimo de preços por UF e semana para calcular a mediana semanal (P2)
MIN_POSTOS_CIDADE = 5    # mínimo de postos na cidade e semana para medir a dispersão (P3)
MIN_POSTOS_GRUPO = 2     # mínimo de postos de cada grupo (branca x bandeirada) na comparação pareada (P3)
RESUMO = {}

UFS = {"AC": "N", "AP": "N", "AM": "N", "PA": "N", "RO": "N", "RR": "N", "TO": "N",
       "AL": "NE", "BA": "NE", "CE": "NE", "MA": "NE", "PB": "NE", "PE": "NE", "PI": "NE", "RN": "NE", "SE": "NE",
       "DF": "CO", "GO": "CO", "MT": "CO", "MS": "CO",
       "ES": "SE", "MG": "SE", "RJ": "SE", "SP": "SE", "PR": "S", "RS": "S", "SC": "S"}


def mostrar(df: pd.DataFrame, nome: str = None):
    """Exibe a tabela e, se tiver nome, salva em CSV. Colunas de texto viram str para o display não falhar."""
    df = df.reset_index(drop=True).copy()
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].astype(str)
    if nome:
        df.to_csv(f"{SAIDA}/{nome}.csv", index=False, encoding="utf-8")
    try:
        display(df)
    except NameError:
        print(df.to_string())


def salvar_figura(fig, nome: str):
    fig.savefig(f"{SAIDA}/{nome}.png", dpi=150, bbox_inches="tight")
    plt.show()


spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW precos AS
    SELECT f.valor_venda, f.sk_posto, t.data, t.ano, t.semestre, t.mes, t.inicio_semana,
           p.produto, p.categoria, l.regiao, l.uf, l.municipio, b.bandeira, b.tipo_bandeira
    FROM {DESTINO}.fato_preco f
    JOIN {DESTINO}.dim_tempo t ON f.sk_data = t.sk_data
    JOIN {DESTINO}.dim_produto p ON f.sk_produto = p.sk_produto
    JOIN {DESTINO}.dim_local l ON f.sk_local = l.sk_local
    JOIN {DESTINO}.dim_bandeira b ON f.sk_bandeira = b.sk_bandeira
""")
print("Preços na fato:", spark.table("precos").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Qualidade dos dados por atributo

# COMMAND ----------

stg = spark.table(f"{DESTINO}.stg_precos")
total_stg = stg.count()
exprs = []
for c, tipo in stg.dtypes:
    exprs += [F.sum(F.col(c).isNull().cast("int")).alias(f"{c}__nulos"),
              F.countDistinct(c).alias(f"{c}__distintos")]
    if tipo in ("double", "date"):
        exprs += [F.min(c).cast("string").alias(f"{c}__min"), F.max(c).cast("string").alias(f"{c}__max")]
perfil_bruto = stg.agg(*exprs).first().asDict()
qualidade = pd.DataFrame([{
    "atributo": c, "tipo": tipo,
    "nulos": int(perfil_bruto[f"{c}__nulos"]),
    "pct_nulos": round(100 * perfil_bruto[f"{c}__nulos"] / total_stg, 3),
    "distintos": int(perfil_bruto[f"{c}__distintos"]),
    "minimo": str(perfil_bruto.get(f"{c}__min") or ""),
    "maximo": str(perfil_bruto.get(f"{c}__max") or ""),
} for c, tipo in stg.dtypes])
mostrar(qualidade, "qualidade_por_atributo")

exclusoes = (stg.groupBy("motivo_exclusao").count()
             .withColumn("pct", F.round(100 * F.col("count") / total_stg, 3))
             .orderBy(F.desc("count")).toPandas())
exclusoes["motivo_exclusao"] = exclusoes["motivo_exclusao"].fillna("(válida)")
mostrar(exclusoes, "qualidade_exclusoes")

# COMMAND ----------

# Verificações de domínio e consistência
validas = stg.filter(F.col("motivo_exclusao").isNull())
uf_regiao = validas.select("uf", "regiao").distinct().toPandas()
unidade = validas.groupBy("produto", "unidade").count().toPandas()
eh_m3 = unidade["unidade"].isin(["R$ / m³", "R$ / m3"])  # mesma unidade, duas grafias no arquivo
unid_esperada = (eh_m3 == (unidade["produto"] == "GNV"))
semanas = spark.table("precos").select("inicio_semana").distinct().count()
semanas_periodo = spark.table(f"{DESTINO}.dim_tempo").select("inicio_semana").distinct().count()
consistencia = pd.DataFrame([
    ("UFs fora da lista das 27 unidades da federação", int((~uf_regiao["uf"].isin(UFS)).sum())),
    ("UFs com região diferente da oficial", int((uf_regiao["uf"].map(UFS) != uf_regiao["regiao"]).sum())),
    ("Produtos com unidade incoerente (GNV em m³; demais em litro)", int(unidade.loc[~unid_esperada, "count"].sum())),
    ("Linhas de GNV com a grafia alternativa 'R$ / m3' (mesma unidade)", int(unidade.loc[unidade["unidade"] == "R$ / m3", "count"].sum())),
    ("UFs cobertas", int(uf_regiao["uf"].nunique())),
    ("Semanas do período sem nenhuma coleta", int(semanas_periodo - semanas)),
], columns=["verificacao", "resultado"])
mostrar(consistencia, "qualidade_consistencia")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. P1 - Variação do preço da gasolina entre estados
# MAGIC
# MAGIC Base: gasolina comum (`GASOLINA`), todo o período.

# COMMAND ----------

p1 = spark.sql("""
    SELECT uf, regiao, COUNT(*) AS coletas, COUNT(DISTINCT sk_posto) AS postos,
           ROUND(AVG(valor_venda), 3) AS preco_medio,
           ROUND(percentile_approx(valor_venda, 0.5), 3) AS preco_mediano,
           ROUND(percentile_approx(valor_venda, 0.1), 3) AS p10,
           ROUND(percentile_approx(valor_venda, 0.9), 3) AS p90
    FROM precos WHERE produto = 'GASOLINA'
    GROUP BY uf, regiao
""").toPandas().sort_values("preco_medio", ascending=False).reset_index(drop=True)
mais_barato = p1["preco_medio"].min()
p1["custo_tanque"] = (p1["preco_medio"] * TANQUE).round(2)
p1["a_mais_por_tanque_vs_uf_mais_barata"] = ((p1["preco_medio"] - mais_barato) * TANQUE).round(2)
mostrar(p1, "p1_gasolina_por_uf")

cara, barata = p1.iloc[0], p1.iloc[-1]
RESUMO.update({
    "P1_uf_mais_cara": f"{cara['uf']} (R$ {cara['preco_medio']:.2f})",
    "P1_uf_mais_barata": f"{barata['uf']} (R$ {barata['preco_medio']:.2f})",
    "P1_diferenca_por_litro": round(float(cara["preco_medio"] - barata["preco_medio"]), 2),
    "P1_diferenca_por_tanque": round(float((cara["preco_medio"] - barata["preco_medio"]) * TANQUE), 2),
    "P1_diferenca_pct": round(float(100 * (cara["preco_medio"] / barata["preco_medio"] - 1)), 1),
})

# Por região e por semestre (estabilidade do ranking ao longo do tempo)
p1_regiao = spark.sql("""
    SELECT regiao, ROUND(AVG(valor_venda), 3) AS preco_medio, COUNT(*) AS coletas
    FROM precos WHERE produto = 'GASOLINA' GROUP BY regiao ORDER BY preco_medio DESC
""").toPandas()
mostrar(p1_regiao, "p1_gasolina_por_regiao")

p1_sem = spark.sql("""
    SELECT uf, CONCAT(ano, '-S', semestre) AS periodo, ROUND(AVG(valor_venda), 3) AS preco_medio
    FROM precos WHERE produto = 'GASOLINA' GROUP BY uf, ano, semestre
""").toPandas().pivot(index="uf", columns="periodo", values="preco_medio")
ranks = p1_sem.rank(ascending=False)
RESUMO["P1_correlacao_ranking_primeiro_x_ultimo_semestre"] = round(
    float(ranks.iloc[:, 0].corr(ranks.iloc[:, -1])), 3)  # correlação de Spearman entre os rankings
mostrar(p1_sem.reset_index(), "p1_gasolina_uf_por_semestre")

# Contexto: evolução mensal do preço mediano nacional por combustível
p1_mensal = spark.sql("""
    SELECT make_date(ano, mes, 1) AS mes_ref, produto, ROUND(percentile_approx(valor_venda, 0.5), 3) AS mediana
    FROM precos WHERE produto IN ('GASOLINA', 'ETANOL', 'DIESEL S10')
    GROUP BY ano, mes, produto ORDER BY mes_ref
""").toPandas()
p1_mensal["mes_ref"] = pd.to_datetime(p1_mensal["mes_ref"])

# COMMAND ----------

fig, (a1, a2) = plt.subplots(1, 2, figsize=(14, 6.5), gridspec_kw={"width_ratios": [1.1, 1]})
ordem = p1.sort_values("preco_medio")
cores = ["#C44E52" if u == cara["uf"] else "#55A868" if u == barata["uf"] else "#4C72B0" for u in ordem["uf"]]
a1.barh(ordem["uf"], ordem["preco_medio"], color=cores)
a1.set_xlim(ordem["preco_medio"].min() * 0.9, ordem["preco_medio"].max() * 1.04)
for y, v in enumerate(ordem["preco_medio"]):
    a1.text(v, y, f" {v:.2f}".replace(".", ","), va="center", fontsize=7)
a1.set_title("P1 - Preço médio da gasolina comum por estado (R$/litro)")
a1.set_xlabel("R$ por litro (eixo não começa em zero)")
a1.spines[["top", "right"]].set_visible(False)

nomes = {"GASOLINA": "Gasolina comum", "ETANOL": "Etanol", "DIESEL S10": "Diesel S10"}
for prod, cor in [("GASOLINA", "#4C72B0"), ("ETANOL", "#55A868"), ("DIESEL S10", "#8172B3")]:
    d = p1_mensal[p1_mensal["produto"] == prod]
    a2.plot(d["mes_ref"], d["mediana"], marker="o", ms=3, color=cor, label=nomes[prod])
a2.set_title("Contexto - Preço mediano nacional por mês (R$/litro)")
a2.legend(frameon=False)
a2.spines[["top", "right"]].set_visible(False)
fig.autofmt_xdate()
salvar_figura(fig, "p1_gasolina_estados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. P2 - Quando o etanol compensa?
# MAGIC
# MAGIC Para cada UF e semana: razão entre o preço mediano do etanol e o da gasolina comum (mínimo de 5 coletas de cada). O etanol compensa quando a razão é de até 0,70.

# COMMAND ----------

semanal = spark.sql(f"""
    SELECT uf, regiao, inicio_semana, ano, semestre,
           percentile_approx(CASE WHEN produto = 'ETANOL' THEN valor_venda END, 0.5) AS etanol,
           percentile_approx(CASE WHEN produto = 'GASOLINA' THEN valor_venda END, 0.5) AS gasolina,
           SUM(CASE WHEN produto = 'ETANOL' THEN 1 ELSE 0 END) AS n_etanol,
           SUM(CASE WHEN produto = 'GASOLINA' THEN 1 ELSE 0 END) AS n_gasolina
    FROM precos WHERE produto IN ('ETANOL', 'GASOLINA')
    GROUP BY uf, regiao, inicio_semana, ano, semestre
""").toPandas()
semanal = semanal[(semanal["n_etanol"] >= MIN_COLETAS_SEMANA) & (semanal["n_gasolina"] >= MIN_COLETAS_SEMANA)].copy()
semanal["razao"] = semanal["etanol"] / semanal["gasolina"]
semanal["compensa"] = semanal["razao"] <= LIMITE_ETANOL

p2 = (semanal.groupby(["uf", "regiao"])
      .agg(semanas=("razao", "size"), pct_semanas_etanol_compensa=("compensa", "mean"),
           razao_mediana=("razao", "median"), razao_minima=("razao", "min"), razao_maxima=("razao", "max"))
      .reset_index().sort_values("pct_semanas_etanol_compensa", ascending=False))
p2["pct_semanas_etanol_compensa"] = (100 * p2["pct_semanas_etanol_compensa"]).round(1)
p2[["razao_mediana", "razao_minima", "razao_maxima"]] = p2[["razao_mediana", "razao_minima", "razao_maxima"]].round(3)
mostrar(p2, "p2_etanol_por_uf")

p2_sem = (semanal.assign(periodo=semanal["ano"].astype(str) + "-S" + semanal["semestre"].astype(str))
          .groupby(["uf", "periodo"])["compensa"].mean().mul(100).round(1).unstack())
mostrar(p2_sem.reset_index(), "p2_etanol_uf_por_semestre")
p2_evolucao = pd.DataFrame({
    "periodo": p2_sem.columns,
    "ufs_com_etanol_vantajoso_na_maioria_das_semanas": [(p2_sem[c] > 50).sum() for c in p2_sem.columns],
    "ufs_com_dados": [p2_sem[c].notna().sum() for c in p2_sem.columns],
})
mostrar(p2_evolucao, "p2_evolucao_semestral")

RESUMO.update({
    "P2_ufs_etanol_compensa_maioria_das_semanas": int((p2["pct_semanas_etanol_compensa"] > 50).sum()),
    "P2_ufs_etanol_nunca_compensa": int((p2["pct_semanas_etanol_compensa"] == 0).sum()),
    "P2_ufs_analisadas": int(len(p2)),
    "P2_pct_semanas_uf_compensa_total": round(float(100 * semanal["compensa"].mean()), 1),
})

# Razão nacional semanal (mediana de todos os postos do país)
nacional = spark.sql("""
    SELECT inicio_semana,
           percentile_approx(CASE WHEN produto = 'ETANOL' THEN valor_venda END, 0.5)
         / percentile_approx(CASE WHEN produto = 'GASOLINA' THEN valor_venda END, 0.5) AS razao
    FROM precos WHERE produto IN ('ETANOL', 'GASOLINA') GROUP BY inicio_semana ORDER BY inicio_semana
""").toPandas()
nacional["inicio_semana"] = pd.to_datetime(nacional["inicio_semana"])

# COMMAND ----------

fig, (a1, a2) = plt.subplots(1, 2, figsize=(14, 6.5), gridspec_kw={"width_ratios": [1, 1.1]})
ordem = p2.sort_values("pct_semanas_etanol_compensa")
a1.barh(ordem["uf"], ordem["pct_semanas_etanol_compensa"],
        color=["#55A868" if v > 50 else "#DD8452" if v > 0 else "#C44E52" for v in ordem["pct_semanas_etanol_compensa"]])
for y, v in enumerate(ordem["pct_semanas_etanol_compensa"]):
    a1.text(v, y, f" {v:.0f}%", va="center", fontsize=7)
a1.axvline(50, ls="--", color="grey", lw=0.8)
a1.set_xlim(0, 110)
a1.set_title("P2 - % das semanas em que o etanol compensou (razão ≤ 0,70)")
a1.set_xlabel("% das semanas analisadas")
a1.spines[["top", "right"]].set_visible(False)

a2.plot(nacional["inicio_semana"], nacional["razao"], color="#4C72B0", label="Brasil (mediana nacional)")
for uf, cor in [("SP", "#55A868"), ("RJ", "#C44E52")]:
    d = semanal[semanal["uf"] == uf].sort_values("inicio_semana")
    if len(d):
        a2.plot(pd.to_datetime(d["inicio_semana"]), d["razao"], color=cor, lw=1, alpha=0.8, label=uf)
a2.axhline(LIMITE_ETANOL, ls="--", color="black", lw=1)
a2.text(nacional["inicio_semana"].min(), LIMITE_ETANOL, " limite de 70%", va="bottom", fontsize=8)
a2.set_title("P2 - Razão preço etanol / gasolina por semana")
a2.legend(frameon=False, fontsize=8)
a2.spines[["top", "right"]].set_visible(False)
fig.autofmt_xdate()
salvar_figura(fig, "p2_etanol_gasolina")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. P3 - Diferença entre postos da mesma cidade e efeito da bandeira
# MAGIC
# MAGIC - **Dispersão:** para cada cidade e semana com pelo menos 5 postos com preço de gasolina comum, mede-se a diferença entre os 10% de postos mais caros e os 10% mais baratos (p90 - p10) e a economia de abastecer entre os 10% mais baratos em vez de no preço mediano. Usam-se percentis, e não o mínimo e o máximo, porque um único preço digitado errado distorce os extremos.
# MAGIC - **Bandeira:** comparação **pareada** dentro da mesma cidade e semana (pelo menos 2 postos de cada grupo), para não confundir o efeito da bandeira com o da localização.

# COMMAND ----------

cidade_semana = spark.sql(f"""
    SELECT uf, municipio, inicio_semana, COUNT(DISTINCT sk_posto) AS postos,
           MIN(valor_venda) AS minimo, MAX(valor_venda) AS maximo,
           percentile_approx(valor_venda, 0.5) AS mediana,
           percentile_approx(valor_venda, 0.1) AS p10, percentile_approx(valor_venda, 0.9) AS p90,
           STDDEV(valor_venda) / AVG(valor_venda) AS cv
    FROM precos WHERE produto = 'GASOLINA'
    GROUP BY uf, municipio, inicio_semana
    HAVING COUNT(DISTINCT sk_posto) >= {MIN_POSTOS_CIDADE}
""").toPandas()
cidade_semana["amplitude"] = cidade_semana["maximo"] - cidade_semana["minimo"]
cidade_semana["faixa_p10_p90"] = cidade_semana["p90"] - cidade_semana["p10"]
cidade_semana["economia_tanque_vs_mediana"] = (cidade_semana["mediana"] - cidade_semana["minimo"]) * TANQUE
# Medida robusta: comparar a mediana da cidade com o 10º percentil (os 10% de postos mais baratos), e não com o mínimo,
# que é sensível a um único preço digitado errado.
cidade_semana["economia_tanque_p10"] = (cidade_semana["mediana"] - cidade_semana["p10"]) * TANQUE

p3_dispersao = pd.DataFrame([{
    "cidades_semanas_analisadas": len(cidade_semana),
    "cidades_distintas": cidade_semana[["uf", "municipio"]].drop_duplicates().shape[0],
    "faixa_p10_p90_mediana_rs_litro (principal)": round(cidade_semana["faixa_p10_p90"].median(), 3),
    "faixa_p10_p90_p75_rs_litro": round(cidade_semana["faixa_p10_p90"].quantile(0.75), 3),
    "pct_com_faixa_p10_p90_de_30_centavos_ou_mais": round(100 * (cidade_semana["faixa_p10_p90"] >= 0.30).mean(), 1),
    "economia_mediana_por_tanque_mediana_x_p10 (principal)": round(cidade_semana["economia_tanque_p10"].median(), 2),
    "cv_mediano_pct": round(100 * cidade_semana["cv"].median(), 2),
    "amplitude_max_min_mediana_rs_litro (sensivel a extremos)": round(cidade_semana["amplitude"].median(), 3),
    "economia_mediana_por_tanque_mediana_x_minimo (teto, sensivel a extremos)": round(cidade_semana["economia_tanque_vs_mediana"].median(), 2),
}]).T.reset_index()
p3_dispersao.columns = ["indicador", "valor"]
mostrar(p3_dispersao, "p3_dispersao_resumo")
RESUMO["P3_faixa_p10_p90_mediana_rs_litro"] = float(round(cidade_semana["faixa_p10_p90"].median(), 3))
RESUMO["P3_economia_mediana_por_tanque_p10"] = float(round(cidade_semana["economia_tanque_p10"].median(), 2))

# Preços suspeitos: gasolina mais de 25% acima ou abaixo da mediana da própria cidade na semana
extremos = spark.sql("""
    WITH m AS (SELECT uf, municipio, inicio_semana, percentile_approx(valor_venda, 0.5) AS med
               FROM precos WHERE produto = 'GASOLINA' GROUP BY uf, municipio, inicio_semana)
    SELECT COUNT(*) AS precos_gasolina,
           SUM(CASE WHEN ABS(p.valor_venda / m.med - 1) > 0.25 THEN 1 ELSE 0 END) AS fora_de_25pct_da_mediana_local,
           MIN(p.valor_venda) AS menor_preco, MAX(p.valor_venda) AS maior_preco
    FROM precos p JOIN m ON p.uf = m.uf AND p.municipio = m.municipio AND p.inicio_semana = m.inicio_semana
    WHERE p.produto = 'GASOLINA'
""").toPandas()
extremos["pct_fora"] = (100 * extremos["fora_de_25pct_da_mediana_local"] / extremos["precos_gasolina"]).round(3)
mostrar(extremos, "p3_precos_extremos")

p3_capitais = (cidade_semana.groupby(["uf", "municipio"])
               .agg(semanas=("amplitude", "size"), postos_medio=("postos", "mean"),
                    faixa_p10_p90_mediana=("faixa_p10_p90", "median"),
                    economia_tanque_p10_mediana=("economia_tanque_p10", "median"),
                    amplitude_mediana=("amplitude", "median"))
               .reset_index().sort_values("postos_medio", ascending=False).head(15).round(2))
mostrar(p3_capitais, "p3_maiores_cidades")

# COMMAND ----------

pares = spark.sql(f"""
    WITH grupos AS (
        SELECT uf, municipio, inicio_semana, tipo_bandeira, bandeira, sk_posto, valor_venda
        FROM precos WHERE produto = 'GASOLINA'
    ),
    por_tipo AS (
        SELECT uf, municipio, inicio_semana, tipo_bandeira,
               percentile_approx(valor_venda, 0.5) AS mediana, COUNT(DISTINCT sk_posto) AS postos
        FROM grupos GROUP BY uf, municipio, inicio_semana, tipo_bandeira
    )
    SELECT b.uf, b.municipio, b.inicio_semana, b.mediana AS mediana_bandeirada, w.mediana AS mediana_branca
    FROM por_tipo b JOIN por_tipo w
      ON b.uf = w.uf AND b.municipio = w.municipio AND b.inicio_semana = w.inicio_semana
    WHERE b.tipo_bandeira = 'Bandeirada' AND w.tipo_bandeira = 'Bandeira branca'
      AND b.postos >= {MIN_POSTOS_GRUPO} AND w.postos >= {MIN_POSTOS_GRUPO}
""").toPandas()
pares["diferenca"] = pares["mediana_bandeirada"] - pares["mediana_branca"]

linhas = [{
    "comparacao": "Bandeiradas (todas) x branca",
    "pares_cidade_semana": len(pares),
    "diferenca_media_rs_litro": round(pares["diferenca"].mean(), 3),
    "diferenca_mediana_rs_litro": round(pares["diferenca"].median(), 3),
    "pct_pares_branca_mais_barata": round(100 * (pares["diferenca"] > 0).mean(), 1),
    "diferenca_media_por_tanque": round(pares["diferenca"].mean() * TANQUE, 2),
}]

# Por distribuidora: cada uma das 3 maiores bandeiras contra a branca, na mesma cidade e semana
top3 = (spark.sql("SELECT bandeira, COUNT(*) n FROM precos WHERE produto = 'GASOLINA' AND tipo_bandeira = 'Bandeirada' "
                  "GROUP BY bandeira ORDER BY n DESC LIMIT 3").toPandas()["bandeira"].tolist())
por_bandeira = spark.sql(f"""
    SELECT uf, municipio, inicio_semana, bandeira,
           percentile_approx(valor_venda, 0.5) AS mediana, COUNT(DISTINCT sk_posto) AS postos
    FROM precos WHERE produto = 'GASOLINA'
    GROUP BY uf, municipio, inicio_semana, bandeira
    HAVING COUNT(DISTINCT sk_posto) >= {MIN_POSTOS_GRUPO}
""").toPandas()
branca = por_bandeira[por_bandeira["bandeira"] == "BRANCA"][["uf", "municipio", "inicio_semana", "mediana"]]
for band in top3:
    d = por_bandeira[por_bandeira["bandeira"] == band].merge(branca, on=["uf", "municipio", "inicio_semana"],
                                                            suffixes=("", "_branca"))
    dif = d["mediana"] - d["mediana_branca"]
    linhas.append({
        "comparacao": f"{band} x branca", "pares_cidade_semana": len(d),
        "diferenca_media_rs_litro": round(dif.mean(), 3), "diferenca_mediana_rs_litro": round(dif.median(), 3),
        "pct_pares_branca_mais_barata": round(100 * (dif > 0).mean(), 1),
        "diferenca_media_por_tanque": round(dif.mean() * TANQUE, 2),
    })
p3_bandeira = pd.DataFrame(linhas)
mostrar(p3_bandeira, "p3_bandeira")

try:
    from scipy.stats import wilcoxon
    _, p_valor = wilcoxon(pares["diferenca"])
    RESUMO["P3_p_valor_wilcoxon_bandeirada_x_branca"] = float(p_valor)
    print(f"Teste de Wilcoxon pareado (bandeiradas x branca): p = {p_valor:.3g}")
except ImportError:
    print("scipy indisponível: teste não calculado.")
RESUMO["P3_diferenca_media_bandeirada_x_branca_rs_litro"] = float(p3_bandeira.loc[0, "diferenca_media_rs_litro"])
RESUMO["P3_pct_pares_branca_mais_barata"] = float(p3_bandeira.loc[0, "pct_pares_branca_mais_barata"])

# COMMAND ----------

fig, (a1, a2) = plt.subplots(1, 2, figsize=(15, 5.2), layout="constrained")
a1.hist(cidade_semana["faixa_p10_p90"].clip(upper=1.5), bins=40, color="#4C72B0")
a1.axvline(cidade_semana["faixa_p10_p90"].median(), color="#C44E52", ls="--",
           label=f"mediana: R\\$ {cidade_semana['faixa_p10_p90'].median():.2f}".replace(".", ","))
a1.set_title("P3 - Diferença de preço entre postos da mesma cidade\n(p90 - p10 da gasolina comum, por cidade e semana)")
a1.set_xlabel("R\\$ por litro (valores acima de R\\$ 1,50 agrupados em 1,50)")
a1.set_ylabel("cidades x semanas")
a1.legend(frameon=False)
a1.spines[["top", "right"]].set_visible(False)

d = p3_bandeira.iloc[::-1]
a2.barh(d["comparacao"], d["diferenca_media_rs_litro"], color=["#8172B3"] * (len(d) - 1) + ["#4C72B0"])
for y, v in enumerate(d["diferenca_media_rs_litro"]):
    a2.text(v, y, f" R\\$ {v:.2f}".replace(".", ","), va="center", fontsize=9)
a2.axvline(0, color="black", lw=0.8)
a2.set_title("P3 - Quanto o bandeirado cobra a mais que o de bandeira branca\n(mesma cidade e semana, R$/litro)")
a2.set_xlabel("diferença média (R$ por litro)")
a2.spines[["top", "right"]].set_visible(False)
salvar_figura(fig, "p3_postos_bandeira")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Resumo dos resultados (texto para copiar)
# MAGIC
# MAGIC Três células, uma por bloco, para facilitar a cópia.

# COMMAND ----------

with open(f"{SAIDA}/resumo.json", "w", encoding="utf-8") as f:
    json.dump(RESUMO, f, indent=2, ensure_ascii=False)
pd.set_option("display.width", 250)
pd.set_option("display.max_columns", 30)
print("RESUMO:", json.dumps(RESUMO, ensure_ascii=False))
for nome, t in [("QUALIDADE", qualidade), ("EXCLUSOES", exclusoes), ("CONSISTENCIA", consistencia),
                ("P1 POR UF", p1), ("P1 POR REGIAO", p1_regiao)]:
    print(f"\n=== {nome} ===")
    print(t.to_string(index=False))

# COMMAND ----------

for nome, t in [("P1 UF POR SEMESTRE", p1_sem.reset_index()), ("P2 POR UF", p2),
                ("P2 EVOLUCAO", p2_evolucao), ("P2 UF POR SEMESTRE (% semanas)", p2_sem.reset_index())]:
    print(f"\n=== {nome} ===")
    print(t.to_string(index=False))

# COMMAND ----------

for nome, t in [("P3 DISPERSAO", p3_dispersao), ("P3 PRECOS EXTREMOS", extremos),
                ("P3 MAIORES CIDADES", p3_capitais), ("P3 BANDEIRA", p3_bandeira)]:
    print(f"\n=== {nome} ===")
    print(t.to_string(index=False))
print("\nArquivos gerados:", sorted(os.listdir(SAIDA)))
