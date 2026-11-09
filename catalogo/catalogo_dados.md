# Catálogo de dados

Gerado pelo notebook `02_modelagem_carga` (seção 7), que confere por código se o catálogo contém exatamente as colunas e os tipos das tabelas gravadas. Também disponível em [`catalogo_dados.csv`](catalogo_dados.csv).

Regras R1 a R13: ver [README](../README.md#43-regras-de-transformação).

## `stg_precos`

| Atributo | Tipo | Descrição | Domínio | Obrigatório | Linhagem |
|---|---|---|---|---|---|
| `arquivo_origem` | STRING | Arquivo CSV de origem da linha | ca-AAAA-SS.csv | Sim | Metadado da leitura |
| `regiao` | STRING | Sigla da região do posto | N, NE, CO, SE, S | Sim | ANP, série semestral: Regiao - Sigla (R2) |
| `uf` | STRING | Sigla do estado do posto | 27 UFs | Sim | ANP, série semestral: Estado - Sigla (R2) |
| `municipio` | STRING | Município do posto | Texto em caixa alta | Sim | ANP, série semestral: Municipio (R2) |
| `cnpj` | STRING | CNPJ do posto, só dígitos | 14 dígitos | Sim | ANP, série semestral: CNPJ da Revenda (R3) |
| `produto` | STRING | Combustível pesquisado | GASOLINA, GASOLINA ADITIVADA, ETANOL, DIESEL, DIESEL S10, GNV | Sim | ANP, série semestral: Produto (R2) |
| `categoria` | STRING | Categoria do combustível; nula se o produto for desconhecido | Gasolina, Etanol, Diesel, GNV | Não | Derivada de produto (R9) |
| `unidade` | STRING | Unidade do preço | R$ / litro, R$ / m³ | Sim | ANP, série semestral: Unidade de Medida |
| `bandeira` | STRING | Distribuidora cuja marca o posto exibe | Texto; BRANCA = sem marca | Sim | ANP, série semestral: Bandeira (R2) |
| `data_coleta` | DATE | Data da coleta do preço; nula se inválida | jan/2024 a jun/2026 | Não | ANP, série semestral: Data da Coleta (R5) |
| `valor_venda` | DOUBLE | Preço de venda ao consumidor; nulo se ausente ou inválido | Número positivo | Não | ANP, série semestral: Valor de Venda (R4) |
| `motivo_exclusao` | STRING | Motivo de exclusão da fato; nulo se a linha é válida | CNPJ inválido, Local ausente, Data inválida, Produto desconhecido, Preço ausente ou inválido, Preço fora da faixa plausível | Não | Regras R6 e R7 |

## `dim_tempo`

| Atributo | Tipo | Descrição | Domínio | Obrigatório | Linhagem |
|---|---|---|---|---|---|
| `sk_data` | INT | Chave da data (AAAAMMDD) | 20240101 a 20260630 | Sim | Gerada (R12) |
| `data` | DATE | Data | Período da coleta | Sim | Gerada |
| `ano` | INT | Ano | 2024 a 2026 | Sim | Derivada de data |
| `semestre` | INT | Semestre do ano | 1, 2 | Sim | Derivada de data |
| `mes` | INT | Mês | 1 a 12 | Sim | Derivada de data |
| `inicio_semana` | DATE | Segunda-feira da semana da data | Data | Sim | Derivada de data |
| `dia_semana` | INT | Dia da semana | 1 (domingo) a 7 (sábado) | Sim | Derivada de data |

## `dim_local`

| Atributo | Tipo | Descrição | Domínio | Obrigatório | Linhagem |
|---|---|---|---|---|---|
| `sk_local` | INT | Chave do município | Inteiro sequencial | Sim | Gerada (R12) |
| `regiao` | STRING | Sigla da região | N, NE, CO, SE, S | Sim | stg_precos.regiao |
| `uf` | STRING | Sigla do estado | 27 UFs | Sim | stg_precos.uf |
| `municipio` | STRING | Município | Texto em caixa alta | Sim | stg_precos.municipio |

## `dim_produto`

| Atributo | Tipo | Descrição | Domínio | Obrigatório | Linhagem |
|---|---|---|---|---|---|
| `sk_produto` | INT | Chave do produto | Inteiro sequencial | Sim | Gerada (R12) |
| `produto` | STRING | Combustível | GASOLINA, GASOLINA ADITIVADA, ETANOL, DIESEL, DIESEL S10, GNV | Sim | stg_precos.produto |
| `categoria` | STRING | Categoria do combustível | Gasolina, Etanol, Diesel, GNV | Sim | R9 |
| `unidade` | STRING | Unidade do preço | R$ / litro, R$ / m³ | Sim | stg_precos.unidade |

## `dim_bandeira`

| Atributo | Tipo | Descrição | Domínio | Obrigatório | Linhagem |
|---|---|---|---|---|---|
| `sk_bandeira` | INT | Chave da bandeira | Inteiro sequencial | Sim | Gerada (R12) |
| `bandeira` | STRING | Distribuidora exibida pelo posto | Texto; BRANCA = sem marca | Sim | stg_precos.bandeira |
| `tipo_bandeira` | STRING | Posto com ou sem marca de distribuidora | Bandeira branca, Bandeirada | Sim | R10 |

## `dim_posto`

| Atributo | Tipo | Descrição | Domínio | Obrigatório | Linhagem |
|---|---|---|---|---|---|
| `sk_posto` | INT | Chave do posto | Inteiro sequencial | Sim | Gerada (R12) |
| `cnpj` | STRING | CNPJ do posto, só dígitos | 14 dígitos | Sim | stg_precos.cnpj (R3, R11) |
| `primeira_coleta` | DATE | Primeira data com preço válido do posto | Período da coleta | Sim | Derivada da fato |
| `ultima_coleta` | DATE | Última data com preço válido do posto | Período da coleta | Sim | Derivada da fato |
| `qtd_precos` | BIGINT | Quantidade de preços válidos do posto | Inteiro positivo | Sim | Derivada da fato |

## `fato_preco`

| Atributo | Tipo | Descrição | Domínio | Obrigatório | Linhagem |
|---|---|---|---|---|---|
| `sk_data` | INT | Chave da data da coleta | FK dim_tempo | Sim | stg_precos.data_coleta (R12) |
| `sk_posto` | INT | Chave do posto | FK dim_posto | Sim | stg_precos.cnpj |
| `sk_produto` | INT | Chave do produto | FK dim_produto | Sim | stg_precos.produto |
| `sk_local` | INT | Chave do município do posto | FK dim_local | Sim | stg_precos.uf, municipio |
| `sk_bandeira` | INT | Chave da bandeira vigente na data da coleta | FK dim_bandeira | Sim | stg_precos.bandeira |
| `valor_venda` | DOUBLE | Preço de venda ao consumidor (média, se houver duplicata) | R$ 2 a 15 (litro); R$ 1 a 15 (m³) | Sim | stg_precos.valor_venda (R6, R8) |
| `qtd_registros_origem` | INT | Quantidade de linhas de origem agregadas na linha | Inteiro >= 1 | Sim | R8 |
