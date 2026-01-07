# 📊 Call Center Analytics – CDR Aspect (29/07/2025)

Análise operacional de Call Center a partir de arquivos CDR (*Call Detail Record*) do discador **Aspect**, com foco em **qualidade de dados, métricas operacionais, SLA e geração de insights acionáveis**.

Projeto desenvolvido como **Teste Prático – Analista Sênior de BI**.

---

## 📌 Objetivo

Transformar dados brutos de CDR em informações confiáveis e acionáveis, respondendo perguntas de negócio e suportando a tomada de decisão operacional.

Principais objetivos do projeto:

- Consolidar arquivos horários de CDR (08h–23h)
- Tratar e validar a qualidade dos dados
- Garantir tipagem correta e consistência temporal
- Calcular KPIs operacionais e SLAs
- Identificar comportamentos atípicos (anomalias)
- Disponibilizar visualizações analíticas no Looker Studio
- Gerar documentação executiva com conclusões e recomendações

---

## 🗂️ Estrutura do Projeto

```text
.
├── BASES_RAW/                    # Arquivos CDR brutos (obrigatório)
├── BASE_TRATADA/
│   ├── base_tratada.csv          # Base tratada
│   └── relatorio_completo.xlsx   # Relatório técnico (qualidade, anomalias e resumo)
├── ARQUIVOS/                     # Credenciais e arquivos sensíveis (obrigatório, fora do Git)
├── TRATA_DADOS.py                # Tratamento, validações e cálculos analíticos
├── IMPORTADOR_BQ.py              # Carga da base tratada no BigQuery
├── requirements.txt              # Arquivo com as bibliotecas necessárias para a execução dos cógigos
├── VW_CALLCENTER_KPIS.sql        # Arquivo contendo o código SQL utilizado para criar a view dentro do BigQuery
└── README.md
```

---

## ⚠️ Estrutura Obrigatória para Execução

Para execução correta do pipeline, é obrigatória a existência das seguintes pastas no diretório raiz:

### 📁 BASES_RAW

- Contém os arquivos CDR do Aspect, segmentados por hora.
- Formato CSV conforme especificação do enunciado.
- Dados totalmente brutos, sem qualquer pré-tratamento.

### 📁 ARQUIVOS

- Contém arquivos sensíveis e de configuração, como:
  - Credenciais do Google Cloud (Service Account)
  - Arquivos `.env`
- Por questões de segurança, não é versionada no GitHub.
- Disponibilizada apenas no `.zip` enviado junto ao teste.

📌 **Sem as pastas BASES_RAW e ARQUIVOS, o projeto não executa corretamente.**

---

## 🧩 Funcionamento dos Scripts

### 🔹 TRATA_DADOS.py

Script responsável por toda a lógica de negócio e validação dos dados.

**Principais responsabilidades:**

- Leitura e consolidação de todos os arquivos da pasta `BASES_RAW`
- Normalização de tipos (datas, numéricos e textos)
- Tratamento de valores ausentes e inconsistências
- Cálculo de métricas temporais:
  - Ring time
  - Talk time
  - Wrap time
  - Duração total da chamada
- Cálculo de SLAs (≤ 15s e ≤ 30s)
- Identificação de anomalias por hora e por grupo
- Geração dos artefatos finais:
  - `BASE_TRATADA/base_tratada.csv`
  - `BASE_TRATADA/relatorio_completo.xlsx`

📌 **Este script concentra engenharia de dados, regras de negócio e análise exploratória.**

### 🔹 IMPORTADOR_BQ.py

Script responsável pela persistência e governança dos dados no BigQuery.

**Principais responsabilidades:**

- Leitura da base tratada (`base_tratada.csv`)
- Criação ou recriação da tabela no BigQuery (camada Bronze)
- Detecção e aplicação de tipagem adequada
- Carga em chunks com estratégia defensiva
- Tratamento de erros e fallback seguro
- Suporte a notificações de execução (opcional)

📌 **Este script garante rastreabilidade, reprocessamento e integridade da carga.**

⚙️ **Configuração de notificações:** No início do arquivo, existe a variável DESTINATARIOS que deve ser preenchida com os e-mails que receberão notificações após a execução do script.

---

## ▶️ Como Executar o Projeto

### Pré-requisitos

- Python 3.9 ou superior
- Projeto configurado no Google BigQuery
- Credenciais GCP válidas (Service Account)
- Pastas `BASES_RAW` e `ARQUIVOS` corretamente configuradas

### Instalação das Dependências

Na pasta raiz do projeto, execute:

```bash
pip install pipreqs
pip install -r requirements.txt
```

O arquivo `requirements.txt` já está incluído no projeto e reflete exatamente as dependências utilizadas.

### Execução do Pipeline

1. **Tratamento e análise dos dados**

```bash
python TRATA_DADOS.py
```

2. **Carga da base tratada no BigQuery**

```bash
python IMPORTADOR_BQ.py
```

3. **Acesso ao dashboard no Looker Studio**

Link disponibilizado ao final deste documento

---

## ⚙️ Premissas e Regras de Negócio

### Definições Operacionais

- **Chamada atendida:** `AnswerDt IS NOT NULL`
- **Chamada não atendida:** `AnswerDt IS NULL`
- **Ring time:**
  ```
  AnswerDt - TimePhoneStartingRinging
  ```
- **Talk time:**
  ```
  WrapEndDt - AnswerDt
  ```
- **Wrap time:** período entre o término da chamada e o fim do atendimento
- **Registros com tempos negativos ou sequências temporais ilógicas:**
  - São excluídos das métricas
  - Permanecem registrados para análise de qualidade

### SLA

- **SLA ≤ 15s:** chamadas atendidas com ring time ≤ 15 segundos
- **SLA ≤ 30s:** chamadas atendidas com ring time ≤ 30 segundos
- O SLA é calculado exclusivamente sobre chamadas atendidas

---

## 🔑 Chave Lógica e Unicidade

```text
chave_lógica = CallId + SeqNum
```

Durante a análise, foram identificados casos de mesmo `CallId` associado a múltiplos `DialedNum`, comportamento típico de discadores automáticos (rediscagens e tentativas).

**Por esse motivo:**

- `CallId` não é utilizado isoladamente como chave
- A combinação `CallId + SeqNum` garante unicidade lógica
- Duplicidades residuais são tratadas como alerta de qualidade, não erro crítico

---

## 🧪 Qualidade dos Dados

São executadas validações automáticas para:

- Campos críticos ausentes
- Inconsistências temporais
- Duplicidade lógica
- Baixa taxa de preenchimento
- Validação de tipagem

Os resultados detalhados estão documentados em:

```
BASE_TRATADA/relatorio_completo.xlsx
```

**Conteúdo do relatório:**

- Estatísticas de preenchimento por campo
- Anomalias por hora e grupo
- Resumo executivo de qualidade
- Recomendações de melhoria

---

## 🧠 Camada Analítica (BigQuery)

Foi criada a view analítica:

```sql
SILVER.VW_CALLCENTER_KPIS
```

**Características da view:**

- Métricas consolidadas por data, hora, grupo e disposition
- Cálculos defensivos (`SAFE_DIVIDE`, `NULLIF`)
- Pronta para consumo direto no Looker Studio
- Consistência garantida sob qualquer filtro aplicado

---

## 📊 Dashboard

🔗 **Acessar Dashboard no Looker Studio**

https://lookerstudio.google.com/reporting/b2bee487-f876-4820-b8cf-bbaabd419a79

**O dashboard apresenta:**

- KPIs gerais do dia
- Evolução horária de chamadas, taxa de atendimento e SLA
- Comparativos por ResourceGroupDesc
- Distribuição por Disposition_Desc
- Identificação visual de anomalias
- Insights e recomendações acionáveis

**Funcionalidades:**

- Filtros interativos
- Séries temporais
- Comparativos lado a lado
- Destaque visual para métricas fora do padrão

---

## 📈 Principais Insights

- Pico de chamadas entre 10h–12h impacta negativamente o SLA
- O grupo FLOW concentra o maior volume de chamadas
- Alta incidência de chamadas sem atendimento humano
- Recomendação de reforço operacional nos horários críticos

---

## 🔭 Fora do Escopo e Próximos Passos

### Fora do escopo do teste:

- Análise por operador individual
- Correlação com campanhas ou conversão
- Modelos preditivos
- Análise multiday

### Possíveis evoluções:

- Automatização do pipeline
- Carga incremental diária
- Alertas automáticos de SLA
- Integração com dados de staffing (WFM)

---

## 🏁 Considerações Finais

Projeto desenvolvido com foco em:

- Qualidade e governança de dados
- Rastreabilidade e reprocessamento
- Métricas confiáveis e auditáveis
- Comunicação clara entre áreas técnicas e executivas
