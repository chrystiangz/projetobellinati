# 📊 Call Center Analytics – CDR Aspect (29/07/2025)

Análise operacional de call center a partir de arquivos CDR (*Call Detail Record*) do discador **Aspect**, com foco em **qualidade de dados, métricas operacionais, SLA e insights acionáveis**.

Projeto desenvolvido como **Teste Prático – Analista Sênior de BI**.

---

## 📌 Objetivo

Transformar os dados brutos do CDR em informações e indicadores acionáveis, respondendo perguntas de negócio e apresentando visualmente os resultados:

- Consolidar arquivos horários de CDR (08h–23h)
- Tratar e validar a qualidade dos dados
- Calcular KPIs operacionais e SLAs
- Identificar anomalias de comportamento
- Disponibilizar dashboard analítico no Looker Studio
- Gerar documento executivo com achados e recomendações

---

## 🗂️ Estrutura do Projeto

```text
.
├── BASES_RAW/                    # Arquivos CDR originais (obrigatório)
├── BASE_TRATADA/
│   ├── base_tratada.csv          # Base consolidada e tratada
│   └── relatorio_completo.xlsx   # Breve resumo com a análise dos dados filtrados
├── ARQUIVOS/                     # Credenciais e arquivos sensíveis (obrigatório, fora do Git)
├── TRATA_DADOS.py                # Tratamento, validações e análises
├── IMPORTADOR_BQ.py              # Carga da base tratada para o BigQuery
├── requirements.txt
└── README.md
```

---

## ⚠️ Estrutura Obrigatória para Execução

Para que o projeto funcione corretamente, é obrigatório que, na pasta onde os scripts são executados, existam as seguintes pastas:

### 📁 BASES_RAW

- Deve conter os arquivos CDR do Aspect, segmentados por hora.
- Os arquivos devem estar no formato CSV conforme especificação do enunciado.
- Essa pasta não contém tratamento prévio — os dados são brutos.

### 📁 ARQUIVOS

- Contém credenciais e configurações sensíveis, como:
  - Chave de acesso do Google Cloud (Service Account)
  - Arquivos `.env` com dados de e-mail/configuração
- Por motivos de segurança, essa pasta não está versionada no GitHub.
- Ela é disponibilizada apenas no arquivo `.zip` enviado junto ao teste.

📌 **Sem essas duas pastas (BASES_RAW e ARQUIVOS), o pipeline não executa corretamente.**

---

## 🧩 Funcionamento dos Scripts

### TRATA_DADOS.py

Responsável por:

- Ler todos os arquivos da pasta `BASES_RAW`
- Consolidar os dados em um único dataset
- Normalizar tipos, valores nulos e campos textuais
- Calcular métricas de tempo (ring, talk, duração)
- Calcular SLAs (15s e 30s)
- Detectar anomalias por hora e por grupo
- Gerar:
  - `base_tratada.csv`
  - `relatorio_completo.xlsx` (qualidade, tipagem, anomalias e resumo executivo)

📌 **Este script concentra a lógica de negócio e a validação dos dados.**

### IMPORTADOR_BQ.py

Responsável por:

- Ler a base tratada (`base_tratada.csv`)
- Criar (ou recriar) a tabela no BigQuery (camada Bronze)
- Detectar e aplicar tipagem adequada das colunas
- Realizar a carga em chunks (com fallback seguro para CSV)
- Enviar notificações de sucesso ou erro (opcional)

📌 **Este script garante persistência, rastreabilidade e governança no BigQuery.**

---

## ▶️ Como Rodar o Projeto

### Pré-requisitos

- Python 3.9+
- Projeto configurado no Google BigQuery
- Credenciais GCP válidas (Service Account)
- Pastas `BASES_RAW` e `ARQUIVOS` corretamente configuradas

### Instalação das Dependências

Na pasta raiz do projeto, execute:

```bash
pip install pipreqs
pipreqs . --force
pip install -r requirements.txt
```

O arquivo `requirements.txt` já está incluso no projeto e reflete as dependências utilizadas.

### Execução

1. **Tratamento e análise dos dados**

```bash
python TRATA_DADOS.py
```

2. **Carga da base tratada no BigQuery**

```bash
python IMPORTADOR_BQ.py
```

3. **Consulta da view analítica**

```sql
SELECT *
FROM SILVER.VW_CALLCENTER_KPIS;
```

4. **Abrir o dashboard no Looker Studio**

---

## ⚙️ Regras de Negócio

### Definições de Chamadas

- **Chamada atendida:** `AnswerDt IS NOT NULL`
- **Chamada não atendida:** ausência de `AnswerDt`
- **Ring time (tempo de toque):**
  ```
  AnswerDt - TimePhoneStartingRinging
  ```
- **Talk time (tempo de conversa):**
  ```
  WrapEndDt - AnswerDt
  ```
- **Wrap time (tempo de pós-atendimento):** período entre o fim da chamada e o fim do wrap
- Tempos negativos ou inconsistentes são invalidados.

### Métricas Calculadas

- **Total de chamadas realizadas** (com e sem atendimento)
- **Taxa de atendimento** por hora e por `ResourceGroupDesc`
- **Tempos médios:** ring, talk e wrap
- **Distribuição** por `Disposition_Desc`

### SLA

- **SLA ≤ 15s:** chamadas atendidas com ring ≤ 15 segundos
- **SLA ≤ 30s:** chamadas atendidas com ring ≤ 30 segundos

---

## 🔑 Chave Única e Observação Importante

```text
chave_unica = CallId + SeqNum
```

⚠️ **Foram identificados vários registros com o mesmo `CallId` associados a números discados (`DialedNum`) diferentes.**

Esse comportamento é inerente ao funcionamento do discador (rediscagens, tentativas automáticas e fluxos internos) e não representa erro de processamento.

**Por esse motivo:**

- `CallId` não é utilizado isoladamente como chave primária
- A combinação `CallId + SeqNum` garante unicidade lógica
- Duplicidades residuais são monitoradas como alerta de qualidade, não como erro crítico

---

## 🧪 Qualidade dos Dados

São executadas validações automáticas para:

- **Campos críticos ausentes:** verificação de campos obrigatórios não preenchidos
- **Inconsistências temporais:** detecção de tempos negativos ou sequências ilógicas
- **Duplicidade lógica:** identificação de registros duplicados
- **Baixa taxa de preenchimento:** campos com excesso de valores nulos
- **Validação de tipagem:** garantia de tipos corretos (datas, numéricos, textos)

Os resultados detalhados estão documentados no relatório técnico:

```
BASE_TRATADA/relatorio_completo.xlsx
```

Este relatório contém:
- Resumo de qualidade por campo
- Estatísticas de preenchimento
- Anomalias detectadas por hora e grupo
- Recomendações de tratamento

---

## 🧠 Camada Analítica (BigQuery)

Foi criada a view analítica:

```sql
SILVER.VW_CALLCENTER_KPIS
```

Essa view:

- Consolida métricas por data, hora, grupo e disposition
- Utiliza cálculos defensivos (`SAFE_DIVIDE`, `NULLIF`)
- Está pronta para consumo direto no Looker Studio
- Garante consistência sob qualquer filtro aplicado

---

## 📊 Dashboard

O dashboard final apresenta:

- **KPIs gerais do dia:** volume total, taxa de atendimento, SLA
- **Evolução horária:** volume de chamadas, taxa de atendimento e SLA ao longo do dia (08h–23h)
- **Comparativos por Resource Group:** distribuição e performance por grupo
- **Distribuição por Disposition:** análise dos códigos de disposição das chamadas
- **Detecção visual de anomalias:** horários ou grupos com comportamento atípico
- **Insights executivos e recomendações acionáveis**

🔗 **[Acessar Dashboard no Looker Studio](https://lookerstudio.google.com/reporting/b2bee487-f876-4820-b8cf-bbaabd419a79)**

### Funcionalidades do Dashboard

- Filtros interativos por hora, grupo e disposition
- Visualizações de série temporal para análise de tendências
- Comparativos lado a lado para análise de performance
- Alertas visuais para métricas fora do padrão

---

## 📈 Principais Insights

- Pico de chamadas entre 10h–12h impacta negativamente o SLA
- Grupo FLOW concentra o maior volume de chamadas
- Alta incidência de chamadas sem atendimento humano
- Recomenda-se reforço operacional e ajuste da estratégia de discagem

---

## 📋 Glossário de Campos

| Campo | Descrição |
|-------|-----------|
| **CallStartDt** | Data e horário de início da chamada |
| **SeqNum** | Código de integração da chamada |
| **CallId** | ID da chamada |
| **DetectionDt** | Horário quando a chamada foi detectada |
| **AnswerDt** | Horário em que a chamada foi respondida |
| **WrapEndDt** | Horário em que houve o fim do atendimento (NULL = sem atendimento) |
| **CallInsertDt** | Horário em que foi feito o registro da chamada no banco de dados |
| **CallEndDt** | Horário em que a chamada terminou |
| **TimePhoneStartingRinging** | Horário em que começou a ringar a chamada |
| **DialedNum** | Número discado |
| **Disp_c** | Código disposition da chamada |
| **Disposition_Desc** | Descrição do código de disposition da chamada |
| **ResourceGroupDesc** | Grupo de recursos utilizado na chamada |

---

## 🏁 Considerações Finais

Projeto desenvolvido com foco em:

- Governança e rastreabilidade
- Qualidade e consistência dos dados
- Métricas confiáveis e auditáveis
- Comunicação executiva orientada a decisão

Entrega alinhada ao nível Sênior de BI / Analytics.
