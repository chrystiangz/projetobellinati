# 📊 Call Center Analytics – CDR Aspect (29/07/2025)

Análise operacional de call center a partir de arquivos CDR (Call Detail Record) do discador **Aspect**, com foco em **qualidade de dados, métricas operacionais, SLA e insights acionáveis**.

Projeto desenvolvido como **Teste Prático – Analista Sênior de BI**.

---

## 📌 Objetivo

- Consolidar arquivos horários de CDR (08h–23h)
- Tratar e validar qualidade dos dados
- Calcular KPIs operacionais e SLAs
- Identificar anomalias de comportamento
- Disponibilizar dashboard analítico no Looker Studio

---

## 🗂️ Estrutura do Projeto

```text
.
├── BASES_RAW/                # Arquivos CDR originais
├── BASE_TRATADA/
│   ├── base_tratada.csv      # Base consolidada e tratada
│   └── relatorio_completo.xlsx
├── TRATA_DADOS.py            # Tratamento e validações
├── IMPORTADOR_BQ.py          # Carga para BigQuery
├── requirements.txt
└── README.md
```

---

## ⚙️ Regras de Negócio

- **Chamada atendida:** `AnswerDt IS NOT NULL`
- **Chamada não atendida:** ausência de `AnswerDt`
- **Ring time:** `AnswerDt - TimePhoneStartingRinging`
- **Talk time:** `WrapEndDt - AnswerDt`
- Tempos negativos são invalidados

### SLA

- **SLA ≤ 15s:** chamadas atendidas com ring ≤ 15s
- **SLA ≤ 30s:** chamadas atendidas com ring ≤ 30s

---

## 🔑 Chave Única e Observação Importante

```text
chave_unica = CallId + SeqNum
```

⚠️ **Foram identificados vários registros com o mesmo `CallId` associados a números discados (`DialedNum`) diferentes.**

Esse comportamento é inerente ao discador (rediscagens, tentativas automáticas e fluxos internos) e não representa erro de processamento.

**Por esse motivo:**

- `CallId` não é utilizado isoladamente como chave primária
- A combinação `CallId + SeqNum` garante unicidade lógica
- Duplicidades residuais são monitoradas como alerta de qualidade

---

## 🧪 Qualidade dos Dados

Validações automáticas incluem:

- Campos críticos ausentes
- Inconsistências temporais
- Duplicidade lógica
- Baixa taxa de preenchimento

Os resultados estão documentados no relatório técnico (`relatorio_completo.xlsx`).

---

## 🧠 Camada Analítica (BigQuery)

View criada:

```sql
SILVER.VW_CALLCENTER_KPIS
```

- Métricas por data, hora, grupo e disposition
- Cálculos defensivos (`SAFE_DIVIDE`, `NULLIF`)
- Pronta para consumo no Looker Studio

---

## 📊 Dashboard

O dashboard apresenta:

- KPIs gerais do dia
- Evolução horária de volume, taxa de atendimento e SLA
- Comparativos por Resource Group
- Distribuição por Disposition
- Detecção visual de anomalias
- Insights executivos e recomendações

---

## ▶️ Como Rodar o Projeto

### Pré-requisitos

- Python 3.9+
- Projeto no Google BigQuery
- Credenciais GCP (Service Account)
- Arquivos CDR disponíveis

### Instalação das Dependências

```bash
pip install pipreqs
pipreqs . --force
pip install -r requirements.txt
```

O arquivo `requirements.txt` está incluso na pasta do projeto.

### Execução

1. **Tratamento dos dados:**

```bash
python TRATA_DADOS.py
```

2. **Carga para o BigQuery:**

```bash
python IMPORTADOR_BQ.py
```

3. **Consultar a view:**

```sql
SELECT * FROM SILVER.VW_CALLCENTER_KPIS;
```

4. Abrir o dashboard no Looker Studio

---

## 📈 Principais Insights

- Pico de chamadas entre 10h–12h impacta o SLA
- Grupo FLOW concentra maior volume
- Alta incidência de chamadas sem atendimento humano
- Recomendado reforço operacional e ajuste de discagem

---

## 🏁 Considerações Finais

Projeto desenvolvido com foco em:

- Governança e rastreabilidade
- Qualidade e consistência dos dados
- Métricas confiáveis
- Comunicação executiva

Entrega alinhada ao nível Sênior de BI / Analytics.
