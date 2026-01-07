import warnings
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound
import re
import time
import os
from dotenv import load_dotenv 
from datetime import datetime
import csv
from pathlib import Path
import traceback
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import shutil
import tempfile
from tqdm import tqdm

# ==================== CONFIGURAÇÕES ==================== #
CSV_PATH = r"C:\Users\CHRYSTIAN.5654\Desktop\CÓDIGOS\TRATATIVA_DADOS_CALL\BASE_TRATADA\base_tratada.csv"
JSON_KEY_PATH = r"C:\Users\CHRYSTIAN.5654\Desktop\CÓDIGOS\TRATATIVA_DADOS_CALL\ARQUIVOS\chave_gcp.json"
EMAIL_ENV_PATH = r"C:\Users\CHRYSTIAN.5654\Desktop\CÓDIGOS\TRATATIVA_DADOS_CALL\ARQUIVOS\dados_email.env"
DESTINATARIOS = "guzchrystian@gmail.com;chrys.farias13@gmail.com"

PROJECT_ID = "projeto-bellinati"
DATASET_ID = "BRONZE"
TABLE_ID = "BASE_CALLCENTER"
LOCATION = "southamerica-east1"

CHUNK_SIZE = 500_000

# ==================== INICIALIZAÇÃO ==================== #
warnings.filterwarnings("ignore", category=UserWarning, module="pandas")

# BigQuery Client
credentials = service_account.Credentials.from_service_account_file(JSON_KEY_PATH)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

# Config do Email
load_dotenv(dotenv_path=EMAIL_ENV_PATH)
EMAIL_REMETENTE = os.getenv("EMAIL")
EMAIL_SENHA = os.getenv("CHAVE_ACESSO")

# Local do Cachê
BASE_DIR = Path(__file__).resolve().parent
CACHE_DIR = BASE_DIR / "cache_chunks"
CACHE_DIR.mkdir(exist_ok=True)

# ==================== FUNÇÕES AUXILIARES ==================== #

def normalize_column_name(name):
    """Normaliza nomes de colunas para padrão BigQuery"""
    return re.sub(r'\W+', '_', name).strip('_').upper()

def normalizar_nulos(df: pd.DataFrame) -> pd.DataFrame:
    """Remove valores vazios e substitui por None"""
    for col in df.columns:
        df[col] = df[col].replace(
            ["", " ", "None", "none", "NONE", "NaN", "nan", "NAN", "NaT", "nat", "NAT", "<NA>"],
            None
        )
    return df

def salvar_csv_seguro(df, temp_file):
    """Salva CSV com limpeza de caracteres especiais"""
    import unicodedata

    def limpar_texto(x):
        if pd.isna(x):
            return None
        x = str(x)
        x = x.replace('"', "'")
        x = x.replace("\n", " ").replace("\r", " ")
        x = "".join(ch for ch in x if unicodedata.category(ch)[0] != "C" or ch in ("\t", "\n"))
        return x.strip()

    for col in df.columns:
        if df[col].dtype == "object" or pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].map(limpar_texto)

    df.to_csv(
        temp_file,
        index=False,
        quoting=csv.QUOTE_ALL,
        quotechar='"',
        escapechar='\\',
        lineterminator='\n',
        encoding="utf-8"
    )

def enviar_email_notificacao(assunto, mensagem, sucesso=True):
    """Envia email de notificação"""
    try:
        msg = MIMEMultipart()
        msg['From'] = EMAIL_REMETENTE
        
        # Converte string de destinatários separados por ponto-e-vírgula em lista
        lista_destinatarios = [email.strip() for email in DESTINATARIOS.split(';') if email.strip()]
        msg['To'] = ', '.join(lista_destinatarios)
        
        msg['Subject'] = assunto

        corpo = f"""
        <html>
          <body>
            <h2>{'✅ Importação Concluída' if sucesso else '❌ Erro na Importação'}</h2>
            <p>{mensagem}</p>
            <hr>
            <p><small>Enviado automaticamente em {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</small></p>
          </body>
        </html>
        """
        
        msg.attach(MIMEText(corpo, 'html'))

        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(EMAIL_REMETENTE, EMAIL_SENHA)
            # Envia para todos os destinatários
            server.send_message(msg)
        
        print(f"📧 Email enviado com sucesso para: {', '.join(lista_destinatarios)}")
    except Exception as e:
        print(f"⚠️ Erro ao enviar email: {e}")

def detectar_tipos_colunas(df_sample):
    """Detecta tipos ideais para cada coluna baseado em amostra"""
    ajustes = {}
    
    for col in df_sample.columns:
        col_normalized = normalize_column_name(col)
        
        # Força STRING para campos desejados
        if any(termo in col_normalized for termo in ["CPF", "CNPJ", "EAN", "TELEFONE", "CEP"]):
            ajustes[col_normalized] = "STRING"
            continue
        
        serie = df_sample[col].dropna()
        
        if len(serie) == 0:
            ajustes[col_normalized] = "STRING"
            continue
        
        # Tenta converter para numérico
        try:
            serie_num = pd.to_numeric(serie, errors='raise')
            
            # Verifica se tem decimais
            if (serie_num % 1 != 0).any():
                ajustes[col_normalized] = "FLOAT64"
            else:
                MAX_INT64 = 9_223_372_036_854_775_807
                if serie_num.abs().max() > MAX_INT64:
                    ajustes[col_normalized] = "STRING"
                else:
                    ajustes[col_normalized] = "INT64"
            continue
        except:
            pass
        
        # Tenta converter para datetime
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                pd.to_datetime(serie, errors='raise')
            ajustes[col_normalized] = "DATETIME"
            continue
        except:
            pass
        
        # Tenta converter para date
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                serie_dt = pd.to_datetime(serie, errors='raise')
                if (serie_dt.dt.hour == 0).all() and (serie_dt.dt.minute == 0).all():
                    ajustes[col_normalized] = "DATE"
                    continue
        except:
            pass
        
        # Padrão: STRING
        ajustes[col_normalized] = "STRING"
    
    return ajustes

def gerar_schema_bq(colunas, tipos_detectados):
    """Gera schema do BigQuery"""
    schema = []
    for col in colunas:
        col_normalized = normalize_column_name(col)
        tipo = tipos_detectados.get(col_normalized, "STRING")
        schema.append(bigquery.SchemaField(col_normalized, tipo))
    return schema

def aplicar_tipos_no_df(df, tipos_detectados):
    """Aplica conversões de tipo no DataFrame"""
    df_converted = df.copy()
    
    for col in df.columns:
        col_normalized = normalize_column_name(col)
        tipo = tipos_detectados.get(col_normalized, "STRING")
        
        try:
            if tipo == "INT64":
                df_converted[col] = pd.to_numeric(df_converted[col], errors='coerce').astype('Int64')
            
            elif tipo == "FLOAT64":
                df_converted[col] = pd.to_numeric(df_converted[col], errors='coerce').astype('float64')
            
            elif tipo == "DATE":
                df_converted[col] = pd.to_datetime(df_converted[col], errors='coerce').dt.date
            
            elif tipo == "DATETIME":
                df_converted[col] = pd.to_datetime(df_converted[col], errors='coerce')
            
            else:  # STRING
                df_converted[col] = df_converted[col].astype(str).replace(['nan', 'None', 'NaT'], None)
        
        except Exception as e:
            print(f"⚠️ Erro ao converter '{col}' para {tipo}: {e}. Usando STRING.")
            df_converted[col] = df_converted[col].astype(str).replace(['nan', 'None', 'NaT'], None)
    
    return df_converted

# ==================== IMPORTAÇÃO PRINCIPAL ==================== #

def importar_csv_para_bigquery():
    """Função principal de importação"""
    inicio = datetime.now()
    
    print("=" * 70)
    print("IMPORTAÇÃO CSV → BIGQUERY")
    print("=" * 70)
    print(f"🕒 Execução iniciada em: {inicio.strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    try:
        # ========== 1. LEITURA E ANÁLISE DO CSV ========== #
        print(f"📂 Lendo CSV: {CSV_PATH}")
        
        # Lê primeiras 10k linhas para detectar tipos
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df_sample = pd.read_csv(CSV_PATH, nrows=10000, low_memory=False)
        print(f"✅ Amostra carregada: {len(df_sample):,} linhas, {len(df_sample.columns)} colunas")
        
        # Detecta tipos
        print("\n🔍 Detectando tipos de dados...")
        tipos_detectados = detectar_tipos_colunas(df_sample)
        
        print("\n📋 Tipos detectados:")
        for col, tipo in tipos_detectados.items():
            print(f"  - {col}: {tipo}")
        
        # Conta total de linhas
        print(f"\n📊 Contando linhas do arquivo...")
        total_linhas = sum(1 for _ in open(CSV_PATH, encoding='utf-8')) - 1
        print(f"ℹ️ Total de linhas: {total_linhas:,}")
        
        num_chunks = (total_linhas // CHUNK_SIZE) + 1
        print(f"📦 Serão processados {num_chunks} chunks de {CHUNK_SIZE:,} linhas")
        
        # ========== 2. PREPARAÇÃO BIGQUERY ========== #
        print(f"\n🔧 Preparando BigQuery...")
        
        # Cria dataset se não existir
        dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"
        try:
            client.get_dataset(dataset_ref)
            print(f"✅ Dataset '{DATASET_ID}' já existe")
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = LOCATION
            client.create_dataset(dataset)
            print(f"✅ Dataset '{DATASET_ID}' criado em {LOCATION}")
        
        # Deleta tabela se existir
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        try:
            client.get_table(table_ref)
            print(f"🗑️ Tabela '{table_ref}' encontrada. Deletando...")
            client.delete_table(table_ref)
            print(f"✅ Tabela deletada com sucesso")
        except NotFound:
            print(f"📄 Tabela '{TABLE_ID}' não existe (será criada)")
        
        # Cria tabela
        schema_bq = gerar_schema_bq(df_sample.columns, tipos_detectados)
        table = bigquery.Table(table_ref, schema=schema_bq)
        client.create_table(table)
        print(f"✅ Tabela '{table_ref}' criada no BigQuery")
        
        # ========== 3. CACHE LOCAL ========== #
        usar_cache = total_linhas > 1_000_000
        
        if usar_cache:
            print(f"\n💾 Criando cache local (arquivo grande: {total_linhas:,} linhas)")
            
            # Remove o cachê antigo
            if CACHE_DIR.exists():
                shutil.rmtree(CACHE_DIR)
            CACHE_DIR.mkdir(exist_ok=True)
            
            # Lê CSV em chunks e salva como parquet
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                reader = pd.read_csv(CSV_PATH, chunksize=CHUNK_SIZE, low_memory=False)
            
            for i, chunk in enumerate(reader):
                cache_file = CACHE_DIR / f"part-{i:05d}.parquet"
                chunk.to_parquet(cache_file, compression='snappy', index=False)
                print(f"  💾 Cache chunk {i+1}/{num_chunks} salvo")
            
            print("✅ Cache local criado")
        
        # ========== 4. PROCESSAMENTO PARALELO ========== #
        print(f"\nℹ️ A importação de {TABLE_ID} será feita em {total_linhas:,} linhas, divididas em {num_chunks} chunks")
        print("-" * 50)
        
        # FASE 1: Processamento paralelo
        print(f"🔄 FASE 1: Processamento paralelo de {num_chunks} chunk(s)...\n")
        
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        chunks_processados = {}  # Dicionário para armazenar chunks processados: {idx: dataframe}
        
        def processar_chunk(chunk_idx):
            """Processa um chunk e retorna o DataFrame processado"""
            barra_proc.write(f"➡️  Processando chunk {chunk_idx + 1}/{num_chunks}")
            
            # Carrega chunk
            if usar_cache:
                cache_file = CACHE_DIR / f"part-{chunk_idx:05d}.parquet"
                df = pd.read_parquet(cache_file)
            else:
                skiprows = chunk_idx * CHUNK_SIZE
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    df = pd.read_csv(CSV_PATH, skiprows=range(1, skiprows + 1), nrows=CHUNK_SIZE, low_memory=False)
            
            if df.empty:
                barra_proc.write(f"⚠️ Chunk {chunk_idx + 1} vazio")
                return chunk_idx, None
            
            # Normaliza colunas
            df.columns = [normalize_column_name(c) for c in df.columns]
            
            # Aplica tipos
            df = aplicar_tipos_no_df(df, tipos_detectados)
            
            # Normaliza nulos
            df = normalizar_nulos(df)
            
            barra_proc.write(f"✅ Chunk {chunk_idx + 1}/{num_chunks} processado ({len(df):,} linhas)")
            
            return chunk_idx, df
        
        # Barra de progresso para processamento
        barra_proc = tqdm(
            total=num_chunks,
            desc="📊 Processando",
            bar_format="{desc}: {bar} {percentage:3.0f}% ({n_fmt}/{total_fmt})",
            ncols=70
        )
        
        # Executa processamento em paralelo (4 workers)
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {executor.submit(processar_chunk, i): i for i in range(num_chunks)}
            
            for future in as_completed(futures):
                try:
                    chunk_idx, df = future.result()
                    chunks_processados[chunk_idx] = df
                    barra_proc.update(1)
                except Exception as e:
                    barra_proc.close()
                    print(f"\n❌ ERRO no processamento do chunk: {e}")
                    raise
        
        barra_proc.close()
        print(f"\n✅ Processamento paralelo concluído! {len(chunks_processados)} chunks prontos\n")
        
        # FASE 2: Upload sequencial
        print("-" * 50)
        print(f"📤 FASE 2: Upload sequencial para BigQuery...\n")
        
        barra_upload = tqdm(
            total=num_chunks,
            desc="📊 Enviando",
            bar_format="{desc}: {bar} {percentage:3.0f}% ({n_fmt}/{total_fmt})",
            ncols=70
        )
        
        # Envia chunks na ordem (0, 1, 2, 3...)
        for chunk_idx in range(num_chunks):
            df = chunks_processados.get(chunk_idx)
            
            if df is None:
                barra_upload.write(f"⚠️ Chunk {chunk_idx + 1} vazio, pulando...")
                barra_upload.update(1)
                continue
            
            try:
                barra_upload.write(f"📤 Enviando chunk {chunk_idx + 1}/{num_chunks} para BigQuery...")
                
                job_config = bigquery.LoadJobConfig(
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND
                )
                
                job = client.load_table_from_dataframe(
                    df, table_ref, 
                    job_config=job_config,
                    location=LOCATION
                )
                job.result()
                
                barra_upload.write(f"✅ Chunk {chunk_idx + 1}/{num_chunks} enviado com sucesso ({len(df):,} linhas)")
            
            except Exception as e:
                barra_upload.write(f"⚠️ Erro no upload DataFrame. Tentando fallback CSV...")
                
                # ========== FALLBACK CSV (caso dê erro) ========== #
                for col in df.columns:
                    df[col] = df[col].astype(str).replace(['nan', 'None', 'NaT', '<NA>'], None)
                
                # Recria tabela como STRING (apenas no primeiro chunk)
                if chunk_idx == 0:
                    client.delete_table(table_ref, not_found_ok=True)
                    schema_string = [bigquery.SchemaField(col, "STRING") for col in df.columns]
                    table = bigquery.Table(table_ref, schema=schema_string)
                    client.create_table(table)
                    barra_upload.write("♻️ Tabela recriada com tipos STRING para fallback")
                
                # Salva como CSV temporário
                with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode='w', encoding='utf-8') as tmp:
                    temp_path = tmp.name
                
                salvar_csv_seguro(df, temp_path)
                
                # Upload CSV
                with open(temp_path, "rb") as f:
                    job_config = bigquery.LoadJobConfig(
                        source_format=bigquery.SourceFormat.CSV,
                        skip_leading_rows=1,
                        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                        allow_quoted_newlines=True,
                        autodetect=False
                    )
                    
                    job = client.load_table_from_file(f, table_ref, job_config=job_config, location=LOCATION)
                    job.result()
                
                os.remove(temp_path)
                barra_upload.write(f"✅ Chunk {chunk_idx + 1} enviado via CSV fallback")
            
            barra_upload.update(1)
        
        barra_upload.close()
        
        # ========== 5. FINALIZAÇÃO ========== #
        fim = datetime.now()
        duracao = fim - inicio
        
        # Verifica tabela final
        table_final = client.get_table(table_ref)
        
        print("\n" + "-" * 50)
        print("🎉 Processo FINALIZADO com sucesso!")
        print(f"🕒 Horário de término: {fim.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⏳ Tempo total de execução: {duracao}")
        print("-" * 50)
        print(f"📊 Linhas importadas: {table_final.num_rows:,}")
        print(f"💾 Tamanho: {table_final.num_bytes / 1024 / 1024:.2f} MB")
        print(f"📍 Tabela: {table_ref}")
        print("-" * 50)
        
        # Envia email de sucesso
        mensagem = f"""
        <b>Tabela:</b> {table_ref}<br>
        <b>Linhas:</b> {table_final.num_rows:,}<br>
        <b>Tamanho:</b> {table_final.num_bytes / 1024 / 1024:.2f} MB<br>
        <b>Duração:</b> {duracao}
        """
        enviar_email_notificacao(
            f"✅ Importação Concluída - {TABLE_ID}",
            mensagem,
            sucesso=True
        )
        
        return True
    
    except Exception as e:
        fim = datetime.now()
        duracao = fim - inicio
        
        print("\n" + "-" * 50)
        print("❌ Processo FINALIZADO com ERROS!")
        print(f"🕒 Horário de término: {fim.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⏳ Tempo até falha: {duracao}")
        print("-" * 50)
        print(f"❌ Erro: {e}")
        print("-" * 50)
        traceback.print_exc()
        
        # Envia email de erro
        mensagem = f"""
        <b>Erro:</b> {str(e)}<br>
        <b>Arquivo:</b> {CSV_PATH}<br>
        <b>Duração até falha:</b> {duracao}<br>
        <br>
        <pre>{traceback.format_exc()}</pre>
        """
        enviar_email_notificacao(
            f"❌ Erro na Importação - {TABLE_ID}",
            mensagem,
            sucesso=False
        )
        
        return False
    
    finally:
        # Limpa cache
        if CACHE_DIR.exists():
            try:
                shutil.rmtree(CACHE_DIR)
                print("🧹 Cache limpo")
            except:
                pass

# ==================== EXECUÇÃO ==================== #

if __name__ == "__main__":
    importar_csv_para_bigquery()