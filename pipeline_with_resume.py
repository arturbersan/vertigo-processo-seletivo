#!/usr/bin/env python3
"""
Pipeline CAPES-DOI Final Corrigido
Implementa todas as correções e melhorias identificadas
"""

import asyncio
import aiohttp
import pandas as pd
import sqlite3
import json
import time
import os
from datetime import datetime
from pathlib import Path
from rapidfuzz import fuzz
from tqdm.asyncio import tqdm
import logging
import re

class CAPESDOIEnricherFinal:
    """
    Pipeline final com todas as correções implementadas
    """
    
    def __init__(self, cache_db='data/processed/cache.db'):
        self.base_url = "https://api.crossref.org/works"
        self.cache_db = cache_db
        self.max_concurrent = 50
        self.rate_limit = 80
        self.similarity_threshold = 90
        self.batch_size = 200
        self.checkpoint_interval = 1000
        
        # Filtros corrigidos - menos restritivos
        self.skip_patterns = [
            r'^[A-Z\s]{10,}$',     # Só maiúsculas muito longas
            r'^\d+\s*$',           # Só números
            r'^.{0,5}$',           # Muito curtos (5 chars ao invés de 10)
            r'^.{250,}$'           # Muito longos (250 ao invés de 200)
        ]
        
        self.setup_logging()
        self.setup_cache()
        self.session = None
        
    def should_skip_title(self, title):
        """Decide se deve pular um título - versão corrigida"""
        if pd.isna(title) or not title.strip():
            return True
            
        title_str = str(title).strip()
        
        # Aplicar filtros de padrões
        for pattern in self.skip_patterns:
            if re.match(pattern, title_str):
                return True
                
        # Filtros de português específico mantidos
        portuguese_indicators = ['OCORRÊNCIA DE', 'PREVALÊNCIA DE', 'ESTUDO DE CASO']
        if any(indicator in title_str.upper() for indicator in portuguese_indicators):
            return True
            
        return False
        
    def setup_logging(self):
        """Configuração de logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('logs/enrichment_final.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def setup_cache(self):
        """Configuração do cache e logs de progresso"""
        Path(self.cache_db).parent.mkdir(parents=True, exist_ok=True)
        
        conn = sqlite3.connect(self.cache_db)
        conn.execute('''
            CREATE TABLE IF NOT EXISTS crossref_cache (
                title_normalized TEXT PRIMARY KEY,
                doi TEXT,
                crossref_title TEXT,
                similarity_score REAL,
                found_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_title_normalized 
            ON crossref_cache(title_normalized)
        ''')
        
        conn.execute('''
            CREATE TABLE IF NOT EXISTS processing_progress (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT,
                records_processed INTEGER,
                target_size INTEGER,
                checkpoint_file TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT
            )
        ''')
        conn.commit()
        conn.close()
        
    def get_last_checkpoint(self, target_size):
        """Encontra o último checkpoint válido para o tamanho alvo"""
        checkpoint_files = list(Path('data/processed').glob(f'checkpoint_{target_size}_*.parquet'))
        
        if checkpoint_files:
            latest_checkpoint = max(checkpoint_files, key=lambda x: x.stat().st_mtime)
            filename = latest_checkpoint.stem
            records_processed = int(filename.split('_')[-1])
            
            self.logger.info(f"Checkpoint encontrado: {records_processed} registros processados")
            return latest_checkpoint, records_processed
        
        return None, 0
    
    def save_checkpoint(self, df, target_size, records_processed):
        """Salva checkpoint com identificação clara - versão corrigida"""
        checkpoint_file = f'data/processed/checkpoint_{target_size}_{records_processed}.parquet'
        df.to_parquet(checkpoint_file, index=False)
        
        # Registrar no banco
        session_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        conn = sqlite3.connect(self.cache_db)
        conn.execute('''
            INSERT INTO processing_progress 
            (session_id, records_processed, target_size, checkpoint_file, status)
            VALUES (?, ?, ?, ?, ?)
        ''', (session_id, records_processed, target_size, checkpoint_file, 'checkpoint'))
        conn.commit()
        conn.close()
        
        self.logger.info(f"Checkpoint salvo: {checkpoint_file}")
        
        # Limpar checkpoints antigos (manter apenas os últimos 3)
        old_checkpoints = sorted(
            Path('data/processed').glob(f'checkpoint_{target_size}_*.parquet'),
            key=lambda x: x.stat().st_mtime
        )[:-3]
        
        for old_checkpoint in old_checkpoints:
            old_checkpoint.unlink()
            
    def normalize_title(self, title):
        """Normalização de título"""
        if pd.isna(title) or title == '':
            return None
        return str(title).strip().upper()[:200]
        
    def get_cached_result(self, title):
        """Busca resultado no cache"""
        normalized = self.normalize_title(title)
        if not normalized:
            return None
            
        conn = sqlite3.connect(self.cache_db)
        cursor = conn.execute(
            'SELECT doi, crossref_title, similarity_score FROM crossref_cache WHERE title_normalized = ?',
            (normalized,)
        )
        result = cursor.fetchone()
        conn.close()
        
        if result:
            return {
                'doi': result[0],
                'crossref_title': result[1],
                'similarity_score': float(result[2]) if result[2] is not None else 0.0,
                'from_cache': True
            }
        return None
        
    def cache_result(self, title, doi, crossref_title, similarity_score):
        """Salva resultado no cache"""
        normalized = self.normalize_title(title)
        if not normalized:
            return
            
        conn = sqlite3.connect(self.cache_db)
        conn.execute('''
            INSERT OR REPLACE INTO crossref_cache 
            (title_normalized, doi, crossref_title, similarity_score)
            VALUES (?, ?, ?, ?)
        ''', (normalized, doi, crossref_title, similarity_score))
        conn.commit()
        conn.close()
        
    async def setup_session(self):
        """Configuração da sessão HTTP"""
        headers = {
            'User-Agent': 'CAPES-DOI-Enricher-Final/1.0 (educational-research@university.edu)',
            'Accept': 'application/json'
        }
        timeout = aiohttp.ClientTimeout(total=10, connect=5)
        connector = aiohttp.TCPConnector(
            limit=100,
            limit_per_host=50,
            keepalive_timeout=30
        )
        self.session = aiohttp.ClientSession(
            headers=headers, 
            timeout=timeout,
            connector=connector
        )
        
    async def close_session(self):
        """Fecha sessão HTTP"""
        if self.session:
            await self.session.close()
            
    async def search_crossref(self, title):
        """Busca no Crossref API"""
        try:
            params = {
                'query.title': title[:100],
                'rows': 3,
                'select': 'DOI,title'
            }
            
            async with self.session.get(self.base_url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    items = data.get('message', {}).get('items', [])
                    return self.find_best_match(title, items)
                elif response.status == 429:
                    await asyncio.sleep(1)
                    return None
                else:
                    return None
                    
        except:
            return None
            
    def find_best_match(self, original_title, crossref_items):
        """Encontra melhor match usando fuzzy logic"""
        if not crossref_items:
            return None
            
        original_clean = original_title.upper().strip()
        
        for item in crossref_items:
            crossref_title = item.get('title', [''])[0] if item.get('title') else ''
            if not crossref_title:
                continue
                
            similarity = fuzz.token_sort_ratio(
                original_clean,
                crossref_title.upper().strip()
            )
            
            if similarity >= self.similarity_threshold:
                return {
                    'doi': item.get('DOI'),
                    'crossref_title': crossref_title,
                    'similarity_score': similarity,
                    'from_cache': False
                }
                
        return None
        
    async def process_title(self, title, semaphore):
        """Processa um título individual - versão corrigida"""
        if self.should_skip_title(title):
            return None
        
        # Cache lookup fora do semáforo (não precisa de rate limiting)
        cached = self.get_cached_result(title)
        if cached:
            return cached
        
        # Apenas chamadas HTTP usam semáforo e rate limiting
        async with semaphore:
            # Rate limiting apenas para chamadas reais à API
            await asyncio.sleep(1.0 / self.rate_limit)
            
            result = await self.search_crossref(title)
            
            # Cache sempre, com ou sem resultado
            if result:
                self.cache_result(
                    title, 
                    result['doi'], 
                    result['crossref_title'], 
                    result['similarity_score']
                )
            else:
                self.cache_result(title, None, None, 0)
                
            return result
            
    async def process_batch(self, batch_df, batch_id):
        """Processa um lote de registros - versão corrigida"""
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        # Identificar títulos válidos
        valid_pairs = []
        for idx, row in batch_df.iterrows():
            title = row['NM_PRODUCAO']
            if not self.should_skip_title(title):
                valid_pairs.append((idx, title))
        
        self.logger.info(f"Lote {batch_id}: {len(valid_pairs)}/{len(batch_df)} títulos válidos")
        
        # Inicializar colunas de resultado (nomes consistentes em minúscula)
        batch_df['doi'] = None
        batch_df['crossref_title'] = None  
        batch_df['similarity_score'] = 0.0
        batch_df['from_cache'] = False
        
        if not valid_pairs:
            return batch_df
        
        # Processar títulos válidos
        tasks = [
            self.process_title(title, semaphore) 
            for idx, title in valid_pairs
        ]
        
        results = await tqdm.gather(*tasks, desc=f"Lote {batch_id}")
        
        # Aplicar resultados com validação de tipo
        for (idx, title), result in zip(valid_pairs, results):
            if result:
                batch_df.loc[idx, 'doi'] = result.get('doi')
                batch_df.loc[idx, 'crossref_title'] = result.get('crossref_title')
                
                # Validação segura do similarity_score
                score = result.get('similarity_score', 0)
                if score is not None:
                    try:
                        batch_df.loc[idx, 'similarity_score'] = float(score)
                    except (ValueError, TypeError):
                        batch_df.loc[idx, 'similarity_score'] = 0.0
                
                batch_df.loc[idx, 'from_cache'] = result.get('from_cache', False)
        
        found_dois = batch_df['doi'].notna().sum()
        self.logger.info(f"Lote {batch_id}: {found_dois}/{len(batch_df)} DOIs encontrados")
        
        return batch_df
        
    async def enrich_with_resume(self, df, target_size=10000):
        """
        Enriquece dataset com capacidade de resumo automático - versão corrigida
        """
        print(f"Iniciando enriquecimento para {target_size:,} registros")
        
        # Verificar se há checkpoint existente
        checkpoint_file, records_processed = self.get_last_checkpoint(target_size)
        
        if checkpoint_file and records_processed > 0:
            print(f"Resumindo do checkpoint: {records_processed:,} registros já processados")
            processed_df = pd.read_parquet(checkpoint_file)
            
            if len(processed_df) >= target_size:
                print(f"Meta de {target_size:,} registros já atingida!")
                return processed_df.head(target_size)
        else:
            print("Iniciando processamento do zero")
            processed_df = pd.DataFrame()
        
        await self.setup_session()
        
        try:
            remaining_records = target_size - len(processed_df)
            
            if remaining_records <= 0:
                return processed_df
            
            print(f"Processando mais {remaining_records:,} registros...")
            
            # Amostragem estratificada corrigida
            if 'AN_BASE' in df.columns:
                # Calcular registros por ano de forma balanceada
                years = df['AN_BASE'].unique()
                records_per_year = remaining_records // len(years)
                
                year_samples = []
                for year in years:
                    year_data = df[df['AN_BASE'] == year]
                    sample_size = min(len(year_data), records_per_year)
                    if sample_size > 0:
                        year_sample = year_data.sample(n=sample_size)
                        year_samples.append(year_sample)
                
                remaining_df = pd.concat(year_samples, ignore_index=True)
                
                # Ajustar para o tamanho exato se necessário
                if len(remaining_df) > remaining_records:
                    remaining_df = remaining_df.sample(n=remaining_records)
            else:
                remaining_df = df.sample(n=min(len(df), remaining_records))
            
            self.logger.info(f"Processando {len(remaining_df):,} novos registros")
            
            # Processar em lotes
            total_batches = (len(remaining_df) + self.batch_size - 1) // self.batch_size
            new_enriched_data = []
            
            for batch_id in range(total_batches):
                start_idx = batch_id * self.batch_size
                end_idx = min((batch_id + 1) * self.batch_size, len(remaining_df))
                
                batch_df = remaining_df.iloc[start_idx:end_idx].copy()
                enriched_batch = await self.process_batch(batch_df, batch_id)
                new_enriched_data.append(enriched_batch)
                
                # Checkpoint incremental corrigido
                current_total = len(processed_df) + sum(len(df) for df in new_enriched_data)
                if current_total % self.checkpoint_interval == 0 or batch_id == total_batches - 1:
                    # Tratar DataFrame vazio corretamente
                    if len(processed_df) > 0:
                        temp_df = pd.concat([processed_df] + new_enriched_data, ignore_index=True)
                    else:
                        temp_df = pd.concat(new_enriched_data, ignore_index=True)
                    
                    self.save_checkpoint(temp_df, target_size, current_total)
                
                # Progresso
                progress = ((batch_id + 1) / total_batches) * 100
                new_found = sum(df['doi'].notna().sum() for df in new_enriched_data)
                total_found = (processed_df['doi'].notna().sum() if len(processed_df) > 0 else 0) + new_found
                
                self.logger.info(
                    f"Progresso: {progress:.1f}% | Total DOIs: {total_found}"
                )
            
            # Combinar dados final corrigido
            if len(processed_df) > 0:
                final_df = pd.concat([processed_df] + new_enriched_data, ignore_index=True)
            else:
                final_df = pd.concat(new_enriched_data, ignore_index=True)
            
            # Salvar resultado final
            final_output = f'data/enriched/capes_enriched_{target_size}_final.parquet'
            final_df.to_parquet(final_output, index=False)
            
            return final_df
            
        finally:
            await self.close_session()

# Função principal
async def run_pipeline_final(target_size=10000):
    """
    Executa pipeline final corrigido
    """
    print(f"Pipeline CAPES-DOI Final Corrigido - {target_size:,} registros")
    print("="*60)
    
    # Carregar dados
    print("Carregando dados CAPES...")
    df1 = pd.read_csv('data/raw/capes_parte1.csv', encoding='ISO-8859-1', sep=';', low_memory=False)
    df2 = pd.read_csv('data/raw/capes_parte2.csv', encoding='ISO-8859-1', sep=';', low_memory=False)
    df_combined = pd.concat([df1, df2], ignore_index=True)
    
    print(f"Carregados {len(df_combined):,} registros")
    
    # Inicializar enricher
    enricher = CAPESDOIEnricherFinal()
    
    # Executar enriquecimento
    start_time = time.time()
    df_enriched = await enricher.enrich_with_resume(df_combined, target_size=target_size)
    end_time = time.time()
    
    duration = end_time - start_time
    
    # Estatísticas finais
    total_processed = len(df_enriched)
    dois_found = df_enriched['doi'].notna().sum()
    success_rate = (dois_found / total_processed) * 100 if total_processed > 0 else 0
    
    print(f"\nResultados finais:")
    print(f"Tempo: {duration/60:.1f} minutos")
    print(f"Processados: {total_processed:,}")
    print(f"DOIs encontrados: {dois_found:,}")
    print(f"Taxa de sucesso: {success_rate:.1f}%")
    print(f"Velocidade: {total_processed/duration:.1f} reg/seg")
    
    return df_enriched

if __name__ == "__main__":
    import sys
    
    target_size = int(sys.argv[1]) if len(sys.argv) > 1 else 10000
    
    asyncio.run(run_pipeline_final(target_size))
