#!/usr/bin/env python3
"""
Script para criar dataset reduzido e estratificado para análise
Executar: python create_sample_dataset.py
"""

import pandas as pd
import numpy as np
from pathlib import Path

def create_stratified_sample():
    """
    Cria amostra estratificada do dataset enriquecido
    """
    # Carregar dados
    input_file = "data/enriched/capes_enriched_10000_final.parquet"
    print(f"Carregando dados de: {input_file}")
    
    if not Path(input_file).exists():
        print(f"ERRO: Arquivo não encontrado: {input_file}")
        return
    
    df = pd.read_parquet(input_file)
    print(f"Dataset carregado: {len(df):,} registros")
    
    # Estatísticas iniciais
    total_with_doi = df['doi'].notna().sum()
    total_without_doi = df['doi'].isna().sum()
    
    print(f"\nEstatísticas do dataset completo:")
    print(f"Com DOI: {total_with_doi:,} ({total_with_doi/len(df)*100:.1f}%)")
    print(f"Sem DOI: {total_without_doi:,} ({total_without_doi/len(df)*100:.1f}%)")
    
    # Separar registros por categoria
    df_with_doi = df[df['doi'].notna()].copy()
    df_without_doi = df[df['doi'].isna()].copy()
    
    # Para registros com DOI, separar por faixa de similarity_score
    if len(df_with_doi) > 0:
        df_high_similarity = df_with_doi[df_with_doi['similarity_score'] >= 90].copy()
        df_medium_similarity = df_with_doi[
            (df_with_doi['similarity_score'] >= 80) & 
            (df_with_doi['similarity_score'] < 90)
        ].copy()
        
        print(f"\nDistribuição de similarity_score:")
        print(f"Score >= 90%: {len(df_high_similarity):,}")
        print(f"Score 80-89%: {len(df_medium_similarity):,}")
    else:
        df_high_similarity = pd.DataFrame()
        df_medium_similarity = pd.DataFrame()
    
    # Configurar tamanhos da amostra
    target_high_sim = 150    # Registros com alta similaridade (>=90%)
    target_medium_sim = 50   # Registros com similaridade média (80-89%)
    target_no_doi = 100      # Registros sem DOI
    target_total = target_high_sim + target_medium_sim + target_no_doi
    
    print(f"\nCriando amostra estratificada:")
    print(f"Target alta similaridade (>=90%): {target_high_sim}")
    print(f"Target média similaridade (80-89%): {target_medium_sim}")
    print(f"Target sem DOI: {target_no_doi}")
    print(f"Total target: {target_total}")
    
    samples = []
    
    # 1. Amostra com alta similaridade (estratificada por ano)
    if len(df_high_similarity) > 0:
        high_sim_sample = stratified_sample_by_year(
            df_high_similarity, 
            target_high_sim, 
            "alta similaridade"
        )
        samples.append(high_sim_sample)
    
    # 2. Amostra com similaridade média (se disponível)
    if len(df_medium_similarity) > 0:
        medium_sim_sample = stratified_sample_by_year(
            df_medium_similarity, 
            min(target_medium_sim, len(df_medium_similarity)),
            "média similaridade"
        )
        samples.append(medium_sim_sample)
    
    # 3. Amostra sem DOI
    if len(df_without_doi) > 0:
        no_doi_sample = stratified_sample_by_year(
            df_without_doi, 
            target_no_doi,
            "sem DOI"
        )
        samples.append(no_doi_sample)
    
    # Combinar todas as amostras
    if samples:
        final_sample = pd.concat(samples, ignore_index=True)
        
        # Shuffle para misturar os registros
        final_sample = final_sample.sample(frac=1, random_state=42).reset_index(drop=True)
        
        # Selecionar colunas essenciais para análise
        essential_columns = [
            'AN_BASE',
            'NM_PROGRAMA_IES', 
            'NM_ENTIDADE_ENSINO',
            'NM_PRODUCAO',
            'doi',
            'crossref_title',
            'similarity_score',
            'from_cache'
        ]
        
        # Adicionar colunas que existem no dataset
        available_columns = [col for col in essential_columns if col in final_sample.columns]
        final_sample_clean = final_sample[available_columns].copy()
        
        # Estatísticas da amostra
        print(f"\n" + "="*50)
        print(f"AMOSTRA CRIADA:")
        print(f"Total de registros: {len(final_sample_clean):,}")
        
        # Distribuição por ano
        if 'AN_BASE' in final_sample_clean.columns:
            year_dist = final_sample_clean['AN_BASE'].value_counts().sort_index()
            print(f"\nDistribuição por ano:")
            for year, count in year_dist.items():
                print(f"  {year}: {count}")
        
        # Distribuição de DOI
        sample_with_doi = final_sample_clean['doi'].notna().sum()
        sample_without_doi = final_sample_clean['doi'].isna().sum()
        
        print(f"\nDistribuição de DOI na amostra:")
        print(f"  Com DOI: {sample_with_doi} ({sample_with_doi/len(final_sample_clean)*100:.1f}%)")
        print(f"  Sem DOI: {sample_without_doi} ({sample_without_doi/len(final_sample_clean)*100:.1f}%)")
        
        # Distribuição de similarity_score
        if sample_with_doi > 0:
            scores = final_sample_clean[final_sample_clean['doi'].notna()]['similarity_score']
            print(f"\nEstatísticas de similarity_score (registros com DOI):")
            print(f"  Média: {scores.mean():.1f}")
            print(f"  Mediana: {scores.median():.1f}")
            print(f"  Min: {scores.min():.1f}")
            print(f"  Max: {scores.max():.1f}")
            
            high_scores = (scores >= 90).sum()
            medium_scores = ((scores >= 80) & (scores < 90)).sum()
            print(f"  Score >= 90%: {high_scores}")
            print(f"  Score 80-89%: {medium_scores}")
        
        # Top programas
        if 'NM_PROGRAMA_IES' in final_sample_clean.columns:
            top_programs = final_sample_clean['NM_PROGRAMA_IES'].value_counts().head(10)
            print(f"\nTop 10 programas na amostra:")
            for program, count in top_programs.items():
                print(f"  {program}: {count}")
        
        # Salvar amostra
        output_file = "data/enriched/capes_sample_analysis.parquet"
        Path(output_file).parent.mkdir(parents=True, exist_ok=True)
        final_sample_clean.to_parquet(output_file, index=False)
        
        print(f"\n" + "="*50)
        print(f"AMOSTRA SALVA EM: {output_file}")
        print(f"Pronta para análise!")
        
        return final_sample_clean
    
    else:
        print("ERRO: Não foi possível criar amostras")
        return None

def stratified_sample_by_year(df, target_size, category_name):
    """
    Cria amostra estratificada por ano
    """
    if len(df) == 0:
        return pd.DataFrame()
    
    if 'AN_BASE' not in df.columns:
        # Se não tem coluna de ano, fazer amostra simples
        sample_size = min(target_size, len(df))
        return df.sample(n=sample_size, random_state=42)
    
    years = df['AN_BASE'].unique()
    per_year = target_size // len(years)
    remainder = target_size % len(years)
    
    year_samples = []
    
    for i, year in enumerate(sorted(years)):
        year_data = df[df['AN_BASE'] == year]
        
        # Distribuir o remainder nos primeiros anos
        year_target = per_year + (1 if i < remainder else 0)
        sample_size = min(len(year_data), year_target)
        
        if sample_size > 0:
            year_sample = year_data.sample(n=sample_size, random_state=42)
            year_samples.append(year_sample)
    
    result = pd.concat(year_samples, ignore_index=True) if year_samples else pd.DataFrame()
    
    print(f"  {category_name}: {len(result)} registros selecionados")
    
    return result

if __name__ == "__main__":
    print("Criando dataset reduzido para análise...")
    print("="*50)
    
    sample_df = create_stratified_sample()
    
    if sample_df is not None:
        print(f"\n✅ Sucesso! Dataset reduzido criado.")
        print(f"Execute a análise com o arquivo: data/enriched/capes_sample_analysis.parquet")
    else:
        print(f"\n❌ Erro ao criar dataset reduzido.")
