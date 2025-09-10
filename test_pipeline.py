#!/usr/bin/env python3
"""
Script de Teste - Pipeline com Amostra Pequena
Objetivo: Testar o pipeline antes de processar todos os dados
"""

import asyncio
import pandas as pd
import json
from main_pipeline import CAPESDOIEnricher

async def test_pipeline_sample():
    """
    Testa o pipeline com uma amostra pequena
    """
    print("🧪 TESTE DO PIPELINE COM AMOSTRA")
    print("="*50)
    
    # Carregar apenas uma pequena amostra
    print("📥 Carregando amostra...")
    df1 = pd.read_csv('data/raw/capes_parte1.csv', 
                     encoding='ISO-8859-1', 
                     sep=';', 
                     nrows=1000,  # Apenas 1000 registros para teste
                     low_memory=False)
    
    print(f"✅ {len(df1):,} registros carregados para teste")
    
    # Filtrar apenas registros com títulos válidos
    valid_df = df1[df1['NM_PRODUCAO'].notna() & (df1['NM_PRODUCAO'] != '')].head(50)
    print(f"🎯 Testando com {len(valid_df)} registros válidos")
    
    # Mostrar alguns títulos de exemplo
    print("\n📝 Títulos de exemplo que serão processados:")
    for i, title in enumerate(valid_df['NM_PRODUCAO'].head(5), 1):
        print(f"   {i}. {title[:80]}...")
    
    # Inicializar enricher com configurações de teste
    enricher = CAPESDOIEnricher(
        cache_db='data/processed/test_cache.db',
        max_concurrent=5  # Menor concorrência para teste
    )
    enricher.rate_limit = 10  # Mais conservador para teste
    enricher.batch_size = 10  # Lotes menores
    
    # Executar enriquecimento
    print("\n🔄 Iniciando enriquecimento de teste...")
    start_time = asyncio.get_event_loop().time()
    
    df_enriched = await enricher.enrich_dataset(valid_df)
    
    end_time = asyncio.get_event_loop().time()
    duration = end_time - start_time
    
    # Analisar resultados
    print("\n📊 RESULTADOS DO TESTE:")
    print("="*30)
    
    total_processed = len(df_enriched)
    dois_found = df_enriched['DOI'].notna().sum()
    success_rate = (dois_found / total_processed) * 100 if total_processed > 0 else 0
    
    print(f"⏱️  Tempo de execução: {duration:.1f} segundos")
    print(f"📊 Registros processados: {total_processed}")
    print(f"📊 DOIs encontrados: {dois_found}")
    print(f"📊 Taxa de sucesso: {success_rate:.1f}%")
    print(f"🚀 Velocidade: {total_processed/duration:.1f} registros/segundo")
    
    if dois_found > 0:
        avg_similarity = df_enriched[df_enriched['DOI'].notna()]['SIMILARITY_SCORE'].mean()
        print(f"📊 Similaridade média: {avg_similarity:.1f}%")
    
    # Mostrar alguns exemplos de sucesso
    successful_matches = df_enriched[df_enriched['DOI'].notna()].head(3)
    if len(successful_matches) > 0:
        print(f"\n✅ Exemplos de matches encontrados:")
        for idx, row in successful_matches.iterrows():
            print(f"\n   Original: {row['NM_PRODUCAO'][:60]}...")
            print(f"   Crossref: {row['CROSSREF_TITLE'][:60]}...")
            print(f"   DOI: {row['DOI']}")
            print(f"   Similaridade: {row['SIMILARITY_SCORE']:.1f}%")
    
    # Salvar resultado do teste
    test_output = 'data/processed/test_results.csv'
    df_enriched.to_csv(test_output, index=False, encoding='utf-8')
    print(f"\n💾 Resultados do teste salvos em: {test_output}")
    
    # Estimativa para dataset completo
    if duration > 0:
        total_records = 1162324  # Total conhecido
        estimated_hours = (total_records * duration) / (total_processed * 3600)
        print(f"\n🔮 ESTIMATIVA PARA DATASET COMPLETO:")
        print(f"   📊 Total de registros: {total_records:,}")
        print(f"   ⏱️  Tempo estimado: {estimated_hours:.1f} horas")
        print(f"   📊 DOIs estimados: {int(total_records * success_rate / 100):,}")
    
    return df_enriched

def analyze_test_results():
    """
    Analisa os resultados do teste salvo
    """
    try:
        df = pd.read_csv('data/processed/test_results.csv')
        
        print("\n📈 ANÁLISE DETALHADA DOS RESULTADOS:")
        print("="*40)
        
        # Distribuição de scores de similaridade
        similarity_scores = df[df['DOI'].notna()]['SIMILARITY_SCORE']
        if len(similarity_scores) > 0:
            print(f"📊 Distribuição de scores de similaridade:")
            print(f"   Mínimo: {similarity_scores.min():.1f}%")
            print(f"   Máximo: {similarity_scores.max():.1f}%")
            print(f"   Média: {similarity_scores.mean():.1f}%")
            print(f"   Mediana: {similarity_scores.median():.1f}%")
        
        # Cache usage
        cache_usage = df['FROM_CACHE'].sum() if 'FROM_CACHE' in df.columns else 0
        print(f"📊 Uso do cache: {cache_usage}/{len(df)} ({cache_usage/len(df)*100:.1f}%)")
        
        # Títulos não encontrados
        not_found = df[df['DOI'].isna()]
        if len(not_found) > 0:
            print(f"\n❌ Exemplos de títulos não encontrados:")
            for title in not_found['NM_PRODUCAO'].head(3):
                print(f"   • {title[:70]}...")
                
    except FileNotFoundError:
        print("❌ Arquivo de teste não encontrado. Execute o teste primeiro.")

async def main():
    """
    Executa teste e análise
    """
    # Executar teste
    await test_pipeline_sample()
    
    # Analisar resultados
    analyze_test_results()
    
    print("\n🎯 PRÓXIMOS PASSOS:")
    print("1. Se os resultados estão bons, execute o pipeline completo")
    print("2. Ajuste os parâmetros se necessário")
    print("3. Execute: python main_pipeline.py")

if __name__ == "__main__":
    asyncio.run(main())
