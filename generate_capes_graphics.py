#!/usr/bin/env python3
"""
Gerador dos 3 gráficos essenciais para apresentação CAPES-DOI
Gera 3 imagens separadas: grafico_1_taxa_sucesso.png, grafico_2_top_programas.png, grafico_3_similarity_scores.png
Execução: python generate_capes_graphics.py
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path

def setup_plot_style():
    """Configuração do estilo dos gráficos"""
    plt.style.use('default')
    sns.set_palette("husl")
    plt.rcParams['font.size'] = 12
    plt.rcParams['axes.titlesize'] = 14
    plt.rcParams['axes.labelsize'] = 12

def load_data():
    """Carrega e valida os dados"""
    data_path = "data/enriched/capes_enriched_10000_final.parquet"
    
    if not Path(data_path).exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {data_path}")
    
    df = pd.read_parquet(data_path)
    print(f"Dados carregados: {len(df):,} registros")
    print(f"Colunas disponíveis: {list(df.columns)}")
    
    return df

def grafico_1_taxa_sucesso(df):
    """
    Gráfico 1: Taxa de Sucesso Geral (Donut Chart)
    Salva como: grafico_1_taxa_sucesso.png
    """
    print("Gerando Gráfico 1: Taxa de Sucesso...")
    
    # Calcular dados
    total_registros = len(df)
    com_doi = df['doi'].notna().sum()
    sem_doi = total_registros - com_doi
    percentual_sucesso = (com_doi / total_registros) * 100
    
    # Dados para o gráfico
    labels = ['Com DOI', 'Sem DOI']
    sizes = [com_doi, sem_doi]
    colors = ['#2E8B57', '#DC143C']  # Verde e vermelho
    explode = (0.05, 0)  # Destacar fatia "Com DOI"
    
    # Criar figura
    fig, ax = plt.subplots(figsize=(10, 8))
    
    # Criar donut chart
    wedges, texts, autotexts = ax.pie(
        sizes, 
        labels=labels, 
        colors=colors,
        autopct='%1.1f%%',
        startangle=90,
        explode=explode,
        textprops={'fontsize': 14, 'fontweight': 'bold'}
    )
    
    # Criar o "buraco" do donut
    centre_circle = plt.Circle((0,0), 0.70, fc='white')
    fig.gca().add_artist(centre_circle)
    
    # Adicionar texto no centro
    ax.text(0, 0.1, f'{total_registros:,}', 
            horizontalalignment='center', fontsize=20, fontweight='bold')
    ax.text(0, -0.1, 'Registros\nProcessados', 
            horizontalalignment='center', fontsize=12)
    
    # Adicionar números absolutos
    ax.text(0, -0.3, f'DOIs: {com_doi:,}', 
            horizontalalignment='center', fontsize=14, color='#2E8B57', fontweight='bold')
    
    # Título
    plt.title('Taxa de Sucesso na Obtenção de DOIs\nPipeline CAPES-Crossref', 
              fontsize=16, fontweight='bold', pad=20)
    
    # Salvar
    plt.tight_layout()
    plt.savefig('output/grafico_1_taxa_sucesso.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"✓ Gráfico 1 salvo: Taxa de sucesso {percentual_sucesso:.1f}% ({com_doi:,}/{total_registros:,})")

def grafico_2_top_programas(df):
    """
    Gráfico 2: Top 10 Programas com Mais DOIs (Barras Horizontais)
    Salva como: grafico_2_top_programas.png
    """
    print("Gerando Gráfico 2: Top Programas...")
    
    # Filtrar apenas registros com DOI e contar por programa
    df_com_doi = df[df['doi'].notna()]
    top_programas = df_com_doi.groupby('NM_PROGRAMA_IES').size().nlargest(10)
    
    # Criar figura
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Criar gráfico de barras horizontais
    bars = ax.barh(range(len(top_programas)), top_programas.values, 
                   color='#1f77b4', alpha=0.8)
    
    # Configurar eixos
    ax.set_yticks(range(len(top_programas)))
    ax.set_yticklabels(top_programas.index, fontsize=11)
    ax.set_xlabel('Número de DOIs Encontrados', fontsize=12, fontweight='bold')
    ax.set_title('Top 10 Programas com Mais DOIs Encontrados\nDistribuição por Área de Conhecimento', 
                 fontsize=14, fontweight='bold', pad=20)
    
    # Adicionar valores nas barras
    for i, (bar, value) in enumerate(zip(bars, top_programas.values)):
        ax.text(value + 1, bar.get_y() + bar.get_height()/2, 
                f'{value}', va='center', fontweight='bold')
    
    # Inverter ordem (maior no topo)
    ax.invert_yaxis()
    
    # Grid para facilitar leitura
    ax.grid(axis='x', alpha=0.3)
    
    # Ajustar layout
    plt.tight_layout()
    plt.savefig('output/grafico_2_top_programas.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"✓ Gráfico 2 salvo: Top programa '{top_programas.index[0]}' com {top_programas.iloc[0]} DOIs")

def grafico_3_similarity_scores(df):
    """
    Gráfico 3: Distribuição de Similarity Scores (Histograma)
    Salva como: grafico_3_similarity_scores.png
    """
    print("Gerando Gráfico 3: Similarity Scores...")
    
    # Filtrar apenas registros com DOI (que têm similarity_score)
    df_com_doi = df[df['doi'].notna()]
    scores = df_com_doi['similarity_score']
    
    # Estatísticas básicas
    score_medio = scores.mean()
    score_mediano = scores.median()
    total_scores = len(scores)
    
    # Criar figura
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # Criar histograma
    n_bins = 20
    counts, bins, patches = ax.hist(scores, bins=n_bins, color='#2E8B57', 
                                    alpha=0.7, edgecolor='black', linewidth=0.5)
    
    # Destacar área acima do threshold (90%)
    threshold = 90
    for i, (count, bin_left, bin_right, patch) in enumerate(zip(counts, bins[:-1], bins[1:], patches)):
        if bin_left >= threshold:
            patch.set_color('#228B22')  # Verde mais escuro para scores >= 90%
        else:
            patch.set_color('#DC143C')  # Vermelho para scores < 90%
    
    # Adicionar linha vertical no threshold
    ax.axvline(threshold, color='red', linestyle='--', linewidth=2, 
               label=f'Threshold: {threshold}%')
    
    # Adicionar linha vertical na média
    ax.axvline(score_medio, color='orange', linestyle='-', linewidth=2,
               label=f'Média: {score_medio:.1f}%')
    
    # Configurar eixos e título
    ax.set_xlabel('Similarity Score (%)', fontsize=12, fontweight='bold')
    ax.set_ylabel('Número de Registros', fontsize=12, fontweight='bold')
    ax.set_title('Distribuição dos Similarity Scores\nQualidade dos Matches Encontrados', 
                 fontsize=14, fontweight='bold', pad=20)
    
    # Adicionar estatísticas no gráfico
    stats_text = f'Total: {total_scores:,} registros\nMédia: {score_medio:.1f}%\nMediana: {score_mediano:.1f}%'
    ax.text(0.02, 0.98, stats_text, transform=ax.transAxes, 
            verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
    
    # Adicionar legenda
    ax.legend()
    
    # Grid para facilitar leitura
    ax.grid(axis='y', alpha=0.3)
    
    # Ajustar layout
    plt.tight_layout()
    plt.savefig('output/grafico_3_similarity_scores.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"✓ Gráfico 3 salvo: Média de similarity {score_medio:.1f}% em {total_scores:,} matches")

def main():
    """Função principal - gera os 3 gráficos"""
    print("=" * 60)
    print("GERADOR DE GRÁFICOS - PIPELINE CAPES-DOI")
    print("=" * 60)
    
    try:
        # Configurar estilo
        setup_plot_style()
        
        # Carregar dados
        df = load_data()
        
        # Validar colunas necessárias
        required_columns = ['doi', 'NM_PROGRAMA_IES', 'similarity_score']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            raise ValueError(f"Colunas obrigatórias não encontradas: {missing_columns}")
        
        print("\n" + "-" * 40)
        print("GERANDO GRÁFICOS...")
        print("-" * 40)
        
        # Gerar os 3 gráficos
        grafico_1_taxa_sucesso(df)
        grafico_2_top_programas(df)
        grafico_3_similarity_scores(df)
        
        print("\n" + "=" * 60)
        print("✅ TODOS OS GRÁFICOS GERADOS COM SUCESSO!")
        print("=" * 60)
        print("Arquivos criados:")
        print("  - grafico_1_taxa_sucesso.png")
        print("  - grafico_2_top_programas.png") 
        print("  - grafico_3_similarity_scores.png")
        print("\nProntos para usar na apresentação!")
        
    except Exception as e:
        print(f"\n❌ ERRO: {str(e)}")
        print("Verifique se o arquivo de dados existe e está no formato correto.")

if __name__ == "__main__":
    main()
