#!/usr/bin/env python3
"""
Script para download dos dados CAPES necessários para o pipeline
Execução: python download_data.py
"""

import requests
import os
from pathlib import Path
import sys

def create_data_structure():
    """Cria a estrutura de pastas necessária"""
    folders = [
        'data/raw',
        'data/processed', 
        'data/enriched',
        'logs',
        'output'
    ]
    
    for folder in folders:
        Path(folder).mkdir(parents=True, exist_ok=True)
        print(f"✓ Pasta criada: {folder}")

def download_file(url, filepath, description):
    """Download de arquivo com barra de progresso"""
    print(f"\nBaixando {description}...")
    print(f"URL: {url}")
    print(f"Destino: {filepath}")
    
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        
        with open(filepath, 'wb') as file:
            downloaded = 0
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)
                    downloaded += len(chunk)
                    
                    if total_size > 0:
                        percent = (downloaded / total_size) * 100
                        print(f"\rProgresso: {percent:.1f}% ({downloaded:,}/{total_size:,} bytes)", end='')
                    else:
                        print(f"\rBaixado: {downloaded:,} bytes", end='')
        
        print(f"\n✓ {description} baixado com sucesso!")
        return True
        
    except requests.exceptions.RequestException as e:
        print(f"\n❌ Erro ao baixar {description}: {e}")
        return False
    except Exception as e:
        print(f"\n❌ Erro inesperado: {e}")
        return False

def check_existing_files():
    """Verifica se os arquivos já existem"""
    files_to_check = [
        ('data/raw/capes_parte1.csv', 'CAPES Parte 1'),
        ('data/raw/capes_parte2.csv', 'CAPES Parte 2')
    ]
    
    existing_files = []
    for filepath, description in files_to_check:
        if Path(filepath).exists():
            size = Path(filepath).stat().st_size
            existing_files.append((filepath, description, size))
    
    return existing_files

def main():
    """Função principal"""
    print("=" * 60)
    print("DOWNLOAD DOS DADOS CAPES")
    print("Pipeline CAPES-DOI")
    print("=" * 60)
    
    # URLs dos dados CAPES
    urls = {
        'parte1': {
            'url': 'https://dadosabertos.capes.gov.br/dataset/589e8182-71e3-4669-b47a-3f8ecf1a16ab/resource/25020945-8050-4922-92f0-606c0eb45e7b/download/br-capes-colsucup-producao-2017a2020-2023-11-30-bibliografica-artpe_parte1.csv',
            'filepath': 'data/raw/capes_parte1.csv',
            'description': 'CAPES Parte 1'
        },
        'parte2': {
            'url': 'https://dadosabertos.capes.gov.br/dataset/589e8182-71e3-4669-b47a-3f8ecf1a16ab/resource/209740e3-2e6e-4c5f-a705-7566bd7276c0/download/br-capes-colsucup-producao-2017a2020-2023-11-30-bibliografica-artpe_parte2.csv',
            'filepath': 'data/raw/capes_parte2.csv',
            'description': 'CAPES Parte 2'
        }
    }
    
    # Criar estrutura de pastas
    print("\n1. Criando estrutura de pastas...")
    create_data_structure()
    
    # Verificar arquivos existentes
    print("\n2. Verificando arquivos existentes...")
    existing_files = check_existing_files()
    
    if existing_files:
        print("Arquivos já encontrados:")
        for filepath, description, size in existing_files:
            print(f"  ✓ {description}: {filepath} ({size:,} bytes)")
        
        response = input("\nDeseja baixar novamente? (s/N): ").lower().strip()
        if response not in ['s', 'sim', 'y', 'yes']:
            print("Download cancelado. Usando arquivos existentes.")
            return
    
    # Download dos arquivos
    print("\n3. Iniciando downloads...")
    success_count = 0
    
    for key, info in urls.items():
        success = download_file(
            info['url'], 
            info['filepath'], 
            info['description']
        )
        if success:
            success_count += 1
    
    # Resultado final
    print("\n" + "=" * 60)
    if success_count == len(urls):
        print("✅ TODOS OS ARQUIVOS BAIXADOS COM SUCESSO!")
        print("\nArquivos disponíveis:")
        for info in urls.values():
            if Path(info['filepath']).exists():
                size = Path(info['filepath']).stat().st_size
                print(f"  • {info['description']}: {info['filepath']} ({size:,} bytes)")
        
        print("\nPróximo passo:")
        print("  python pipeline_with_resume.py")
        
    else:
        print("❌ ALGUNS DOWNLOADS FALHARAM")
        print(f"Sucesso: {success_count}/{len(urls)} arquivos")
        print("\nVerifique sua conexão e tente novamente.")
        sys.exit(1)

if __name__ == "__main__":
    main()
