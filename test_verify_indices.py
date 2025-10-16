#!/usr/bin/env python3
"""
Teste para verificar os índices de um site específico passando o ID
"""

import sys
import os
import json
import logging
from typing import Dict, List, Any

# Adiciona o diretório src ao path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

from db_manager import DBManager
from google_sheets_processor import GoogleSheetsProcessor

def setup_logging():
    """Configura o logging para o teste."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )

def verify_site_indices(site_id: int, target_date: str = "01/10"):
    """
    Verifica os índices de um site específico pelo ID.
    
    Args:
        site_id: ID do site no banco de dados
        target_date: Data alvo para verificar os dados
        
    Returns:
        Dicionário com informações dos índices e dados extraídos
    """
    try:
        # Conecta ao banco de dados
        db = DBManager()
        db.connect()
        
        # Obtém configuração do site pelo ID
        site_config = db.get_site_by_id(site_id)
        if not site_config:
            return {
                "status": "error",
                "message": f"Site com ID {site_id} não encontrado",
                "site_id": site_id
            }
        
        site_name = site_config['name']
        sheet_url = site_config['sheet_url']
        indices = site_config['indices']
        
        print(f"=== VERIFICAÇÃO DE ÍNDICES - SITE ID {site_id} ===")
        print(f"Site: {site_name}")
        print(f"URL: {sheet_url}")
        print("=" * 60)
        
        # Inicializa o processador de planilhas
        sheets_processor = GoogleSheetsProcessor(sheet_url, site_name=site_name)
        
        # Obtém todas as abas
        sheets = sheets_processor.get_sheet_ids()
        if not sheets:
            return {
                "status": "error",
                "message": "Nenhuma aba encontrada na planilha",
                "site_id": site_id,
                "site_name": site_name
            }
        
        print("ABAS DISPONÍVEIS:")
        for i, sheet in enumerate(sheets):
            print(f"  {i+1}. {sheet['name']} (ID: {sheet['id']})")
        
        # Procura pela aba de Outubro 2025
        target_sheet = None
        for sheet in sheets:
            if "Outubro" in sheet['name'] and "2025" in sheet['name']:
                target_sheet = sheet
                break
        
        if not target_sheet:
            # Se não encontrar Outubro, usa a primeira aba
            target_sheet = sheets[0]
            print(f"\nAVISO: Aba de Outubro 2025 não encontrada. Usando: {target_sheet['name']}")
        else:
            print(f"\nProcessando aba: {target_sheet['name']}")
        
        # Lê os dados da aba selecionada
        records, summary, actual_name = sheets_processor.read_data(target_sheet['id'])
        
        if not records:
            return {
                "status": "error",
                "message": "Nenhum registro encontrado na planilha",
                "site_id": site_id,
                "site_name": site_name
            }
        
        # Pega o primeiro registro para ver a estrutura
        first_record = records[0]
        print(f"\nESTRUTURA DO PRIMEIRO REGISTRO:")
        print("-" * 50)
        
        for i, (key, value) in enumerate(first_record.items()):
            print(f"Índice {i:2d}: '{key}' = '{value}'")
        
        print(f"\nÍNDICES CONFIGURADOS NO BANCO:")
        print("-" * 50)
        print(f"Investimento: {indices['investimento']} -> '{list(first_record.keys())[indices['investimento']] if indices['investimento'] < len(first_record) else 'N/A'}'")
        print(f"Receita: {indices['receita']} -> '{list(first_record.keys())[indices['receita']] if indices['receita'] < len(first_record) else 'N/A'}'")
        print(f"ROAS: {indices['roas']} -> '{list(first_record.keys())[indices['roas']] if indices['roas'] < len(first_record) else 'N/A'}'")
        print(f"MC: {indices['mc']} -> '{list(first_record.keys())[indices['mc']] if indices['mc'] < len(first_record) else 'N/A'}'")
        
        # Procura por registro da data específica
        target_record = None
        for record in reversed(records):
            if record.get('Data') == target_date:
                target_record = record
                break
        
        if not target_record:
            print(f"\nAVISO: Nenhum registro encontrado para a data {target_date}")
            target_record = first_record  # Usa o primeiro registro como exemplo
        
        print(f"\nDADOS EXTRAÍDOS PARA DATA {target_date}:")
        print("-" * 50)
        
        # Extrai os dados usando os índices configurados
        investimento = target_record.get('Investimento', 'N/A')
        receita = target_record.get('Receita', 'N/A')
        roas = target_record.get('ROAS Geral', 'N/A')
        mc = target_record.get('MC Geral', 'N/A')
        
        print(f"Investimento: {investimento}")
        print(f"Receita: {receita}")
        print(f"ROAS: {roas}")
        print(f"MC: {mc}")
        
        # Verifica se os dados foram extraídos corretamente
        data_quality = {
            "investimento_ok": investimento != 'N/A' and investimento != '0,00',
            "receita_ok": receita != 'N/A' and receita != '0,00',
            "roas_ok": roas != 'N/A' and roas != '0,00',
            "mc_ok": mc != 'N/A' and mc != '0,00'
        }
        
        print(f"\nQUALIDADE DOS DADOS:")
        print("-" * 50)
        for field, is_ok in data_quality.items():
            status = "[OK]" if is_ok else "[PROBLEMA]"
            print(f"{field.replace('_', ' ').title()}: {status}")
        
        # Retorna resultado
        result = {
            "status": "success",
            "site_id": site_id,
            "site_name": site_name,
            "sheet_url": sheet_url,
            "indices": indices,
            "first_record_structure": first_record,
            "target_date": target_date,
            "target_record": target_record,
            "extracted_data": {
                "investimento": investimento,
                "receita": receita,
                "roas": roas,
                "mc": mc
            },
            "data_quality": data_quality,
            "sheets_available": [sheet['name'] for sheet in sheets]
        }
        
        return result
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"Erro ao verificar índices: {str(e)}",
            "site_id": site_id
        }

def main():
    """Função principal do teste."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Verifica os índices de um site específico')
    parser.add_argument('site_id', type=int, help='ID do site a ser verificado')
    parser.add_argument('--date', type=str, default='01/10', help='Data alvo (padrão: 01/10)')
    parser.add_argument('--json', action='store_true', help='Salva resultado em JSON')
    
    args = parser.parse_args()
    
    setup_logging()
    
    print(f"Verificando índices do site ID {args.site_id} para data {args.date}")
    print("=" * 60)
    
    result = verify_site_indices(args.site_id, args.date)
    
    if args.json:
        # Salva resultado em JSON
        filename = f"verify_indices_site_{args.site_id}_{args.date.replace('/', '_')}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        print(f"\nResultado salvo em: {filename}")
    
    print(f"\nSTATUS: {result['status']}")
    if result['status'] == 'error':
        print(f"ERRO: {result['message']}")
    else:
        print(f"Site: {result['site_name']}")
        print(f"Índices: {result['indices']}")
        print(f"Qualidade dos dados: {result['data_quality']}")

if __name__ == "__main__":
    main()
