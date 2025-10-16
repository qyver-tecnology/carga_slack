#!/usr/bin/env python3
"""
Script para verificar os índices das colunas na aba de Outubro
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

from google_sheets_processor import GoogleSheetsProcessor

def check_october_indices():
    """Verifica os índices das colunas na aba de Outubro"""
    
    # URL da planilha
    sheet_url = "https://docs.google.com/spreadsheets/d/1tE7ZBhvsfUqcZNa4UnrrALrXOwRlc185a7iVPh_iv7g/edit?gid=1046712131"
    
    print("=== VERIFICANDO ÍNDICES DA ABA DE OUTUBRO ===")
    print(f"URL: {sheet_url}")
    print("=" * 60)
    
    try:
        # Inicializa o processador
        processor = GoogleSheetsProcessor(sheet_url, "Tech Pra Todos")
        
        # Pega as abas
        sheets = processor.get_sheet_ids()
        if not sheets:
            print("Nenhuma aba encontrada!")
            return
        
        # Procura pela aba de Outubro
        october_sheet = None
        for sheet in sheets:
            if "Outubro" in sheet['name'] and "2025" in sheet['name']:
                october_sheet = sheet
                break
        
        if not october_sheet:
            print("Aba de Outubro 2025 não encontrada!")
            print("Abas disponíveis:")
            for sheet in sheets:
                print(f"  - {sheet['name']}")
            return
        
        print(f"Processando aba: {october_sheet['name']}")
        
        # Lê os dados
        records, summary, actual_name = processor.read_data(october_sheet['id'])
        
        if not records:
            print("Nenhum registro encontrado!")
            return
        
        # Pega o primeiro registro para ver a estrutura
        first_record = records[0]
        print(f"\nEstrutura do primeiro registro:")
        print("-" * 50)
        
        for i, (key, value) in enumerate(first_record.items()):
            print(f"Índice {i:2d}: '{key}' = '{value}'")
        
        print("\n" + "=" * 60)
        print("MAPEAMENTO CORRETO DOS ÍNDICES:")
        print("=" * 60)
        
        # Mapeia as colunas importantes
        column_mapping = {}
        for i, (key, value) in enumerate(first_record.items()):
            if 'Total ADS' in key:
                column_mapping['investimento'] = (i, key)
            elif 'Adx $' in key or 'ADX ($)' in key:
                column_mapping['receita'] = (i, key)
            elif 'ROAS' in key and 'Geral' in key:
                column_mapping['roas'] = (i, key)
            elif 'MC' in key and 'Geral' in key:
                column_mapping['mc'] = (i, key)
        
        print("COLUNAS ENCONTRADAS:")
        for col_type, (index, name) in column_mapping.items():
            print(f"  {col_type.upper()}: Índice {index} = '{name}'")
        
        print("\nSQL UPDATE SUGERIDO:")
        print("=" * 60)
        print("UPDATE carga_slack_db.column_indices")
        print("SET")
        for col_type, (index, name) in column_mapping.items():
            print(f"    {col_type}_idx = {index},    -- {name}")
        print("    updated_at = NOW()")
        print("WHERE site_id = 1;")
        
    except Exception as e:
        print(f"Erro: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_october_indices()
