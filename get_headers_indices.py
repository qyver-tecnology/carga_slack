#!/usr/bin/env python3
"""
Script para retornar apenas headers e índices de um site
"""

import sys
import os

# Adiciona o diretório src ao path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

from db_manager import DBManager
from google_sheets_processor import GoogleSheetsProcessor

def get_headers_indices(site_id: int):
    """Retorna headers e índices de um site"""
    try:
        # Conecta ao banco
        db = DBManager()
        db.connect()
        
        # Obtém configuração do site
        site_config = db.get_site_by_id(site_id)
        if not site_config:
            print(f"Site ID {site_id} não encontrado")
            return
        
        site_name = site_config['name']
        sheet_url = site_config['sheet_url']
        indices = site_config['indices']
        
        print(f"Site: {site_name}")
        print("=" * 40)
        
        # Inicializa processador
        sheets_processor = GoogleSheetsProcessor(sheet_url, site_name=site_name)
        
        # Obtém abas
        sheets = sheets_processor.get_sheet_ids()
        if not sheets:
            print("Nenhuma aba encontrada")
            return
        
        # Procura aba de Outubro
        target_sheet = None
        for sheet in sheets:
            if "Outubro" in sheet['name'] and "2025" in sheet['name']:
                target_sheet = sheet
                break
        
        if not target_sheet:
            target_sheet = sheets[0]
        
        # Lê dados
        records, summary, actual_name = sheets_processor.read_data(target_sheet['id'])
        
        if not records:
            print("Nenhum registro encontrado")
            return
        
        # Pega primeiro registro
        first_record = records[0]
        
        # Cria o JSON mapeando todas as colunas da planilha original
        headers_json = {}
        
        # Pega o cabeçalho original da planilha (não o registro processado)
        # Vamos usar o cabeçalho que foi lido pelo GoogleSheetsProcessor
        print("Mapeando todas as colunas da planilha...")
        
        # Lê novamente para pegar o cabeçalho original
        import gspread
        from google.oauth2.service_account import Credentials
        
        SCOPES = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        
        creds = Credentials.from_service_account_file('google_service_account.json', scopes=SCOPES)
        gc = gspread.authorize(creds)
        spreadsheet = gc.open_by_url(sheet_url)
        
        # Pega a aba de Outubro
        worksheet = None
        for ws in spreadsheet.worksheets():
            if "Outubro" in ws.title and "2025" in ws.title:
                worksheet = ws
                break
        
        if not worksheet:
            worksheet = spreadsheet.worksheets()[0]
        
        # Lê o cabeçalho original
        data = worksheet.get_all_values()
        header_row_index = None
        for i, row in enumerate(data):
            if row and (row[0] == "Data" or "Data" in row):
                header_row_index = i
                break
        
        if header_row_index is None:
            header_row_index = 0
        
        original_headers = data[header_row_index]
        
        # Mapeia todas as colunas
        for i, header in enumerate(original_headers):
            headers_json[f"Index {i}"] = header
        
        # Adiciona índices configurados
        headers_json["Configured Indices"] = {
            "Investimento": indices['investimento'],
            "Receita": indices['receita'],
            "ROAS": indices['roas'],
            "MC": indices['mc']
        }
        
        # Retorna JSON
        import json
        print(json.dumps(headers_json, indent=2, ensure_ascii=False))
        
    except Exception as e:
        print(f"Erro: {e}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Retorna headers e índices de um site')
    parser.add_argument('site_id', type=int, help='ID do site')
    
    args = parser.parse_args()
    get_headers_indices(args.site_id)
