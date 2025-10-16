#!/usr/bin/env python3
"""
Teste para processar um site específico pelo ID
"""

import sys
import os
import json
import logging
from datetime import datetime
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

def clean_value(val):
    """Limpa valores nulos ou inválidos."""
    if val in [None, '', '#DIV/0!', '#N/A', '#VALUE!', '#REF!', '#NAME?']:
        return '0,00'
    return val

def to_float(val):
    """Converte string para float, tratando formatação brasileira."""
    if not val:
        return 0.0
    val = str(val)
    import re
    match = re.search(r'-?\d+[\d.,]*', val.replace('R$', '').replace(' ', ''))
    if not match:
        return 0.0
    num = match.group(0).replace('.', '').replace(',', '.')
    try:
        return float(num)
    except:
        return 0.0

def is_dollar_value(value_str):
    """Determina se um valor está em dólar baseado no formato."""
    value_str = str(value_str).strip()
    return '$' in value_str and 'R$' not in value_str

def process_site_data(site_name: str, target_date: str = "01/10") -> Dict[str, Any]:
    """
    Processa dados de um site específico.
    
    Args:
        site_name: Nome do site
        target_date: Data alvo no formato DD/MM
        
    Returns:
        Dicionário com os dados do site
    """
    import time
    import random
    
    try:
        # Conecta ao banco de dados
        db = DBManager()
        db.connect()
        
        # Obtém configuração do site
        config = db.get_site_config(site_name)
        if not config or not config.get('sheet_url'):
            return {
                "site_name": site_name,
                "status": "error",
                "sheet_url": None,
                "message": "Site não encontrado ou sem URL de planilha",
                "data": None
            }
        
        sheet_url = config['sheet_url']
        logging.info(f"Processando site: {site_name}")
        
        # Aguarda um tempo aleatório antes de processar (1-3 segundos)
        time.sleep(random.uniform(1, 3))
        
        # Inicializa o processador de planilhas
        sheets_processor = GoogleSheetsProcessor(sheet_url, site_name=site_name)
        
        # Aguarda antes de obter as abas
        time.sleep(2)
        
        # Obtém todas as abas
        sheets = sheets_processor.get_sheet_ids()
        if not sheets:
            return {
                "site_name": site_name,
                "status": "error",
                "sheet_url": sheet_url,
                "message": "Nenhuma aba encontrada na planilha",
                "data": None
            }
        
        # Processa cada aba
        site_data = {
            "site_name": site_name,
            "status": "success",
            "sheet_url": sheet_url,
            "data": {
                "target_date": target_date,
                "sheets_processed": [],
                "totals": {
                    "investimento": 0.0,
                    "receita_real": 0.0,
                    "receita_dolar": 0.0,
                    "roas": "0,00",
                    "mc": "0,00"
                }
            }
        }
        
        total_investimento = 0.0
        total_receita_real = 0.0
        total_receita_dolar = 0.0
        roas_values = []
        mc_values = []
        
        # Filtra apenas abas do mês vigente (Outubro 2025)
        current_month = 10  # Outubro
        current_year = 2025
        mes_vigente_sheets = []
        
        for sheet in sheets:
            sheet_name = sheet['name']
            # Verifica se é uma aba do mês vigente
            for mes in ["Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]:
                if mes in sheet_name:
                    mes_num = ["Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"].index(mes) + 1
                    if mes_num == current_month and str(current_year) in sheet_name:
                        mes_vigente_sheets.append(sheet)
                        break
        
        # Se não encontrou aba do mês vigente, usa a primeira aba
        if not mes_vigente_sheets and sheets:
            mes_vigente_sheets = [sheets[0]]
            logging.warning(f"Nenhuma aba do mês vigente encontrada. Usando primeira aba: {sheets[0]['name']}")
        
        logging.info(f"Processando {len(mes_vigente_sheets)} abas do mês vigente")
        
        for sheet in mes_vigente_sheets:
            sheet_id = sheet['id']
            sheet_name = sheet['name']
            
            # Sistema de retry para rate limit
            max_retries = 10  # Máximo 10 tentativas
            retry_count = 0
            success = False
            
            while retry_count < max_retries and not success:
                try:
                    # Aguarda antes de ler cada aba (3-5 segundos)
                    time.sleep(random.uniform(3, 5))
                    
                    records, summary, actual_name = sheets_processor.read_data(sheet_id)
                    if not records:
                        success = True  # Considera sucesso se não há dados
                        continue
                    
                    pagina = actual_name or sheet_name
                    
                    # Procura por registro da data alvo
                    target_record = None
                    print(f"[DEBUG] Procurando por data: {target_date}")
                    for record in reversed(records):
                        record_date = record.get('Data')
                        print(f"[DEBUG] Comparando: '{target_date}' com '{record_date}'")
                        if record_date == target_date:
                            target_record = record
                            print(f"[DEBUG] Encontrou registro para {target_date}: {target_record}")
                            break
                    
                    if not target_record:
                        logging.warning(f"Nenhum registro encontrado para {target_date} na aba {pagina}")
                        success = True  # Considera sucesso se não encontrou dados
                        continue
                
                    # Extrai dados do registro usando os nomes corretos das colunas
                    # Baseado no registro encontrado: Investimento, Receita, ROAS Geral, MC Geral
                    investimento = clean_value(target_record.get('Investimento', '0,00'))
                    receita = clean_value(target_record.get('Receita', '0,00'))
                    roas = clean_value(target_record.get('ROAS Geral', '0,00'))
                    mc = clean_value(target_record.get('MC Geral', '0,00'))
                    
                    # Converte para float
                    investimento_float = to_float(investimento)
                    receita_float = to_float(receita)
                    roas_float = to_float(roas)
                    mc_float = to_float(mc)
                    
                    # Verifica se receita está em dólar
                    is_dolar = is_dollar_value(receita)
                    
                    # Acumula totais
                    total_investimento += investimento_float
                    if is_dolar:
                        total_receita_dolar += receita_float
                    else:
                        total_receita_real += receita_float
                    
                    roas_values.append(roas_float)
                    mc_values.append(mc_float)
                    
                    # Adiciona aos dados processados
                    site_data["data"]["sheets_processed"].append({
                        "sheet_name": pagina,
                        "investimento": investimento,
                        "receita": receita,
                        "roas": roas,
                        "mc": mc,
                        "investimento_float": investimento_float,
                        "receita_float": receita_float,
                        "roas_float": roas_float,
                        "mc_float": mc_float,
                        "is_dollar": is_dolar
                    })
                    
                    success = True
                    
                except Exception as e:
                    error_msg = str(e)
                    if 'Rate Limit' in error_msg or 'Quota' in error_msg or '429' in error_msg:
                        retry_count += 1
                        logging.warning(f"Rate limit detectado. Tentativa {retry_count}/{max_retries}. Aguardando 5 segundos...")
                        time.sleep(5)
                    else:
                        logging.error(f"Erro ao processar aba {sheet_name}: {e}")
                        break
            
            if not success:
                logging.error(f"Falha ao processar aba {sheet_name} após {max_retries} tentativas")
        
        # Calcula totais
        if roas_values:
            roas_medio = sum(roas_values) / len(roas_values)
            site_data["data"]["totals"]["roas"] = f"{roas_medio:.2f}".replace('.', ',')
        
        if mc_values:
            mc_total = sum(mc_values)
            site_data["data"]["totals"]["mc"] = f"{mc_total:.2f}".replace('.', ',')
        
        site_data["data"]["totals"]["investimento"] = total_investimento
        site_data["data"]["totals"]["receita_real"] = total_receita_real
        site_data["data"]["totals"]["receita_dolar"] = total_receita_dolar
        
        return site_data
        
    except Exception as e:
        error_msg = str(e)
        if "NoValidUrlKeyFound" in error_msg:
            error_msg = "URL da planilha inválida ou malformada"
        elif "PermissionError" in error_msg or "403" in error_msg:
            error_msg = "Sem permissão para acessar a planilha. Verifique se a conta de serviço tem acesso."
        elif "APIError" in error_msg:
            error_msg = "Erro da API do Google Sheets. Verifique permissões e quota."
        
        return {
            "site_name": site_name,
            "status": "error",
            "sheet_url": sheet_url if 'sheet_url' in locals() else None,
            "message": f"Erro ao processar site: {error_msg}",
            "data": None
        }

def main():
    """Função principal do teste."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Processa dados de um site específico pelo ID para uma data.')
    parser.add_argument('site_id', type=int, nargs='?', help='ID do site a ser processado. Se não fornecido, processa todos os IDs de 1 a 28.')
    parser.add_argument('target_date', type=str, nargs='?', default='01/10', help='Data alvo no formato DD/MM (padrão: 01/10).')
    
    args = parser.parse_args()
    
    setup_logging()
    
    # Se não foi fornecido ID, processa todos os IDs de 1 a 28
    if args.site_id is None:
        print(f"=== TESTE TODOS OS SITES (IDs 1-28) ===")
        print(f"Data: {args.target_date}")
        print("=" * 50)
        
        # Conecta ao banco
        db = DBManager()
        db.connect()
        
        results = []
        for site_id in range(1, 29):  # IDs de 1 a 28
            print(f"\n--- Processando Site ID: {site_id} ---")
            
            site_config = db.get_site_by_id(site_id)
            if not site_config:
                print(f"Site com ID {site_id} não encontrado. Pulando...")
                continue
            
            site_name = site_config['name']
            print(f"Nome do site: {site_name}")
            
            site_data = process_site_data(site_name, args.target_date)
            results.append(site_data)
            
            # Salva resultado individual em JSON
            site_filename = f"test_site_{site_id}_{site_name.replace(' ', '_').replace('-', '_').lower()}_{args.target_date.replace('/', '_')}.json"
            json_output = json.dumps(site_data, indent=2, ensure_ascii=False)
            
            with open(site_filename, 'w', encoding='utf-8') as f:
                f.write(json_output)
            
            print(f"[OK] Resultado salvo em: {site_filename}")
            
            # Mostra resumo individual
            if site_data["status"] == "success":
                totals = site_data["data"]["totals"]
                print(f"[RESUMO] {site_name}:")
                print(f"  Investimento: {totals['investimento']:.2f}")
                print(f"  ROAS: {totals['roas']}")
                print(f"  MC: {totals['mc']}")
            else:
                print(f"[ERRO] {site_data['message']}")
        
        # Salva resultado consolidado
        consolidated_filename = f"test_all_sites_{args.target_date.replace('/', '_')}.json"
        consolidated_output = json.dumps(results, indent=2, ensure_ascii=False)
        
        with open(consolidated_filename, 'w', encoding='utf-8') as f:
            f.write(consolidated_output)
        
        print(f"\n[OK] Resultado consolidado salvo em: {consolidated_filename}")
        
        # Mostra resumo geral
        successful_sites = [r for r in results if r["status"] == "success"]
        print(f"\n[RESUMO GERAL]")
        print(f"Total de sites processados: {len(results)}")
        print(f"Sites com sucesso: {len(successful_sites)}")
        print(f"Sites com erro: {len(results) - len(successful_sites)}")
        
    else:
        # Processamento de site específico (código original)
        print(f"=== TESTE SITE POR ID ===")
        print(f"ID do Site: {args.site_id}")
        print(f"Data: {args.target_date}")
        print("=" * 30)
        
        # Conecta ao banco para obter o nome do site
        db = DBManager()
        db.connect()
        site_config = db.get_site_by_id(args.site_id)
        
        if not site_config:
            print(f"Erro: Site com ID {args.site_id} não encontrado.")
            sys.exit(1)
        
        site_name = site_config['name']
        site_data = process_site_data(site_name, args.target_date)
        
        # Salva resultado em JSON
        site_filename = f"test_site_{args.site_id}_{site_name.replace(' ', '_').replace('-', '_').lower()}_{args.target_date.replace('/', '_')}.json"
        json_output = json.dumps(site_data, indent=2, ensure_ascii=False)
        
        with open(site_filename, 'w', encoding='utf-8') as f:
            f.write(json_output)
        
        print(f"[OK] Resultado salvo em: {site_filename}")
        
        # Mostra resumo
        if site_data["status"] == "success":
            totals = site_data["data"]["totals"]
            print(f"\n[DADOS] Resultado:")
            print(json.dumps(site_data, indent=2, ensure_ascii=False))
            
            print(f"\n[RESUMO] {site_name}:")
            print(f"  Investimento: {totals['investimento']:.2f}")
            print(f"  ROAS: {totals['roas']}")
            print(f"  MC: {totals['mc']}")
            print(f"  URL: {site_data['sheet_url']}")
        else:
            print(f"[ERRO] {site_data['message']}")

if __name__ == "__main__":
    main()