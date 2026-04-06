"""Data Hub Orchestrator — routes files, manages state, triggers rebuilds."""
import os
import json
import pickle
from datetime import datetime

from data_hub.file_router import detect_file_type, SOURCE_INFO
from data_hub.ingest.sap_export import ingest_sap_export
from data_hub.ingest.sap_handover import ingest_handover
from data_hub.ingest.stock_pipeline import ingest_stock_pipeline
from data_hub.ingest.c4c_leads import ingest_c4c_leads
from data_hub.ingest.santander import ingest_santander, update_santander_cache
from data_hub.ingest.urban_science import ingest_urban_science
from data_hub.ingest.ga4 import ingest_ga4
from data_hub.enrichment import enrich
from data_hub.dashboard_generator import generate_dashboard
from data_hub.compute.engine import (
    compute_retail_sales, compute_dpd, compute_pipeline,
    compute_inventory, compute_historical_sales, compute_vex,
    compute_lead_kpis, compute_brand_leads, compute_santander,
    compute_scorecard, compute_objectives
)


class DataHub:
    def __init__(self, cache_dir='cache', ref_db_path='reference/reference.db',
                 template_path='templates/dashboard_template.html',
                 output_dir='outputs'):
        self.cache_dir = cache_dir
        self.ref_db_path = ref_db_path
        self.template_path = template_path
        self.output_dir = output_dir
        self.status_path = os.path.join(cache_dir, 'upload_status.json')
        self.santander_cache_path = os.path.join(cache_dir, 'santander_history.json')

        os.makedirs(cache_dir, exist_ok=True)
        os.makedirs(os.path.join(cache_dir, 'data'), exist_ok=True)
        os.makedirs(output_dir, exist_ok=True)

        self.upload_status = self._load_status()

    def _load_status(self):
        if os.path.exists(self.status_path):
            with open(self.status_path) as f:
                return json.load(f)
        return {}

    def _save_status(self):
        with open(self.status_path, 'w') as f:
            json.dump(self.upload_status, f, indent=2, default=str)

    def _store_data(self, key, df):
        """Store DataFrame to cache as parquet."""
        path = os.path.join(self.cache_dir, 'data', f'{key}.parquet')
        df.to_parquet(path, index=False)

    def _load_data(self, key):
        """Load DataFrame from cache."""
        path = os.path.join(self.cache_dir, 'data', f'{key}.parquet')
        if os.path.exists(path):
            return __import__('pandas').read_parquet(path)
        return None

    def _has_data(self, key):
        return os.path.exists(os.path.join(self.cache_dir, 'data', f'{key}.parquet'))

    def _update_status(self, source, row_count, filename=''):
        now = datetime.now()
        self.upload_status[source] = {
            'last_upload': now.isoformat(),
            'row_count': row_count,
            'filename': filename,
            'freshness': 'green',
        }
        self._save_status()

    def get_all_status(self):
        """Return status for all sources with freshness computed."""
        now = datetime.now()
        result = {}
        for source, info in SOURCE_INFO.items():
            status = self.upload_status.get(source, {})
            freshness = 'gray'
            if status:
                last = datetime.fromisoformat(status['last_upload'])
                age_hours = (now - last).total_seconds() / 3600
                cadence = info['cadence']
                if cadence == 'Daily':
                    freshness = 'green' if age_hours < 28 else 'yellow' if age_hours < 52 else 'red'
                elif cadence == 'Weekly':
                    freshness = 'green' if age_hours < 192 else 'yellow' if age_hours < 360 else 'red'
                elif cadence == 'Monthly':
                    freshness = 'green' if age_hours < 768 else 'yellow' if age_hours < 1440 else 'red'
                status['freshness'] = freshness

            result[source] = {
                **info,
                'status': status or {'freshness': 'gray', 'last_upload': None, 'row_count': 0},
            }
        return result

    def process_upload(self, filepath, filename=''):
        """Route and process an uploaded file."""
        file_type = detect_file_type(filepath)
        if file_type == 'unknown':
            return {'error': f'Could not detect file type for {filename}', 'detected': 'unknown'}

        result = {'detected': file_type, 'label': SOURCE_INFO.get(file_type, {}).get('label', file_type)}

        try:
            if file_type == 'sap_export':
                df = ingest_sap_export(filepath)
                self._store_data('sap_export', df)
                self._update_status('sap_export', len(df), filename)
                result['rows'] = len(df)

            elif file_type == 'sap_handover':
                df = ingest_handover(filepath)
                self._store_data('handover', df)
                self._update_status('sap_handover', len(df), filename)
                result['rows'] = len(df)

            elif file_type == 'stock_pipeline':
                df = ingest_stock_pipeline(filepath)
                self._store_data('stock_pipeline', df)
                self._update_status('stock_pipeline', len(df), filename)
                result['rows'] = len(df)

            elif file_type == 'c4c_leads':
                df = ingest_c4c_leads(filepath)
                self._store_data('leads', df)
                self._update_status('c4c_leads', len(df), filename)
                result['rows'] = len(df)

            elif file_type == 'santander':
                data = ingest_santander(filepath)
                # Store raw + update cache
                with open(os.path.join(self.cache_dir, 'santander_latest.json'), 'w') as f:
                    json.dump(data, f, default=str)
                update_santander_cache(self.santander_cache_path, data)
                total = sum(len(v) for v in data.values())
                self._update_status('santander', total, filename)
                result['rows'] = total

            elif file_type == 'urban_science':
                df = ingest_urban_science(filepath)
                self._store_data('urban_science', df)
                self._update_status('urban_science', len(df), filename)
                result['rows'] = len(df)

            elif file_type.startswith('ga4_'):
                data = ingest_ga4(filepath)
                self._store_data(file_type, data['data'])
                self._update_status(file_type, len(data['data']), filename)
                result['rows'] = len(data['data'])
                result['report_type'] = data['report_type']

            result['status'] = 'success'

        except Exception as e:
            result['status'] = 'error'
            result['error'] = str(e)

        return result

    def rebuild_dashboard(self):
        """Full dashboard rebuild from all available cached data."""
        today = datetime.now()
        errors = []
        compute_results = {}

        # Load cached data
        sap = self._load_data('sap_export')
        if sap is None:
            return {'error': 'SAP Export not uploaded yet. Upload at least the SAP Vehicle Export to rebuild.'}

        handover = self._load_data('handover')
        stock_pipeline = self._load_data('stock_pipeline')
        leads = self._load_data('leads')
        urban_science = self._load_data('urban_science')
        sales_order = self._load_data('sales_order')
        campaign_codes = self._load_data('campaign_codes')
        incentive_spend = self._load_data('incentive_spend')
        qm_leads = self._load_data('qm_leads')

        # Enrich
        try:
            enriched = enrich(sap, handover, stock_pipeline, urban_science,
                            sales_order, campaign_codes, incentive_spend, qm_leads,
                            self.ref_db_path)
        except Exception as e:
            return {'error': f'Enrichment failed: {e}'}

        # Compute
        funcs = [
            ('retail_sales', lambda: compute_retail_sales(enriched, self.ref_db_path, today)),
            ('dpd', lambda: compute_dpd(enriched, leads, today)),
            ('pipeline', lambda: compute_pipeline(enriched)),
            ('inventory', lambda: compute_inventory(enriched)),
            ('historical', lambda: compute_historical_sales(enriched)),
            ('vex', lambda: compute_vex(enriched)),
            ('scorecard', lambda: compute_scorecard(enriched, leads, self.ref_db_path, today)),
            ('objectives', lambda: compute_objectives(self.ref_db_path, today)),
        ]

        if leads is not None:
            funcs.append(('lead_kpis', lambda: compute_lead_kpis(leads, today)))
            funcs.append(('brand_leads', lambda: compute_brand_leads(leads, today)))

        sant_cache_path = self.santander_cache_path
        if os.path.exists(os.path.join(self.cache_dir, 'santander_latest.json')):
            with open(os.path.join(self.cache_dir, 'santander_latest.json')) as f:
                sant_data = json.load(f)
            sant_cache = {}
            if os.path.exists(sant_cache_path):
                with open(sant_cache_path) as f:
                    sant_cache = json.load(f)
            funcs.append(('santander', lambda: compute_santander(sant_data, sant_cache)))

        for name, func in funcs:
            try:
                compute_results[name] = func()
            except Exception as e:
                errors.append(f'{name}: {e}')
                compute_results[name] = {}

        # Save compute cache
        cache_path = os.path.join(self.cache_dir, 'last_compute.json')
        with open(cache_path, 'w') as f:
            json.dump(compute_results, f, default=str)

        # Serve Dashboard HTML from template (preserves existing Master File data)
        dashboard_result = None
        if os.path.exists(self.template_path):
            try:
                output_html = os.path.join(self.output_dir, 'Americas_Daily_Dashboard.html')
                import shutil
                os.makedirs(os.path.dirname(output_html), exist_ok=True)
                shutil.copy2(self.template_path, output_html)
                dashboard_result = {
                    'output_path': output_html,
                    'file_size': os.path.getsize(output_html),
                    'note': 'Dashboard served from template with existing data. For full refresh, upload Master File to Dashboard App.',
                }
            except Exception as e:
                errors.append(f'Dashboard copy: {e}')

        return {
            'status': 'success',
            'computed': list(compute_results.keys()),
            'errors': errors,
            'vehicle_count': len(enriched),
            'timestamp': today.isoformat(),
            'dashboard': dashboard_result,
        }
