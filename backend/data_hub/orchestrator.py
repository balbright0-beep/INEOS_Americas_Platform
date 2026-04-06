"""Data Hub Orchestrator — routes files, manages state, triggers rebuilds.

Rebuild strategy:
  Individual source uploads → cached as parquet → assembled into temp xlsx
  → original dashboard_refresh_all_in_one.py processor runs against it
  → Dashboard HTML generated with all 35+ constants → served at /dashboard
"""
import os
import json
from datetime import datetime

from data_hub.file_router import detect_file_type, SOURCE_INFO
from data_hub.ingest.sap_export import ingest_sap_export
from data_hub.ingest.sap_handover import ingest_handover
from data_hub.ingest.stock_pipeline import ingest_stock_pipeline
from data_hub.ingest.c4c_leads import ingest_c4c_leads
from data_hub.ingest.santander import ingest_santander, update_santander_cache
from data_hub.ingest.urban_science import ingest_urban_science
from data_hub.ingest.ga4 import ingest_ga4


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
        # Persist to DB
        try:
            status_bytes = json.dumps(self.upload_status, default=str).encode('utf-8')
            self._save_to_db('_upload_status', status_bytes, 0)
        except Exception:
            pass

    def _store_data(self, key, df):
        """Store DataFrame to cache as parquet on disk AND in PostgreSQL."""
        path = os.path.join(self.cache_dir, 'data', f'{key}.parquet')
        df.to_parquet(path, index=False)

        # Also persist to PostgreSQL so data survives Render deploys
        try:
            with open(path, 'rb') as f:
                parquet_bytes = f.read()
            self._save_to_db(key, parquet_bytes, len(df))
        except Exception as e:
            print(f"  Warning: could not persist {key} to DB: {e}")

    def _load_data(self, key):
        """Load DataFrame from cache (disk first, DB fallback)."""
        import pandas as pd
        path = os.path.join(self.cache_dir, 'data', f'{key}.parquet')
        if os.path.exists(path):
            return pd.read_parquet(path)
        # Fallback: restore from PostgreSQL
        restored = self._restore_from_db(key)
        if restored:
            return pd.read_parquet(path)
        return None

    def _has_data(self, key):
        path = os.path.join(self.cache_dir, 'data', f'{key}.parquet')
        if os.path.exists(path):
            return True
        # Check if we can restore from DB
        return self._restore_from_db(key)

    def _save_to_db(self, key, parquet_bytes, row_count=0):
        """Save parquet bytes to PostgreSQL for persistence across deploys."""
        try:
            from app.database import SessionLocal
            from app.models import CachedFile
            db = SessionLocal()
            try:
                existing = db.query(CachedFile).filter(CachedFile.key == key).first()
                if existing:
                    existing.data = parquet_bytes
                    existing.row_count = row_count
                    existing.uploaded_at = datetime.now()
                else:
                    db.add(CachedFile(key=key, data=parquet_bytes, row_count=row_count))
                db.commit()
            finally:
                db.close()
        except Exception as e:
            print(f"  DB persist error for {key}: {e}")

    def _restore_from_db(self, key):
        """Restore a cached file from PostgreSQL to disk. Returns True if restored."""
        try:
            from app.database import SessionLocal
            from app.models import CachedFile
            db = SessionLocal()
            try:
                cached = db.query(CachedFile).filter(CachedFile.key == key).first()
                if cached and cached.data:
                    path = os.path.join(self.cache_dir, 'data', f'{key}.parquet')
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                    with open(path, 'wb') as f:
                        f.write(cached.data)
                    print(f"  Restored {key} from DB ({len(cached.data):,} bytes)")
                    return True
            finally:
                db.close()
        except Exception as e:
            print(f"  DB restore error for {key}: {e}")
        return False

    def restore_all_from_db(self):
        """Restore all cached files from PostgreSQL to disk."""
        try:
            from app.database import SessionLocal
            from app.models import CachedFile
            db = SessionLocal()
            try:
                cached_files = db.query(CachedFile).all()
                count = 0
                for cf in cached_files:
                    if not cf.data:
                        continue
                    if cf.key == '_santander_json':
                        # Restore Santander JSON
                        sant_path = os.path.join(self.cache_dir, 'santander_latest.json')
                        os.makedirs(os.path.dirname(sant_path), exist_ok=True)
                        with open(sant_path, 'wb') as f:
                            f.write(cf.data)
                        print(f"  Restored santander JSON from DB ({len(cf.data):,} bytes)")
                    elif cf.key == '_upload_status':
                        # Restore upload status
                        with open(self.status_path, 'wb') as f:
                            f.write(cf.data)
                        self.upload_status = self._load_status()
                        print(f"  Restored upload_status from DB")
                    elif cf.key == 'santander':
                        # Restore santander JSON (stored by upload-source endpoint)
                        sant_path = os.path.join(self.cache_dir, 'santander_latest.json')
                        os.makedirs(os.path.dirname(sant_path), exist_ok=True)
                        with open(sant_path, 'wb') as f:
                            f.write(cf.data)
                        # Also save as santander.json for compatibility
                        with open(os.path.join(self.cache_dir, 'data', 'santander.json'), 'wb') as f:
                            f.write(cf.data)
                        print(f"  Restored santander from DB ({len(cf.data):,} bytes)")
                    else:
                        # Restore parquet file
                        path = os.path.join(self.cache_dir, 'data', f'{cf.key}.parquet')
                        os.makedirs(os.path.dirname(path), exist_ok=True)
                        with open(path, 'wb') as f:
                            f.write(cf.data)
                        # Create alias files for name mismatches
                        ALIASES = {
                            'c4c_leads': 'leads',      # assembler loads 'leads'
                            'sap_handover': 'handover', # assembler fallback
                        }
                        if cf.key in ALIASES:
                            alias_path = os.path.join(self.cache_dir, 'data', f'{ALIASES[cf.key]}.parquet')
                            with open(alias_path, 'wb') as f:
                                f.write(cf.data)
                            print(f"  Restored {cf.key} + alias {ALIASES[cf.key]} from DB ({len(cf.data):,} bytes, {cf.row_count} rows)")
                        else:
                            print(f"  Restored {cf.key} from DB ({len(cf.data):,} bytes, {cf.row_count} rows)")
                    count += 1
                return count
            finally:
                db.close()
        except Exception as e:
            print(f"  DB restore all error: {e}")
            return 0

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
                sant_json = json.dumps(data, default=str)
                with open(os.path.join(self.cache_dir, 'santander_latest.json'), 'w') as f:
                    f.write(sant_json)
                update_santander_cache(self.santander_cache_path, data)
                total = sum(len(v) for v in data.values())
                self._update_status('santander', total, filename)
                # Persist Santander JSON to DB
                try:
                    self._save_to_db('_santander_json', sant_json.encode('utf-8'), total)
                except Exception:
                    pass
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
        """Full dashboard rebuild from uploaded source files.

        Uses the Bridge approach:
        1. Restores any missing cached files from PostgreSQL
        2. Assembles a temporary xlsx from cached source DataFrames
        3. Runs the original dashboard_refresh_all_in_one.py processor against it
        4. The processor generates the Dashboard HTML with all 35+ constants

        This eliminates the need for the Master File — individual source
        uploads produce the same output as the original workflow.
        """
        # Restore cached files from DB if disk was wiped (Render deploys)
        restored = self.restore_all_from_db()
        if restored:
            print(f"  Restored {restored} cached files from PostgreSQL")

        # Verify minimum data exists
        if not self._has_data('sap_export'):
            return {'error': 'SAP Export not uploaded yet. Upload at least the SAP Vehicle Export to rebuild.'}

        if not os.path.exists(self.template_path):
            return {'error': f'Dashboard template not found at {self.template_path}'}

        output_html = os.path.join(self.output_dir, 'Americas_Daily_Dashboard.html')

        try:
            from data_hub.dashboard_bridge import generate_dashboard_from_sources

            result = generate_dashboard_from_sources(
                cache_dir=self.cache_dir,
                template_path=self.template_path,
                output_path=output_html,
            )

            # Also update the Platform's own vehicle database
            try:
                self._update_platform_db()
            except Exception as e:
                result.setdefault('warnings', []).append(f'Platform DB update: {e}')

            return result

        except Exception as e:
            import traceback
            traceback.print_exc()
            return {'status': 'error', 'error': str(e)}

    def _update_platform_db(self):
        """Update the Platform's SQLAlchemy vehicle database from cached source data.
        This keeps the dealer portal data in sync with the dashboard."""
        sap = self._load_data('sap_export')
        if sap is None:
            return

        # The Platform's own processor handles DB updates
        # This is called after the dashboard is generated
        try:
            from data_hub.master_assembler import assemble_master_xlsx
            xlsx_path = assemble_master_xlsx(self.cache_dir)
            try:
                from app.processor import process_master_file
                process_master_file(xlsx_path)
            except ImportError:
                pass  # processor not available in this context
            finally:
                try:
                    os.unlink(xlsx_path)
                except OSError:
                    pass
        except Exception:
            pass  # Non-critical — dealer portal data may be stale
