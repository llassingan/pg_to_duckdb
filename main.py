import os
import time
import cx_Oracle
import duckdb
import csv
import logging
import json
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'/tmp/logs/migration_{timestamp}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class OracleToDuckDBMigration:

    def __init__(self, config_path='/config/config.json'):
        with open(config_path, 'r') as config_file:
            self.config = json.load(config_file)
        
        self.oracle_params = {
            'host'         : os.getenv('ORACLE_HOST'),
            'port'         : os.getenv('ORACLE_PORT'),
            'service_name' : os.getenv('ORACLE_SERVICE'),
            'user'         : os.getenv('ORACLE_USER'),
            'password'     : os.getenv('ORACLE_PASSWORD')
        }

        self.duckdb_params = {
            'database'  : os.getenv('DUCKDB_DB', 'default.db')
        }

        self.dump_dir = '/tmp/dumps'
        os.makedirs(self.dump_dir, exist_ok=True)
    
    def hc_oracle(self):
        while True:
            try:
                dsn = cx_Oracle.makedsn(
                    self.oracle_params['host'],
                    self.oracle_params['port'],
                    service_name=self.oracle_params['service_name']
                )
                conn = cx_Oracle.connect(
                    user=self.oracle_params['user'],
                    password=self.oracle_params['password'],
                    dsn=dsn
                )
                conn.close()
                logger.info("Oracle connection successful")
                return
            except cx_Oracle.DatabaseError as e:
                logger.warning(f"Waiting for Oracle... Error: {str(e)}")
                time.sleep(2)

    def hc_duckdb(self):
        while True:
            try:
                conn = duckdb.connect(self.duckdb_params['database'])
                conn.close()
                logger.info("DuckDB connection successful")
                return
            except Exception as e:
                logger.warning(f"Waiting for DuckDB... Error: {str(e)}")
                time.sleep(2)

    def oracle_to_csv_copy(self, output_file, table):
        logger.info(f"Starting Oracle export to {output_file}")
        start_time = time.time()
        
        dsn = cx_Oracle.makedsn(
            self.oracle_params['host'],
            self.oracle_params['port'],
            service_name=self.oracle_params['service_name']
        )
        conn = cx_Oracle.connect(
            user=self.oracle_params['user'],
            password=self.oracle_params['password'],
            dsn=dsn
        )
        
        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM {table}")
            
            # Get column names
            columns = [col[0] for col in cursor.description]
            
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(columns)  # Write header
                
                batch_size = 10000
                total_rows = 0
                
                while True:
                    rows = cursor.fetchmany(batch_size)
                    if not rows:
                        break
                    writer.writerows(rows)
                    total_rows += len(rows)
                    logger.info(f"Exported {total_rows} rows...")
            
            file_size = os.path.getsize(output_file) / (1024 * 1024)
            elapsed_time = time.time() - start_time
            logger.info(f"Data exported to {output_file} (Size: {file_size:.2f} MB, Rows: {total_rows}, Time: {elapsed_time:.2f}s)")
        
        except Exception as e:
            logger.error(f"Error during Oracle export: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()

    def csv_to_duckdb(self, input_file, batch_size, duckdb_table):
        logger.info(f"Starting DuckDB import from {input_file}")
        start_time = time.time()
        
        conn = duckdb.connect(self.duckdb_params['database'])
        
        try:
            # Create table if not exists (schema inferred from CSV)
            if not conn.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{duckdb_table}'").fetchone()[0]:
                conn.execute(f"CREATE TABLE {duckdb_table} AS SELECT * FROM read_csv_auto('{input_file}')")
                logger.info(f"Created table {duckdb_table} from CSV schema")
            else:
                # Use COPY for existing tables
                conn.execute(f"COPY {duckdb_table} FROM '{input_file}' (HEADER 1)")
            
            # Verify row count
            result = conn.execute(f"SELECT COUNT(*) FROM {duckdb_table}").fetchone()
            total_rows = result[0] if result else 0
            elapsed_time = time.time() - start_time
            logger.info(f"Import complete! {total_rows} rows loaded in {elapsed_time:.2f}s")
        
        except Exception as e:
            logger.error(f"Error during DuckDB import: {str(e)}")
            raise
        finally:
            conn.close()

    def BackupTable(self):
        migration_schema = self.config.get('migration_schema')
        
        if migration_schema == "push":
            for csv_config in self.config.get('csv_files', []):
                csv_name = f"{self.dump_dir}/{csv_config.get('csv')}"
                table_name = csv_config.get('table')
                try:
                    self.hc_duckdb()
                    self.csv_to_duckdb(csv_name, 0, table_name)
                except Exception as e:
                    logger.error(f"Migration failed: {str(e)}")
                    raise
        else:
            for table_config in self.config.get('tables', []):
                table_name = table_config.get('name')
                try:
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    csv_file = f'{self.dump_dir}/dump_{timestamp}_{table_name}.csv'

                    if migration_schema == "pull":
                        self.hc_oracle()
                        self.oracle_to_csv_copy(csv_file, table_name)

                    if migration_schema == "full":
                        self.hc_oracle()
                        self.hc_duckdb()
                        self.oracle_to_csv_copy(csv_file, table_name)
                        self.csv_to_duckdb(csv_file, 0, table_name)
                    
                    logger.info(f"Migration completed for {table_name}")
                
                except Exception as e:
                    logger.error(f"Migration failed for {table_name}: {str(e)}")
                    raise

def main():
    migrator = OracleToDuckDBMigration()
    migrator.BackupTable()

if __name__ == "__main__":
    main()