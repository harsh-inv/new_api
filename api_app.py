from flask import Flask, request, jsonify
from flask_cors import CORS
import sqlite3
import csv
import os
import tempfile
import json
import re
from datetime import datetime
from werkzeug.utils import secure_filename
import logging

app = Flask(__name__)
CORS(app)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

UPLOAD_FOLDER = '/tmp'
ALLOWED_EXTENSIONS = {'csv'}
MAX_CONTENT_LENGTH = 16 * 1024 * 1024

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = MAX_CONTENT_LENGTH

# Extract essential classes from your original file
class DataQualityChecker:
    def __init__(self, db_connection):
        self.db_connection = db_connection
        self.checks_config = {}
        self.system_codes_config = {}

    def load_checks_config(self, csv_file_path: str) -> bool:
        try:
            with open(csv_file_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    table_name = row['table_name']
                    field_name = row['field_name']
                    if table_name not in self.checks_config:
                        self.checks_config[table_name] = {}
                    self.checks_config[table_name][field_name] = {
                        'description': row['description'],
                        'special_characters_check': row['special_characters_check'] == '1',
                        'null_check': row['null_check'] == '1',
                        'blank_check': row['blank_check'] == '1',
                        'max_value_check': row['max_value_check'] == '1',
                        'min_value_check': row['min_value_check'] == '1',
                        'max_count_check': row['max_count_check'] == '1',
                        'email_check': row['email_check'] == '1',
                        'numeric_check': row['numeric_check'] == '1',
                        'system_codes_check': row['system_codes_check'] == '1',
                        'language_check': row['language_check'] == '1',
                        'phone_number_check': row['phone_number_check'] == '1',
                        'duplicate_check': row['duplicate_check'] == '1',
                        'date_check': row['date_check'] == '1'
                    }
            return True
        except Exception as e:
            logger.error(f"Error loading checks configuration: {str(e)}")
            return False

    def load_system_codes_config(self, csv_file_path: str) -> bool:
        try:
            self.system_codes_config = {}
            with open(csv_file_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    table_name = row['table_name']
                    field_name = row['field_name']
                    valid_codes_str = row['valid_codes']
                    valid_codes = [code.strip() for code in valid_codes_str.split(',') if code.strip()]
                    
                    if table_name not in self.system_codes_config:
                        self.system_codes_config[table_name] = {}
                    self.system_codes_config[table_name][field_name] = valid_codes
            return True
        except Exception as e:
            logger.error(f"Error loading system codes configuration: {str(e)}")
            return False

    def _table_exists(self, table_name: str) -> bool:
        try:
            cursor = self.db_connection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
            return cursor.fetchone() is not None
        except sqlite3.Error:
            return False

    def _column_exists(self, table_name: str, column_name: str) -> bool:
        try:
            cursor = self.db_connection.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [row[1] for row in cursor.fetchall()]
            return column_name in columns
        except sqlite3.Error:
            return False

    def _is_numeric(self, value: str) -> bool:
        try:
            float(value)
            return True
        except ValueError:
            return False

    def _is_valid_email(self, email: str) -> bool:
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return re.match(email_pattern, email) is not None

    def _is_valid_phone(self, phone: str) -> bool:
        cleaned_phone = re.sub(r'[^\d+]', '', phone)
        if len(cleaned_phone) < 10 or len(cleaned_phone) > 15:
            return False
        phone_pattern = r'^\+?[1-9]\d{9,14}$'
        return re.match(phone_pattern, cleaned_phone) is not None

    def _is_valid_date(self, date_str: str) -> bool:
        date_formats = [
            '%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S',
            '%m-%d-%Y', '%d-%m-%Y', '%Y/%m/%d', '%d.%m.%Y',
            '%Y', '%m/%Y', '%Y-%m'
        ]
        for fmt in date_formats:
            try:
                datetime.strptime(str(date_str), fmt)
                return True
            except ValueError:
                continue
        return False

    def _has_special_characters(self, text: str) -> bool:
        allowed_pattern = r'^[a-zA-Z0-9\s.,@_-]+$'
        return not re.match(allowed_pattern, text)

    def _has_non_ascii_characters(self, text: str) -> bool:
        try:
            text.encode('ascii')
            return False
        except UnicodeEncodeError:
            return True

    def _looks_like_system_code(self, code: str) -> bool:
        patterns = [
            r'^[A-F0-9]{8}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{12}$',
            r'^[A-Z]{2,3}\d{3,}$',
            r'^\d{6,}$',
            r'^[A-Z0-9]{8,}$',
        ]
        for pattern in patterns:
            if re.match(pattern, code.upper()):
                return True
        return False

    def _get_valid_system_codes(self, table_name: str, field_name: str) -> list:
        return self.system_codes_config.get(table_name, {}).get(field_name, [])

    def _run_field_checks(self, table_name: str, field_name: str, checks: dict) -> list:
        results = []
        
        if not self._column_exists(table_name, field_name):
            results.append({
                'table': table_name,
                'field': field_name,
                'check_type': 'column_existence',
                'status': 'FAIL',
                'message': f"Column '{field_name}' does not exist in table '{table_name}'"
            })
            return results

        try:
            cursor = self.db_connection.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_rows = cursor.fetchone()[0]

            if total_rows == 0:
                results.append({
                    'table': table_name,
                    'field': field_name,
                    'check_type': 'data_existence',
                    'status': 'WARNING',
                    'message': f"Table '{table_name}' has no data"
                })
                return results

            # Null check
            if checks.get('null_check', False):
                cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {field_name} IS NULL")
                null_count = cursor.fetchone()[0]
                if null_count > 0:
                    results.append({
                        'table': table_name,
                        'field': field_name,
                        'check_type': 'null_check',
                        'status': 'FAIL',
                        'message': f"Found {null_count} NULL values out of {total_rows} total rows"
                    })
                else:
                    results.append({
                        'table': table_name,
                        'field': field_name,
                        'check_type': 'null_check',
                        'status': 'PASS',
                        'message': f"No NULL values found"
                    })

            # Blank check
            if checks.get('blank_check', False):
                cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {field_name} = ''")
                blank_count = cursor.fetchone()[0]
                if blank_count > 0:
                    results.append({
                        'table': table_name,
                        'field': field_name,
                        'check_type': 'blank_check',
                        'status': 'FAIL',
                        'message': f"Found {blank_count} blank values out of {total_rows} total rows"
                    })
                else:
                    results.append({
                        'table': table_name,
                        'field': field_name,
                        'check_type': 'blank_check',
                        'status': 'PASS',
                        'message': f"No blank values found"
                    })

            # Email check
            if checks.get('email_check', False):
                cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {field_name} IS NOT NULL AND {field_name} != ''")
                non_null_count = cursor.fetchone()[0]
                if non_null_count > 0:
                    cursor.execute(f"SELECT {field_name} FROM {table_name} WHERE {field_name} IS NOT NULL AND {field_name} != ''")
                    values = cursor.fetchall()
                    invalid_emails = []
                    for value in values:
                        email = str(value[0]).strip()
                        if not self._is_valid_email(email):
                            invalid_emails.append(email)
                    
                    if invalid_emails:
                        results.append({
                            'table': table_name,
                            'field': field_name,
                            'check_type': 'email_check',
                            'status': 'FAIL',
                            'message': f"Found {len(invalid_emails)} invalid email formats out of {non_null_count} values"
                        })
                    else:
                        results.append({
                            'table': table_name,
                            'field': field_name,
                            'check_type': 'email_check',
                            'status': 'PASS',
                            'message': f"All {non_null_count} email formats appear valid"
                        })

            # Add other checks (phone, date, numeric, etc.) following the same pattern...

        except sqlite3.Error as e:
            results.append({
                'table': table_name,
                'field': field_name,
                'check_type': 'database_error',
                'status': 'ERROR',
                'message': f"Database error: {str(e)}"
            })

        return results

    def run_all_checks(self) -> dict:
        if not self.checks_config:
            return {}
        
        results = {}
        for table_name, fields in self.checks_config.items():
            if not self._table_exists(table_name):
                continue
            
            table_results = []
            for field_name, checks in fields.items():
                field_results = self._run_field_checks(table_name, field_name, checks)
                if field_results:
                    table_results.extend(field_results)
            
            if table_results:
                results[table_name] = table_results
        
        return results

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def create_sample_database(db_path):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS employees (
                id INTEGER PRIMARY KEY,
                name TEXT,
                email TEXT,
                phone TEXT,
                department_code TEXT,
                salary REAL,
                hire_date TEXT,
                status TEXT
            )
        ''')
        
        sample_data = [
            (1, 'John Doe', 'john.doe@company.com', '555-0123', 'IT001', 75000, '2023-01-15', 'ACTIVE'),
            (2, 'Jane Smith', 'jane.smith@company', '555-0124', 'HR002', 65000, '2023-02-01', 'ACTIVE'),  # Invalid email
            (3, '', 'bob.wilson@company.com', '555-0125', 'FIN003', 80000, '2023-03-01', 'ACTIVE'),  # Empty name
            (4, 'Alice Brown', 'alice.brown@company.com', '123', 'IT001', -5000, '2023-04-01', 'INACTIVE'),  # Invalid phone, negative salary
            (5, 'Mike Davis', 'mike.davis@company.com', '555-0127', 'INVALID', 70000, 'invalid-date', 'ACTIVE'),  # Invalid dept code, invalid date
        ]
        
        cursor.executemany('''
            INSERT OR REPLACE INTO employees 
            (id, name, email, phone, department_code, salary, hire_date, status) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', sample_data)
        
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error creating sample database: {str(e)}")
        return False

@app.route('/', methods=['GET'])
def home():
    return jsonify({
        "service": "Data Quality Checker API",
        "version": "1.0.0",
        "status": "running",
        "endpoints": {
            "/health": "Health check",
            "/api/data-quality-check": "Run data quality checks (POST)",
            "/api/sample-configs": "Get sample configuration formats"
        }
    })

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "Data Quality Checker API"
    })

@app.route('/api/data-quality-check', methods=['POST'])
def run_data_quality_checks():
    try:
        # Check if files are present
        if 'data_quality_file' not in request.files or 'system_codes_file' not in request.files:
            return jsonify({
                "success": False,
                "error": "Both data_quality_file and system_codes_file are required",
                "code": "MISSING_FILES"
            }), 400
        
        data_quality_file = request.files['data_quality_file']
        system_codes_file = request.files['system_codes_file']
        
        # Validate files
        if data_quality_file.filename == '' or system_codes_file.filename == '':
            return jsonify({
                "success": False,
                "error": "Both files must have valid filenames",
                "code": "EMPTY_FILENAMES"
            }), 400
        
        if not (allowed_file(data_quality_file.filename) and allowed_file(system_codes_file.filename)):
            return jsonify({
                "success": False,
                "error": "Only CSV files are allowed",
                "code": "INVALID_FILE_TYPE"
            }), 400
        
        # Create temporary directory for this request
        with tempfile.TemporaryDirectory() as temp_dir:
            # Save uploaded files
            dq_filename = secure_filename(data_quality_file.filename)
            sc_filename = secure_filename(system_codes_file.filename)
            
            dq_path = os.path.join(temp_dir, dq_filename)
            sc_path = os.path.join(temp_dir, sc_filename)
            db_path = os.path.join(temp_dir, 'temp_database.db')
            
            data_quality_file.save(dq_path)
            system_codes_file.save(sc_path)
            
            # Create sample database for testing
            if not create_sample_database(db_path):
                return jsonify({
                    "success": False,
                    "error": "Failed to create sample database",
                    "code": "DATABASE_ERROR"
                }), 500
            
            # Initialize data quality checker
            conn = sqlite3.connect(db_path)
            checker = DataQualityChecker(conn)
            
            # Load configurations
            if not checker.load_checks_config(dq_path):
                conn.close()
                return jsonify({
                    "success": False,
                    "error": "Failed to load data quality checks configuration",
                    "code": "CONFIG_LOAD_ERROR"
                }), 400
            
            if not checker.load_system_codes_config(sc_path):
                conn.close()
                return jsonify({
                    "success": False,
                    "error": "Failed to load system codes configuration", 
                    "code": "SYSTEM_CODES_LOAD_ERROR"
                }), 400
            
            # Run data quality checks
            results = checker.run_all_checks()
            conn.close()
            
            if not results:
                return jsonify({
                    "success": True,
                    "message": "No data quality issues found",
                    "results": {},
                    "summary": {
                        "total_checks": 0,
                        "passed_checks": 0,
                        "failed_checks": 0,
                        "warnings": 0,
                        "tables_checked": 0
                    },
                    "timestamp": datetime.now().isoformat()
                })
            
            # Process results for JSON response
            json_results = {}
            summary_stats = {
                "total_checks": 0,
                "passed_checks": 0,
                "failed_checks": 0,
                "warnings": 0,
                "tables_checked": len(results)
            }
            
            for table_name, table_results in results.items():
                json_results[table_name] = []
                
                for result in table_results:
                    summary_stats["total_checks"] += 1
                    
                    if result['status'] == 'PASS':
                        summary_stats["passed_checks"] += 1
                    elif result['status'] == 'FAIL':
                        summary_stats["failed_checks"] += 1
                    elif result['status'] == 'WARNING':
                        summary_stats["warnings"] += 1
                    
                    json_results[table_name].append({
                        "field": result['field'],
                        "check_type": result['check_type'],
                        "status": result['status'],
                        "message": result['message']
                    })
            
            # Generate failed fields summary
            failed_fields_summary = {}
            for table_name, table_results in results.items():
                table_failed_fields = {}
                for result in table_results:
                    if result['status'] in ['FAIL', 'ERROR']:
                        field_name = result['field']
                        if field_name not in table_failed_fields:
                            table_failed_fields[field_name] = []
                        table_failed_fields[field_name].append(result['check_type'])
                
                if table_failed_fields:
                    failed_fields_summary[table_name] = table_failed_fields
            
            return jsonify({
                "success": True,
                "message": "Data quality checks completed successfully",
                "results": json_results,
                "summary": summary_stats,
                "failed_fields_summary": failed_fields_summary,
                "timestamp": datetime.now().isoformat()
            })
            
    except Exception as e:
        logger.error(f"Error in data quality check endpoint: {str(e)}")
        return jsonify({
            "success": False,
            "error": f"Internal server error: {str(e)}",
            "code": "INTERNAL_ERROR"
        }), 500

@app.route('/api/sample-configs', methods=['GET'])
def get_sample_configs():
    sample_data_quality = [
        {
            "table_name": "employees",
            "field_name": "name",
            "description": "Employee name validation",
            "null_check": "1",
            "blank_check": "1",
            "special_characters_check": "0",
            "max_value_check": "0",
            "min_value_check": "0",
            "max_count_check": "0",
            "email_check": "0",
            "numeric_check": "0",
            "system_codes_check": "0",
            "language_check": "1",
            "phone_number_check": "0",
            "duplicate_check": "0",
            "date_check": "0"
        },
        {
            "table_name": "employees",
            "field_name": "email",
            "description": "Employee email validation",
            "null_check": "1",
            "blank_check": "1",
            "special_characters_check": "0",
            "max_value_check": "0",
            "min_value_check": "0",
            "max_count_check": "0",
            "email_check": "1",
            "numeric_check": "0",
            "system_codes_check": "0",
            "language_check": "0",
            "phone_number_check": "0",
            "duplicate_check": "1",
            "date_check": "0"
        }
    ]
    
    sample_system_codes = [
        {
            "table_name": "employees",
            "field_name": "department_code",
            "valid_codes": "IT001,HR002,FIN003,MKT004,OPS005"
        },
        {
            "table_name": "employees", 
            "field_name": "status",
            "valid_codes": "ACTIVE,INACTIVE,PENDING"
        }
    ]
    
    return jsonify({
        "success": True,
        "sample_configurations": {
            "data_quality_checks": {
                "description": "CSV format for data quality checks configuration",
                "headers": [
                    "table_name", "field_name", "description", "null_check", "blank_check",
                    "special_characters_check", "max_value_check", "min_value_check",
                    "max_count_check", "email_check", "numeric_check", "system_codes_check",
                    "language_check", "phone_number_check", "duplicate_check", "date_check"
                ],
                "sample_data": sample_data_quality
            },
            "system_codes": {
                "description": "CSV format for system codes configuration",
                "headers": ["table_name", "field_name", "valid_codes"],
                "sample_data": sample_system_codes
            }
        }
    })

@app.errorhandler(413)
def too_large(e):
    return jsonify({
        "success": False,
        "error": "File too large. Maximum size is 16MB.",
        "code": "FILE_TOO_LARGE"
    }), 413

@app.errorhandler(404)
def not_found(e):
    return jsonify({
        "success": False,
        "error": "Endpoint not found",
        "code": "NOT_FOUND"
    }), 404

@app.errorhandler(500)
def internal_error(e):
    return jsonify({
        "success": False,
        "error": "Internal server error",
        "code": "INTERNAL_ERROR"
    }), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
