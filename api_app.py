from flask import Flask, request, jsonify
from flask_cors import CORS
import sqlite3
import csv
import os
import tempfile
import json
from datetime import datetime
from werkzeug.utils import secure_filename
import logging

# Import your existing classes
from org_1_2907 import DataQualityChecker, Colors

app = Flask(__name__)
CORS(app)  # Enable CORS for cross-origin requests from UI5

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
UPLOAD_FOLDER = '/tmp'
ALLOWED_EXTENSIONS = {'csv'}
MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16MB max file size

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = MAX_CONTENT_LENGTH

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def create_sample_database(db_path):
    """Create a sample database for testing"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create sample employees table
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
        
        # Insert sample data with some quality issues
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

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "Data Quality Checker API"
    })

@app.route('/api/data-quality-check', methods=['POST'])
def run_data_quality_checks():
    """Main endpoint for running data quality checks"""
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
    """Endpoint to get sample configuration file formats"""
    
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
