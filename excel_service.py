from flask import Flask, request, jsonify
import pandas as pd
import os
from flask_cors import CORS
import json
import tempfile
import xlrd  # This will be version 1.2.0

app = Flask(__name__)
CORS(app)  # Enable CORS for n8n requests

def read_excel_file(file_path, sheet_name=0):
    ext = os.path.splitext(file_path)[1].lower()

    if ext == '.xlsx':
        return pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl')

    elif ext == '.xls':
        # Read .xls using xlrd directly to avoid Pandas version check
        book = xlrd.open_workbook(file_path)
        sheet = book.sheet_by_index(sheet_name if isinstance(sheet_name, int) else 0)
        data = []
        for row_idx in range(sheet.nrows):
            data.append(sheet.row_values(row_idx))

        # Convert to DataFrame
        header = data[0]
        rows = data[1:]
        df_temp = pd.DataFrame(rows, columns=header)

        # Save to temp xlsx and re-read with openpyxl (optional)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            df_temp.to_excel(tmp.name, index=False)
            return pd.read_excel(tmp.name, sheet_name=0, engine='openpyxl')

    else:
        raise ValueError(f"Unsupported file extension: {ext}")

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "service": "excel-service"})

@app.route('/append-excel', methods=['POST'])
def append_excel():
    """Append data to existing Excel file"""
    try:
        data = request.json
        file_path = data.get('file_path')
        new_data = data.get('new_data')
        
        if not file_path or not new_data:
            return jsonify({"error": "file_path and new_data are required"}), 400
        
        if not os.path.exists(file_path):
            return jsonify({"error": f"File not found: {file_path}"}), 404
        
        # Read existing Excel using your custom function
        df = read_excel_file(file_path)
        
        # Convert new_data to DataFrame
        if isinstance(new_data, list):
            new_df = pd.DataFrame(new_data)
        else:
            new_df = pd.DataFrame([new_data])
        
        # Append new data
        result = pd.concat([df, new_df], ignore_index=True)
        
        # Save back to Excel
        ext = os.path.splitext(file_path)[1].lower()
        if ext == '.xlsx':
            result.to_excel(file_path, index=False, engine='openpyxl')
        else:  # .xls - convert to xlsx for better compatibility
            xlsx_path = file_path.replace('.xls', '.xlsx')
            result.to_excel(xlsx_path, index=False, engine='openpyxl')
            print(f"Note: Converted {file_path} to {xlsx_path} for better compatibility")
        
        return jsonify({
            "status": "success", 
            "message": f"Appended {len(new_df)} rows to {file_path}",
            "total_rows": len(result)
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/update-excel', methods=['POST'])
def update_excel():
    """Update specific rows in Excel file"""
    try:
        data = request.json
        file_path = data.get('file_path')
        updates = data.get('updates')  # List of updates with conditions
        
        if not file_path or not updates:
            return jsonify({"error": "file_path and updates are required"}), 400
        
        if not os.path.exists(file_path):
            return jsonify({"error": f"File not found: {file_path}"}), 404
        
        # Read existing Excel using custom function
        df = read_excel_file(file_path)
        
        updated_count = 0
        
        # Apply each update
        for update in updates:
            condition_column = update.get('condition_column')
            condition_value = update.get('condition_value')
            update_column = update.get('update_column')
            new_value = update.get('new_value')
            
            # Find rows matching condition
            mask = df[condition_column] == condition_value
            
            if mask.any():
                df.loc[mask, update_column] = new_value
                updated_count += mask.sum()
        
        # Save back to Excel
        ext = os.path.splitext(file_path)[1].lower()
        if ext == '.xlsx':
            df.to_excel(file_path, index=False, engine='openpyxl')
        else:  # .xls - convert to xlsx
            xlsx_path = file_path.replace('.xls', '.xlsx')
            df.to_excel(xlsx_path, index=False, engine='openpyxl')
        
        return jsonify({
            "status": "success",
            "message": f"Updated {updated_count} rows in {file_path}",
            "total_rows": len(df)
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/read-excel', methods=['POST'])
def read_excel():
    """Read Excel file and return data"""
    try:
        data = request.json
        file_path = data.get('file_path')
        sheet_name = data.get('sheet_name', 0)  # Default to first sheet
        
        if not file_path:
            return jsonify({"error": "file_path is required"}), 400
        
        if not os.path.exists(file_path):
            return jsonify({"error": f"File not found: {file_path}"}), 404
        
        # Read Excel using custom function
        df = read_excel_file(file_path, sheet_name)
        
        # Convert to JSON
        result = df.to_dict('records')
        
        return jsonify({
            "status": "success",
            "data": result,
            "total_rows": len(result)
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/create-excel', methods=['POST'])
def create_excel():
    """Create new Excel file with data"""
    try:
        data = request.json
        file_path = data.get('file_path')
        excel_data = data.get('data')
        
        if not file_path or not excel_data:
            return jsonify({"error": "file_path and data are required"}), 400
        
        # Create DataFrame
        df = pd.DataFrame(excel_data)
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # Save to Excel (always use xlsx for new files)
        if not file_path.endswith('.xlsx'):
            file_path = file_path.replace('.xls', '.xlsx')
        
        df.to_excel(file_path, index=False, engine='openpyxl')
        
        return jsonify({
            "status": "success",
            "message": f"Created Excel file at {file_path}",
            "total_rows": len(df)
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    print("Starting Excel Service...")
    print("Available endpoints:")
    print("  GET  /health - Health check")
    print("  POST /append-excel - Append data to Excel")
    print("  POST /update-excel - Update Excel rows")
    print("  POST /read-excel - Read Excel file")
    print("  POST /create-excel - Create new Excel file")
    print("\nService running on http://localhost:8000")
    
    app.run(host='0.0.0.0', port=8000, debug=True)