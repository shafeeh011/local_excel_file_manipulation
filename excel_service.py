from flask import Flask, request, jsonify
import pandas as pd
import os
from flask_cors import CORS
import json
import tempfile
import xlrd

app = Flask(__name__)
CORS(app)

def read_excel_file(file_path, sheet_name=0):
    """Read Excel file handling both .xls and .xlsx formats"""
    ext = os.path.splitext(file_path)[1].lower()
    if ext == '.xlsx':
        return pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl')
    elif ext == '.xls':
        book = xlrd.open_workbook(file_path)
        sheet = book.sheet_by_index(sheet_name if isinstance(sheet_name, int) else 0)
        data = [sheet.row_values(r) for r in range(sheet.nrows)]
        header, rows = data[0], data[1:]
        df_temp = pd.DataFrame(rows, columns=header)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            df_temp.to_excel(tmp.name, index=False)
            return pd.read_excel(tmp.name, sheet_name=0, engine='openpyxl')
    else:
        raise ValueError(f"Unsupported file extension: {ext}")

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "service": "excel-service"})

@app.route('/append-to-next-row', methods=['POST'])
def append_to_next_row():
    """Append new data to the next available row in Excel file"""
    try:
        data = request.json
        file_path = data.get('file_path')
        new_data = data.get('new_data')
        
        if not file_path or not new_data:
            return jsonify({"error": "file_path and new_data are required"}), 400
        
        if not os.path.exists(file_path):
            return jsonify({"error": f"File not found: {file_path}"}), 404
        
        # Read existing Excel file
        df = read_excel_file(file_path)
        
        # Convert new_data to DataFrame if it's a dict
        if isinstance(new_data, dict):
            new_data = [new_data]
        
        # Create DataFrame for new data with same columns as existing
        new_df = pd.DataFrame(new_data, columns=df.columns)
        
        # Append to existing data
        updated_df = pd.concat([df, new_df], ignore_index=True)
        
        # Save back to file
        ext = os.path.splitext(file_path)[1].lower()
        if ext == '.xlsx':
            updated_df.to_excel(file_path, index=False, engine='openpyxl')
        else:
            xlsx_path = file_path.replace('.xls', '.xlsx')
            updated_df.to_excel(xlsx_path, index=False, engine='openpyxl')
        
        return jsonify({
            "status": "success",
            "message": f"Added {len(new_df)} new rows to {file_path}",
            "new_row_numbers": list(range(len(df) + 1, len(updated_df) + 1)),
            "total_rows": len(updated_df)
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/smart-update', methods=['POST'])
def smart_update():
    """Smart update: append if no match found, update if match exists"""
    try:
        data = request.json
        file_path = data.get('file_path')
        new_data = data.get('new_data')
        match_column = data.get('match_column')  # Column to check for existing records
        
        if not file_path or not new_data:
            return jsonify({"error": "file_path and new_data are required"}), 400
        
        if not os.path.exists(file_path):
            return jsonify({"error": f"File not found: {file_path}"}), 404
        
        # Read existing Excel file
        df = read_excel_file(file_path)
        
        # Convert new_data to list if it's a dict
        if isinstance(new_data, dict):
            new_data = [new_data]
        
        updated_count = 0
        added_count = 0
        
        for record in new_data:
            if match_column and match_column in record and match_column in df.columns:
                # Check if record already exists
                match_value = record[match_column]
                mask = df[match_column] == match_value
                
                if mask.any():
                    # Update existing record
                    for col, value in record.items():
                        if col in df.columns:
                            df.loc[mask, col] = value
                    updated_count += mask.sum()
                else:
                    # Add new record
                    new_row = pd.DataFrame([record], columns=df.columns)
                    df = pd.concat([df, new_row], ignore_index=True)
                    added_count += 1
            else:
                # No match column specified, just append
                new_row = pd.DataFrame([record], columns=df.columns)
                df = pd.concat([df, new_row], ignore_index=True)
                added_count += 1
        
        # Save back to file
        ext = os.path.splitext(file_path)[1].lower()
        if ext == '.xlsx':
            df.to_excel(file_path, index=False, engine='openpyxl')
        else:
            xlsx_path = file_path.replace('.xls', '.xlsx')
            df.to_excel(xlsx_path, index=False, engine='openpyxl')
        
        return jsonify({
            "status": "success",
            "message": f"Updated {updated_count} rows, added {added_count} new rows",
            "updated_rows": updated_count,
            "added_rows": added_count,
            "total_rows": len(df)
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/append-excel', methods=['POST'])
def append_excel():
    """Original append function - keeps for backward compatibility"""
    try:
        data = request.json
        file_path = data.get('file_path')
        new_data = data.get('new_data')

        if not file_path or not new_data:
            return jsonify({"error": "file_path and new_data are required"}), 400
        if not os.path.exists(file_path):
            return jsonify({"error": f"File not found: {file_path}"}), 404

        df = read_excel_file(file_path)

        if isinstance(new_data, dict):
            new_data = [new_data]
        new_df = pd.DataFrame(new_data, columns=df.columns)

        updated_df = pd.concat([df, new_df], ignore_index=True)

        ext = os.path.splitext(file_path)[1].lower()
        if ext == '.xlsx':
            updated_df.to_excel(file_path, index=False, engine='openpyxl')
        else:
            xlsx_path = file_path.replace('.xls', '.xlsx')
            updated_df.to_excel(xlsx_path, index=False, engine='openpyxl')

        return jsonify({
            "status": "success",
            "message": f"Appended {len(new_df)} rows to {file_path}",
            "total_rows": len(updated_df)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/update-excel', methods=['POST'])
def update_excel():
    """Original update function - keeps for backward compatibility"""
    try:
        data = request.json
        file_path = data.get('file_path')
        updates = data.get('updates')

        if not file_path or not updates:
            return jsonify({"error": "file_path and updates are required"}), 400
        if not os.path.exists(file_path):
            return jsonify({"error": f"File not found: {file_path}"}), 404

        df = read_excel_file(file_path)
        updated_count = 0

        for update in updates:
            mask = df[update['condition_column']] == update['condition_value']
            if mask.any():
                df.loc[mask, update['update_column']] = update['new_value']
                updated_count += mask.sum()

        ext = os.path.splitext(file_path)[1].lower()
        if ext == '.xlsx':
            df.to_excel(file_path, index=False, engine='openpyxl')
        else:
            df.to_excel(file_path.replace('.xls', '.xlsx'), index=False, engine='openpyxl')

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
        sheet_name = data.get('sheet_name', 0)

        if not file_path:
            return jsonify({"error": "file_path is required"}), 400
        if not os.path.exists(file_path):
            return jsonify({"error": f"File not found: {file_path}"}), 404

        df = read_excel_file(file_path, sheet_name)
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

        df = pd.DataFrame(excel_data)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

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
    print("Starting Enhanced Excel Service...")
    print("Available endpoints:")
    print("  GET  /health - Health check")
    print("  POST /append-to-next-row - Append data to next available row")
    print("  POST /smart-update - Update existing or append new based on match column")
    print("  POST /append-excel - Original append function")
    print("  POST /update-excel - Original update function")
    print("  POST /read-excel - Read Excel file")
    print("  POST /create-excel - Create new Excel file")
    print("\nService running on http://localhost:8000")
    
    app.run(host='0.0.0.0', port=8000, debug=True)