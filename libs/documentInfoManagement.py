from db.conn import get_db_connection

def create_document_info(bank_code, service_code, doc_name, doc_issue_date, content):
    try:
        db_conn = get_db_connection()
        cursor = db_conn.cursor()
        cursor.execute("INSERT INTO DOCUMENTS_INFO (BANK_CODE, SERVICE_CODE, DOC_NAME, DOC_ISSUE_DATE, CONTENT, CREATED_DATE, UPDATED_DATE) VALUES (:bank_code, :service_code, :doc_name, :doc_issue_date, :content, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)", {
            "bank_code": bank_code,
            "service_code": service_code,
            "doc_name": doc_name,
            "doc_issue_date": doc_issue_date,
            "content": content
        })
        db_conn.commit()
    except Exception as e:
        print(f"Error while creating document info with doc_name {doc_name}\n\tError: {e}")
        db_conn.rollback()