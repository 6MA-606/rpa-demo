from db.conn import get_db_connection

def write_log(process_name, doc_name, process_status, annotation, create_by):
    try:
        db_conn = get_db_connection()
        cursor = db_conn.cursor()
        cursor.execute("INSERT INTO PROCESS_HISTORY (PROCESS_NAME, DOC_NAME, PROCESS_STATUS, ANNOTATION, CREATED_BY, CREATED_DATE) VALUES (:process_name, :doc_name, :process_status, :annotation, :create_by, CURRENT_TIMESTAMP)", {
            "process_name": process_name,
            "doc_name": doc_name,
            "process_status": process_status,
            "annotation": annotation,
            "create_by": create_by
        })
        db_conn.commit()
    except Exception as e:
        print(f"Error while writing log of process {process_name} with doc_name {doc_name}\n\tStatus: {process_status}\n\tAnnotation: {annotation}\n\tCreated by: {create_by}\n\tError: {e}")
        db_conn.rollback()