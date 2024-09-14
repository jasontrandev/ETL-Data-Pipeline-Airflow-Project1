from datetime import datetime

#################################### AWS RDS Queries ###################################
SELECT_EMP_SAL = """
select * from finance_emp_sal
"""

SELECT_EMP_DETAIL = """
select * from hr_emp_details
"""

################################## Snowflake DWH Queries ###############################
SELECT_DWH_EMP_DIM = """
select * from airflow_db.tcops_dimensions.employee_dim
"""

def INSERT_INTO_DWH_EMP_DIM(rows_to_insert):
    print("rows_to_insert:", rows_to_insert)
    sql = f"""
    insert into airflow_db.tcops_dimensions.employee_dim (
        employee_name, employee_id, employee_email, job, supervisor_name, company, 
        org_level_3, org_level_2, org_level_1, org_level_4, last_hire_date, termination_date, 
        effective_start_date, effective_end_date, is_active
    )
    values {rows_to_insert}
    """
    return sql

def UPDATE_DWH_EMP_DIM(ids_to_update):
    print("ids_to_update", ids_to_update)
    sql = f"""
    update airflow_db.tcops_dimensions.employee_dim
    set effective_end_date = '{datetime.now().date().strftime("%Y-%m-%d")}', is_active = 'N'
    where employee_id in ({ids_to_update}) and is_active = 'Y'
    """
    return sql